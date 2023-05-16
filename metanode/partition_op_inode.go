// Copyright 2018 The CubeFS Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License.

package metanode

import (
	"encoding/binary"
	"encoding/json"
	"fmt"
	"net/url"
	"strings"
	"time"

	"github.com/cubefs/cubefs/proto"
	"github.com/cubefs/cubefs/util/errors"
	"github.com/cubefs/cubefs/util/log"
)

func replyInfoNoCheck(info *proto.InodeInfo, ino *Inode, quotaIds []uint32) bool {
	ino.RLock()
	defer ino.RUnlock()

	info.Inode = ino.Inode
	info.Mode = ino.Type
	info.Size = ino.Size
	info.Nlink = ino.NLink
	info.Uid = ino.Uid
	info.Gid = ino.Gid
	info.Generation = ino.Generation
	info.VerSeq = ino.verSeq
	if length := len(ino.LinkTarget); length > 0 {
		info.Target = make([]byte, length)
		copy(info.Target, ino.LinkTarget)
	}
	info.CreateTime = time.Unix(ino.CreateTime, 0)
	info.AccessTime = time.Unix(ino.AccessTime, 0)
	info.ModifyTime = time.Unix(ino.ModifyTime, 0)
	return true
}

func replyInfo(info *proto.InodeInfo, ino *Inode, quotaIds []uint32) bool {
	ino.RLock()
	defer ino.RUnlock()
	if ino.Flag&DeleteMarkFlag > 0 {
		return false
	}
	info.Inode = ino.Inode
	info.Mode = ino.Type
	info.Size = ino.Size
	info.Nlink = ino.NLink
	info.Uid = ino.Uid
	info.Gid = ino.Gid
	info.Generation = ino.Generation
	info.VerSeq = ino.verSeq
	if length := len(ino.LinkTarget); length > 0 {
		info.Target = make([]byte, length)
		copy(info.Target, ino.LinkTarget)
	}
	info.CreateTime = time.Unix(ino.CreateTime, 0)
	info.AccessTime = time.Unix(ino.AccessTime, 0)
	info.ModifyTime = time.Unix(ino.ModifyTime, 0)
	info.QuotaIds = quotaIds
	return true
}

func txReplyInfo(inode *Inode, txInfo *proto.TransactionInfo) (resp *proto.TxCreateInodeResponse) {
	inoInfo := &proto.InodeInfo{
		Inode:      inode.Inode,
		Mode:       inode.Type,
		Nlink:      inode.NLink,
		Size:       inode.Size,
		Uid:        inode.Uid,
		Gid:        inode.Gid,
		Generation: inode.Generation,
		ModifyTime: time.Unix(inode.ModifyTime, 0),
		CreateTime: time.Unix(inode.CreateTime, 0),
		AccessTime: time.Unix(inode.AccessTime, 0),
		Target:     nil,
	}
	if length := len(inode.LinkTarget); length > 0 {
		inoInfo.Target = make([]byte, length)
		copy(inoInfo.Target, inode.LinkTarget)
	}

	resp = &proto.TxCreateInodeResponse{
		Info:   inoInfo,
		TxInfo: txInfo,
	}
	return
}

/*func txReplyInfo(resp *proto.TxCreateInodeResponse, txInode *TxInode) bool {
	resp.Info.Inode = txInode.Inode.Inode
	resp.Info.Mode = txInode.Inode.Type
	resp.Info.Size = txInode.Inode.Size
	resp.Info.Nlink = txInode.Inode.NLink
	resp.Info.Uid = txInode.Inode.Uid
	resp.Info.Gid = txInode.Inode.Gid
	resp.Info.Generation = txInode.Inode.Generation
	if length := len(txInode.Inode.LinkTarget); length > 0 {
		resp.Info.Target = make([]byte, length)
		copy(resp.Info.Target, txInode.Inode.LinkTarget)
	}
	resp.Info.CreateTime = time.Unix(txInode.Inode.CreateTime, 0)
	resp.Info.AccessTime = time.Unix(txInode.Inode.AccessTime, 0)
	resp.Info.ModifyTime = time.Unix(txInode.Inode.ModifyTime, 0)

	//resp.TxInfo = txInode.TxInfo
	return true
}*/

// CreateInode returns a new inode.
func (mp *metaPartition) CreateInode(req *CreateInoReq, p *Packet) (err error) {
	var (
		status = proto.OpNotExistErr
		reply  []byte
		resp   interface{}
		qinode *MetaQuotaInode
	)
	inoID, err := mp.nextInodeID()
	if err != nil {
		p.PacketErrorWithBody(proto.OpInodeFullErr, []byte(err.Error()))
		return
	}
	ino := NewInode(inoID, req.Mode)
	ino.Uid = req.Uid
	ino.Gid = req.Gid
	ino.verSeq = mp.verSeq
	ino.LinkTarget = req.Target

	if defaultQuotaSwitch {
		for _, quotaId := range req.QuotaIds {
			status = mp.mqMgr.IsOverQuota(false, true, quotaId)
			if status != 0 {
				err = errors.New("create inode is over quota")
				reply = []byte(err.Error())
				p.PacketErrorWithBody(status, reply)
				return
			}
		}
		qinode = &MetaQuotaInode{
			inode:    ino,
			quotaIds: req.QuotaIds,
		}
		val, err := qinode.Marshal()
		if err != nil {
			p.PacketErrorWithBody(proto.OpErr, []byte(err.Error()))
			return err
		}
		resp, err = mp.submit(opFSMCreateInodeQuota, val)
		if err != nil {
			p.PacketErrorWithBody(proto.OpAgain, []byte(err.Error()))
			return err
		}
	} else {
		log.LogDebugf("action[CreateInode] mp[%v] %v with seq %v", mp.config.PartitionId, inoID, mp.verSeq)
		val, err := ino.Marshal()
		if err != nil {
			p.PacketErrorWithBody(proto.OpErr, []byte(err.Error()))
			return err
		}
		resp, err = mp.submit(opFSMCreateInode, val)
		if err != nil {
			p.PacketErrorWithBody(proto.OpAgain, []byte(err.Error()))
			return err
		}
	}

	if resp.(uint8) == proto.OpOk {
		resp := &CreateInoResp{
			Info: &proto.InodeInfo{},
		}
		if replyInfo(resp.Info, ino, req.QuotaIds) {
			status = proto.OpOk
			reply, err = json.Marshal(resp)
			if err != nil {
				status = proto.OpErr
				reply = []byte(err.Error())
			}
		}
	}
	p.PacketErrorWithBody(status, reply)
	log.LogInfof("CreateInode req [%v] qinode [%v] success.", req, qinode)
	return
}

func (mp *metaPartition) TxUnlinkInode(req *proto.TxUnlinkInodeRequest, p *Packet) (err error) {

	txInfo := req.TxInfo.GetCopy()
	/*if req.TxInfo.TxID == "" && req.TxInfo.TmID == -1 {
		txInfo.TxID = mp.txProcessor.txManager.nextTxID()
		txInfo.TmID = int64(mp.config.PartitionId)
		txInfo.CreateTime = time.Now().Unix()
	} else {
		//todo_tx:RM
	}*/

	if !txInfo.IsInitialized() {
		mp.initTxInfo(txInfo)
	} /* else {
		if txInfo.IsExpired() {
			p.PacketErrorWithBody(proto.OpTxTimeoutErr, nil)
			return
		}
	}*/

	ino := NewInode(req.Inode, 0)
	inoResp := mp.getInode(ino, true)
	if inoResp.Status != proto.OpOk {
		p.PacketErrorWithBody(inoResp.Status, nil)
		return
	}
	ti := &TxInode{
		Inode:  inoResp.Msg,
		TxInfo: txInfo,
	}

	val, err := ti.Marshal()
	if err != nil {
		p.PacketErrorWithBody(proto.OpErr, []byte(err.Error()))
		return
	}
	r, err := mp.submit(opFSMTxUnlinkInode, val)
	if err != nil {
		p.PacketErrorWithBody(proto.OpAgain, []byte(err.Error()))
		return
	}
	msg := r.(*InodeResponse)
	status := msg.Status
	var reply []byte
	if status == proto.OpOk {
		var rstTxInfo *proto.TransactionInfo
		if req.TxInfo.TxID == "" && req.TxInfo.TmID == -1 {
			rstTxInfo = mp.txProcessor.txManager.getTransaction(txInfo.TxID)
		} else {
			rstTxInfo = req.TxInfo
		}
		//rstTxInfo := mp.txProcessor.txManager.getTransaction(txInfo.TxID)
		resp := &proto.TxUnlinkInodeResponse{
			Info:   &proto.InodeInfo{},
			TxInfo: rstTxInfo,
		}
		replyInfo(resp.Info, msg.Msg, make([]uint32, 0))
		if reply, err = json.Marshal(resp); err != nil {
			status = proto.OpErr
			reply = []byte(err.Error())
		}
	}
	p.PacketErrorWithBody(status, reply)
	return
}

// DeleteInode deletes an inode.
func (mp *metaPartition) UnlinkInode(req *UnlinkInoReq, p *Packet) (err error) {
	var (
		msg   *InodeResponse
		reply []byte
	)

	makeRspFunc := func() {
		status := msg.Status
		if status == proto.OpOk {
			resp := &UnlinkInoResp{
				Info: &proto.InodeInfo{},
			}
			replyInfo(resp.Info, msg.Msg, make([]uint32, 0))
			if reply, err = json.Marshal(resp); err != nil {
				status = proto.OpErr
				reply = []byte(err.Error())
			}
		}
		p.PacketErrorWithBody(status, reply)
	}

	ino := NewInode(req.Inode, 0)
	ino.verSeq = req.VerSeq
	log.LogDebugf("action[UnlinkInode] verseq %v ino %v", ino.verSeq, ino)
	item := mp.inodeTree.Get(ino)
	if item == nil {
		err = fmt.Errorf("inode %v reqeust cann't found", ino)
		log.LogErrorf("action[UnlinkInode] %v", err)
		p.PacketErrorWithBody(proto.OpNotExistErr, []byte(err.Error()))
		return
	}
	log.LogDebugf("action[UnlinkInode] ino %v submit", ino)

	val, err := ino.Marshal()
	if err != nil {
		p.PacketErrorWithBody(proto.OpErr, []byte(err.Error()))
		return
	}
	log.LogDebugf("action[UnlinkInode] ino %v submit", ino)
	r, err := mp.submit(opFSMUnlinkInode, val)
	if err != nil {
		p.PacketErrorWithBody(proto.OpAgain, []byte(err.Error()))
		return
	}

	log.LogDebugf("action[UnlinkInode] %v get resp", ino)
	msg = r.(*InodeResponse)
	makeRspFunc()

	return
}

// DeleteInode deletes an inode.
func (mp *metaPartition) UnlinkInodeBatch(req *BatchUnlinkInoReq, p *Packet) (err error) {

	if len(req.Inodes) == 0 {
		return nil
	}

	var inodes InodeBatch

	for _, id := range req.Inodes {
		inodes = append(inodes, NewInode(id, 0))
	}

	val, err := inodes.Marshal()
	if err != nil {
		p.PacketErrorWithBody(proto.OpErr, []byte(err.Error()))
		return
	}
	r, err := mp.submit(opFSMUnlinkInodeBatch, val)
	if err != nil {
		p.PacketErrorWithBody(proto.OpAgain, []byte(err.Error()))
		return
	}

	result := &BatchUnlinkInoResp{}
	status := proto.OpOk
	for _, ir := range r.([]*InodeResponse) {
		if ir.Status != proto.OpOk {
			status = ir.Status
		}

		info := &proto.InodeInfo{}
		replyInfo(info, ir.Msg, make([]uint32, 0))
		result.Items = append(result.Items, &struct {
			Info   *proto.InodeInfo `json:"info"`
			Status uint8            `json:"status"`
		}{
			Info:   info,
			Status: ir.Status,
		})
	}

	reply, err := json.Marshal(result)
	if err != nil {
		status = proto.OpErr
		reply = []byte(err.Error())
	}
	p.PacketErrorWithBody(status, reply)
	return
}

// InodeGet executes the inodeGet command from the client.
func (mp *metaPartition) InodeGet(req *InodeGetReq, p *Packet) (err error) {

	ino := NewInode(req.Inode, 0)
	ino.verSeq = req.VerSeq
	getAllVerInfo := req.VerAll
	retMsg := mp.getInode(ino, getAllVerInfo)

	log.LogDebugf("action[Inode] %v seq %v retMsg.Status %v, getAllVerInfo %v",
		ino.Inode, req.VerSeq, retMsg.Status, getAllVerInfo)

	ino = retMsg.Msg

	var (
		reply  []byte
		status = proto.OpNotExistErr
	)
	quotaIds, err := mp.getInodeQuotaIds(req.Inode)
	if err != nil {
		status = proto.OpErr
		reply = []byte(err.Error())
		p.PacketErrorWithBody(status, reply)
		return
	}

	ino = retMsg.Msg
	if retMsg.Status == proto.OpOk {
		resp := &proto.InodeGetResponse{
			Info: &proto.InodeInfo{},
		}
		if getAllVerInfo {
			replyInfoNoCheck(resp.Info, retMsg.Msg, quotaIds)
		} else {
			if !replyInfo(resp.Info, retMsg.Msg, quotaIds) {
				p.PacketErrorWithBody(status, reply)
				return
			}
		}

		status = proto.OpOk
		if getAllVerInfo {
			inode := mp.getInodeTopLayer(ino)
			log.LogDebugf("req ino %v, toplayer ino %v", retMsg.Msg, inode)
			resp.LayAll = inode.Msg.getAllInodesInfo()
		}
		reply, err = json.Marshal(resp)
		if err != nil {
			status = proto.OpErr
			reply = []byte(err.Error())
		}

	}
	p.PacketErrorWithBody(status, reply)
	return
}

func (mp *metaPartition) inodeExpired(inode *Inode, cond *proto.InodeExpireCondition) bool {

	if inode == nil || cond == nil {
		return false
	}

	now := Now.GetCurrentTime().Unix()
	if cond.ExpirationInfo.Days > 0 {
		if now-inode.ModifyTime < int64(cond.ExpirationInfo.Days*24*60*60) {
			return false
		}
	}

	if cond.ExpirationInfo.Date != nil {
		if now < cond.ExpirationInfo.Date.Unix() {
			return false
		}
	}

	if cond.ObjectSizeGreaterThan > 0 {
		if inode.Size <= uint64(cond.ObjectSizeGreaterThan) {
			return false
		}
	}

	if cond.ObjectSizeLessThan > 0 {
		if inode.Size >= uint64(cond.ObjectSizeLessThan) {
			return false
		}
	}

	if len(cond.Tags) > 0 {
		treeItem := mp.extendTree.Get(NewExtend(inode.Inode))
		if treeItem != nil {
			extend := treeItem.(*Extend)

			if val, exist := extend.Get([]byte("oss:tagging")); exist {
				tagParser := func(src string) map[string]string {
					values, err := url.ParseQuery(src)
					if err != nil {
						return nil
					}
					tagMap := make(map[string]string, 0)
					for key, value := range values {
						tagMap[key] = value[0]
					}
					if len(tagMap) == 0 {
						return nil
					}
					return tagMap
				}

				tags := tagParser(string(val))
				for _, tag := range cond.Tags {
					if tagVal, exist := tags[tag.Key]; exist {
						if tagVal != tag.Value {
							return false
						}
					} else {
						return false
					}
				}

			} else {
				return false
			}

		}
	}
	return true
}

func (mp *metaPartition) InodeExpirationGetBatch(req *InodeGetExpirationReqBatch, p *Packet) (err error) {
	resp := &proto.BatchInodeGetExpirationResponse{
		ExpirationResults: make([]*proto.ExpireInfo, 0),
	}
	ino := NewInode(0, 0)
	for _, dentry := range req.Dentries {
		ino.Inode = dentry.Inode
		retMsg := mp.getInode(ino, false)
		expireInfo := &proto.ExpireInfo{
			Dentry:  dentry,
			Expired: false,
		}
		if retMsg.Status == proto.OpOk {

			if mp.inodeExpired(retMsg.Msg, req.Cond) {
				expireInfo.Expired = true
			}
		}
		resp.ExpirationResults = append(resp.ExpirationResults, expireInfo)
	}
	data, err := json.Marshal(resp)
	if err != nil {
		p.PacketErrorWithBody(proto.OpErr, []byte(err.Error()))
		return
	}
	p.PacketOkWithBody(data)
	return
}

// InodeGetBatch executes the inodeBatchGet command from the client.
func (mp *metaPartition) InodeGetBatch(req *InodeGetReqBatch, p *Packet) (err error) {

	var quotaIds []uint32
	log.LogDebugf("action[InodeGetBatch] req %v", req)

	resp := &proto.BatchInodeGetResponse{}
	ino := NewInode(0, 0)
	for _, inoId := range req.Inodes {
		ino.Inode = inoId
		ino.verSeq = req.VerSeq

		retMsg := mp.getInode(ino, false)
		quotaIds, err = mp.getInodeQuotaIds(inoId)
		if err != nil {
			p.PacketErrorWithBody(proto.OpErr, []byte(err.Error()))
			return
		}
		if retMsg.Status == proto.OpOk {
			inoInfo := &proto.InodeInfo{}
			if replyInfo(inoInfo, retMsg.Msg, quotaIds) {
				resp.Infos = append(resp.Infos, inoInfo)
			}
		}
	}
	data, err := json.Marshal(resp)
	if err != nil {
		p.PacketErrorWithBody(proto.OpErr, []byte(err.Error()))
		return
	}
	p.PacketOkWithBody(data)
	return
}

func (mp *metaPartition) TxCreateInodeLink(req *proto.TxLinkInodeRequest, p *Packet) (err error) {
	txInfo := req.TxInfo.GetCopy()
	/*if req.TxInfo.TxID == "" && req.TxInfo.TmID == -1 {
		txInfo.TxID = mp.txProcessor.txManager.nextTxID()
		txInfo.TmID = int64(mp.config.PartitionId)
		txInfo.CreateTime = time.Now().Unix()
	} else {
		//todo_tx:RM
	}*/

	if !txInfo.IsInitialized() {
		mp.initTxInfo(txInfo)
	} /* else {
		if txInfo.IsExpired() {
			p.PacketErrorWithBody(proto.OpTxTimeoutErr, nil)
			return
		}
	}*/

	ino := NewInode(req.Inode, 0)
	inoResp := mp.getInode(ino, true)
	if inoResp.Status != proto.OpOk {
		p.PacketErrorWithBody(inoResp.Status, []byte(err.Error()))
		return
	}
	ti := &TxInode{
		Inode:  inoResp.Msg,
		TxInfo: txInfo,
	}

	val, err := ti.Marshal()
	if err != nil {
		p.PacketErrorWithBody(proto.OpErr, []byte(err.Error()))
		return
	}

	resp, err := mp.submit(opFSMTxCreateLinkInode, val)
	if err != nil {
		p.PacketErrorWithBody(proto.OpAgain, []byte(err.Error()))
		return
	}
	retMsg := resp.(*InodeResponse)
	status := proto.OpNotExistErr
	var reply []byte
	if retMsg.Status == proto.OpOk {
		var rstTxInfo *proto.TransactionInfo
		if req.TxInfo.TxID == "" && req.TxInfo.TmID == -1 {
			rstTxInfo = mp.txProcessor.txManager.getTransaction(txInfo.TxID)
		} else {
			rstTxInfo = req.TxInfo
		}
		//rstTxInfo := mp.txProcessor.txManager.getTransaction(txInfo.TxID)
		resp := &proto.TxLinkInodeResponse{
			Info:   &proto.InodeInfo{},
			TxInfo: rstTxInfo,
		}
		if replyInfo(resp.Info, retMsg.Msg, make([]uint32, 0)) {
			status = proto.OpOk
			reply, err = json.Marshal(resp)
			if err != nil {
				status = proto.OpErr
				reply = []byte(err.Error())
			}
		}

	}
	p.PacketErrorWithBody(status, reply)
	return
}

// CreateInodeLink creates an inode link (e.g., soft link).
func (mp *metaPartition) CreateInodeLink(req *LinkInodeReq, p *Packet) (err error) {
	ino := NewInode(req.Inode, 0)

	val, err := ino.Marshal()
	if err != nil {
		p.PacketErrorWithBody(proto.OpErr, []byte(err.Error()))
		return
	}
	resp, err := mp.submit(opFSMCreateLinkInode, val)
	if err != nil {
		p.PacketErrorWithBody(proto.OpAgain, []byte(err.Error()))
		return
	}
	retMsg := resp.(*InodeResponse)
	status := proto.OpNotExistErr
	var reply []byte
	if retMsg.Status == proto.OpOk {
		resp := &LinkInodeResp{
			Info: &proto.InodeInfo{},
		}
		if replyInfo(resp.Info, retMsg.Msg, make([]uint32, 0)) {
			status = proto.OpOk
			reply, err = json.Marshal(resp)
			if err != nil {
				status = proto.OpErr
				reply = []byte(err.Error())
			}
		}

	}
	p.PacketErrorWithBody(status, reply)
	return
}

// EvictInode evicts an inode.
func (mp *metaPartition) EvictInode(req *EvictInodeReq, p *Packet) (err error) {
	ino := NewInode(req.Inode, 0)
	val, err := ino.Marshal()
	if err != nil {
		p.PacketErrorWithBody(proto.OpErr, []byte(err.Error()))
		return
	}
	resp, err := mp.submit(opFSMEvictInode, val)
	if err != nil {
		p.PacketErrorWithBody(proto.OpAgain, []byte(err.Error()))
		return
	}
	msg := resp.(*InodeResponse)
	p.PacketErrorWithBody(msg.Status, nil)
	return
}

// EvictInode evicts an inode.
func (mp *metaPartition) EvictInodeBatch(req *BatchEvictInodeReq, p *Packet) (err error) {

	if len(req.Inodes) == 0 {
		return nil
	}

	var inodes InodeBatch

	for _, id := range req.Inodes {
		inodes = append(inodes, NewInode(id, 0))
	}

	val, err := inodes.Marshal()
	if err != nil {
		p.PacketErrorWithBody(proto.OpErr, []byte(err.Error()))
		return
	}
	resp, err := mp.submit(opFSMEvictInodeBatch, val)
	if err != nil {
		p.PacketErrorWithBody(proto.OpAgain, []byte(err.Error()))
		return
	}

	status := proto.OpOk
	for _, m := range resp.([]*InodeResponse) {
		if m.Status != proto.OpOk {
			status = m.Status
		}
	}

	p.PacketErrorWithBody(status, nil)
	return
}

// SetAttr set the inode attributes.
func (mp *metaPartition) SetAttr(req *SetattrRequest, reqData []byte, p *Packet) (err error) {
	if mp.verSeq != 0 {
		req.VerSeq = mp.verSeq
		reqData, err = json.Marshal(req)
		if err != nil {
			log.LogErrorf("setattr: marshal err(%v)", err)
			return
		}
	}
	_, err = mp.submit(opFSMSetAttr, reqData)
	if err != nil {
		p.PacketErrorWithBody(proto.OpAgain, []byte(err.Error()))
		return
	}
	log.LogDebugf("action[SetAttr] inode %v ver %v exit", req.Inode, req.VerSeq)
	p.PacketOkReply()
	return
}

// GetInodeTree returns the inode tree.
func (mp *metaPartition) GetInodeTree() *BTree {
	return mp.inodeTree.GetTree()
}

// GetInodeTreeLen returns the inode tree length.
func (mp *metaPartition) GetInodeTreeLen() int {
	if mp.inodeTree == nil {
		return 0
	}
	return mp.inodeTree.Len()
}

func (mp *metaPartition) DeleteInode(req *proto.DeleteInodeRequest, p *Packet) (err error) {
	var bytes = make([]byte, 8)
	binary.BigEndian.PutUint64(bytes, req.Inode)
	_, err = mp.submit(opFSMInternalDeleteInode, bytes)
	if err != nil {
		p.PacketErrorWithBody(proto.OpAgain, []byte(err.Error()))
		return
	}
	p.PacketOkReply()
	return
}

func (mp *metaPartition) DeleteInodeBatch(req *proto.DeleteInodeBatchRequest, p *Packet) (err error) {
	if len(req.Inodes) == 0 {
		return nil
	}

	var inodes InodeBatch

	for _, id := range req.Inodes {
		inodes = append(inodes, NewInode(id, 0))
	}

	encoded, err := inodes.Marshal()
	if err != nil {
		p.PacketErrorWithBody(proto.OpErr, []byte(err.Error()))
		return
	}
	_, err = mp.submit(opFSMInternalDeleteInodeBatch, encoded)
	if err != nil {
		p.PacketErrorWithBody(proto.OpAgain, []byte(err.Error()))
		return
	}
	p.PacketOkReply()
	return
}

// ClearInodeCache clear a inode's cbfs extent but keep ebs extent.
func (mp *metaPartition) ClearInodeCache(req *proto.ClearInodeCacheRequest, p *Packet) (err error) {
	if len(mp.extDelCh) > defaultDelExtentsCnt-100 {
		err = fmt.Errorf("extent del chan full")
		p.PacketErrorWithBody(proto.OpAgain, []byte(err.Error()))
		return
	}

	ino := NewInode(req.Inode, 0)
	val, err := ino.Marshal()
	if err != nil {
		p.PacketErrorWithBody(proto.OpErr, []byte(err.Error()))
		return
	}
	resp, err := mp.submit(opFSMClearInodeCache, val)
	if err != nil {
		p.PacketErrorWithBody(proto.OpAgain, []byte(err.Error()))
		return
	}
	p.PacketErrorWithBody(resp.(uint8), nil)
	return
}

// TxCreateInode returns a new inode.
func (mp *metaPartition) TxCreateInode(req *proto.TxCreateInodeRequest, p *Packet) (err error) {
	var (
		status = proto.OpNotExistErr
		reply  []byte
		resp   interface{}
	)
	inoID, err := mp.nextInodeID()
	if err != nil {
		p.PacketErrorWithBody(proto.OpInodeFullErr, []byte(err.Error()))
		return
	}

	txInfo := req.TxInfo.GetCopy()
	/*if req.TxInfo.TxID == "" && req.TxInfo.TmID == -1 {
		txInfo.TxID = mp.txProcessor.txManager.nextTxID()
		txInfo.TmID = int64(mp.config.PartitionId)
		txInfo.CreateTime = time.Now().Unix()
	} else {
		//todo_tx:RM
	}*/

	if !txInfo.IsInitialized() {
		mp.initTxInfo(txInfo)
	} /* else {
		if txInfo.IsExpired() {
			p.PacketErrorWithBody(proto.OpTxTimeoutErr, nil)
			return
		}
	}*/

	addrs := make([]string, 0)
	for _, peer := range mp.config.Peers {
		//if mp.config.NodeId == peer.ID {
		//	continue
		//}
		addr := strings.Split(peer.Addr, ":")[0] + ":" + mp.manager.metaNode.listen
		addrs = append(addrs, addr)
	}
	members := strings.Join(addrs, ",")

	//mpIp := mp.manager.metaNode.localAddr
	//mpPort := mp.manager.metaNode.listen
	//mpAddr := fmt.Sprintf("%s:%s", mpIp, mpPort)
	//txIno := NewTxInode(mpAddr, inoID, req.Mode, req.PartitionID, txInfo)
	txIno := NewTxInode(members, inoID, req.Mode, req.PartitionID, txInfo)
	log.LogDebugf("NewTxInode: TxInode: %v", txIno)
	txIno.Inode.Uid = req.Uid
	txIno.Inode.Gid = req.Gid
	txIno.Inode.LinkTarget = req.Target

	if defaultQuotaSwitch {
		for _, quotaId := range req.QuotaIds {
			status = mp.mqMgr.IsOverQuota(false, true, quotaId)
			if status != 0 {
				err = errors.New("tx create inode is over quota")
				reply = []byte(err.Error())
				p.PacketErrorWithBody(status, reply)
				return
			}
		}

		qinode := &TxMetaQuotaInode{
			txinode:  txIno,
			quotaIds: req.QuotaIds,
		}
		val, err := qinode.Marshal()
		if err != nil {
			p.PacketErrorWithBody(proto.OpErr, []byte(err.Error()))
			return err
		}
		resp, err = mp.submit(opFSMTxCreateInodeQuota, val)
		if err != nil {
			p.PacketErrorWithBody(proto.OpAgain, []byte(err.Error()))
			return err
		}
	} else {
		val, err := txIno.Marshal()
		if err != nil {
			p.PacketErrorWithBody(proto.OpErr, []byte(err.Error()))
			return err
		}
		resp, err = mp.submit(opFSMTxCreateInode, val)
		if err != nil {
			p.PacketErrorWithBody(proto.OpAgain, []byte(err.Error()))
			return err
		}
	}

	var rstTxInfo *proto.TransactionInfo
	if req.TxInfo.TxID == "" && req.TxInfo.TmID == -1 {
		log.LogDebugf("TxCreateInode: getTransaction")
		rstTxInfo = mp.txProcessor.txManager.getTransaction(txInfo.TxID)
	} else {
		log.LogDebugf("TxCreateInode: getTransaction from req")
		rstTxInfo = req.TxInfo
	}

	if resp == proto.OpOk {
		/*resp := &proto.TxCreateInodeResponse{
			Info:   &proto.InodeInfo{},
			TxInfo: rstTxInfo,
		}
		if txReplyInfo(resp, txIno) {
			status = proto.OpOk
			reply, err = json.Marshal(resp)
			if err != nil {
				status = proto.OpErr
				reply = []byte(err.Error())
			}
		}*/
		resp := txReplyInfo(txIno.Inode, rstTxInfo)
		status = proto.OpOk
		reply, err = json.Marshal(resp)
		if err != nil {
			status = proto.OpErr
			reply = []byte(err.Error())
		}
	}
	p.PacketErrorWithBody(status, reply)
	return
}
