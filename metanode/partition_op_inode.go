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
	"github.com/cubefs/cubefs/util/log"
	"net/url"
	"time"

	"github.com/cubefs/cubefs/proto"
)

func replyInfoNoCheck(info *proto.InodeInfo, ino *Inode) bool {
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

func replyInfo(info *proto.InodeInfo, ino *Inode) bool {
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
	return true
}

// CreateInode returns a new inode.
func (mp *metaPartition) CreateInode(req *CreateInoReq, p *Packet) (err error) {
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
	log.LogDebugf("action[CreateInode] mp[%v] %v with seq %v", mp.config.PartitionId, inoID, mp.verSeq)
	val, err := ino.Marshal()
	if err != nil {
		p.PacketErrorWithBody(proto.OpErr, []byte(err.Error()))
		return
	}
	resp, err := mp.submit(opFSMCreateInode, val)
	if err != nil {
		p.PacketErrorWithBody(proto.OpAgain, []byte(err.Error()))
		return
	}
	var (
		status = proto.OpNotExistErr
		reply  []byte
	)
	if resp.(uint8) == proto.OpOk {
		resp := &CreateInoResp{
			Info: &proto.InodeInfo{},
		}
		if replyInfo(resp.Info, ino) {
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
			replyInfo(resp.Info, msg.Msg)
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
	if req.DenVerSeq == item.(*Inode).verSeq {
		ino.Flag |= InodeDelTop
	}

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
		replyInfo(info, ir.Msg)
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
func (mp *metaPartition) InodeGetSplitEk(req *InodeGetSplitReq, p *Packet) (err error) {
	ino := NewInode(req.Inode, 0)
	ino.verSeq = req.VerSeq
	getAllVerInfo := req.VerAll
	retMsg := mp.getInode(ino, getAllVerInfo)

	log.LogDebugf("action[InodeGetSplitEk] %v seq %v retMsg.Status %v, getAllVerInfo %v",
		ino.Inode, req.VerSeq, retMsg.Status, getAllVerInfo)

	ino = retMsg.Msg
	var (
		reply  []byte
		status = proto.OpNotExistErr
	)
	if retMsg.Status == proto.OpOk {
		resp := &proto.InodeGetSplitResponse{
			Info: &proto.InodeSplitInfo{
				Inode: ino.Inode,
				VerSeq: ino.verSeq,
			},
		}
		if retMsg.Msg.ekRefMap != nil {
			retMsg.Msg.ekRefMap.Range(func(key, value interface{}) bool {
				dpID, extID := proto.ParseFromId(key.(uint64))
				resp.Info.SplitArr = append(resp.Info.SplitArr, proto.SimpleExtInfo{
					ID:key.(uint64),
					PartitionID: uint32(dpID),
					ExtentID: uint32(extID),
				})
				return true
			})
		}
		log.LogDebugf("action[InodeGetSplitEk] %v seq %v retMsg.Status %v, getAllVerInfo %v",
			ino.Inode, req.VerSeq, retMsg.Status, getAllVerInfo)
		status = proto.OpOk
		reply, err = json.Marshal(resp)
		if err != nil {
			log.LogDebugf("action[InodeGetSplitEk] %v seq %v retMsg.Status %v, getAllVerInfo %v",
				ino.Inode, req.VerSeq, retMsg.Status, getAllVerInfo)
			status = proto.OpErr
			reply = []byte(err.Error())
		}
		log.LogDebugf("action[InodeGetSplitEk] %v seq %v retMsg.Status %v, getAllVerInfo %v",
			ino.Inode, req.VerSeq, retMsg.Status, getAllVerInfo)
	}
	log.LogDebugf("action[InodeGetSplitEk] %v seq %v retMsg.Status %v, getAllVerInfo %v",
		ino.Inode, req.VerSeq, retMsg.Status, getAllVerInfo)
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
	if retMsg.Status == proto.OpOk {
		resp := &proto.InodeGetResponse{
			Info: &proto.InodeInfo{},
		}
		if getAllVerInfo {
			replyInfoNoCheck(resp.Info, retMsg.Msg)
		} else {
			if !replyInfo(resp.Info, retMsg.Msg) {
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

	resp := &proto.BatchInodeGetResponse{}
	ino := NewInode(0, 0)
	for _, inoId := range req.Inodes {
		ino.Inode = inoId
		ino.verSeq = req.VerSeq
		retMsg := mp.getInode(ino, false)
		if retMsg.Status == proto.OpOk {
			inoInfo := &proto.InodeInfo{}
			if replyInfo(inoInfo, retMsg.Msg) {
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
		if replyInfo(resp.Info, retMsg.Msg) {
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
