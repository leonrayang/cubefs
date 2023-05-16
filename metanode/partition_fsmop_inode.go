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
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"math"
	"time"

	"github.com/cubefs/cubefs/proto"
	"github.com/cubefs/cubefs/util/log"
)

type InodeResponse struct {
	Status uint8
	Msg    *Inode
}

func NewInodeResponse() *InodeResponse {
	return &InodeResponse{}
}

// Create and inode and attach it to the inode tree.
func (mp *metaPartition) fsmTxCreateInode(txIno *TxInode, quotaIds []uint32) (status uint8) {
	status = proto.OpOk
	//1.if mpID == -1, register transaction in transaction manager
	//if txIno.TxInfo.TxID != "" && txIno.TxInfo.TmID == -1 {
	_ = mp.txProcessor.txManager.registerTransaction(txIno.TxInfo)
	//}
	//2.register rollback item

	//inodeInfo := mp.txProcessor.txManager.getTxInodeInfo(txIno.TxInfo.TxID, txIno.Inode.Inode)
	inodeInfo, ok := txIno.TxInfo.TxInodeInfos[txIno.Inode.Inode]
	if !ok {
		status = proto.OpTxInodeInfoNotExistErr
		return
	}
	rbInode := NewTxRollbackInode(txIno.Inode, quotaIds, inodeInfo, TxDelete)
	if status = mp.txProcessor.txResource.addTxRollbackInode(rbInode); status != proto.OpOk {
		//status = proto.OpTxConflictErr
		return
	}
	//3.insert inode in inode tree
	return mp.fsmCreateInode(txIno.Inode)
}

// Create and inode and attach it to the inode tree.
func (mp *metaPartition) fsmCreateInode(ino *Inode) (status uint8) {
	log.LogDebugf("action[fsmCreateInode] inode  %v be created", ino.Inode)
	if status = mp.uidManager.addUidSpace(ino.Uid, ino.Inode, nil); status != proto.OpOk {
		return
	}
	log.LogDebugf("action[fsmCreateInode] inode  %v be created", ino)

	status = proto.OpOk
	if _, ok := mp.inodeTree.ReplaceOrInsert(ino, false); !ok {
		status = proto.OpExistErr
	}

	return
}

func (mp *metaPartition) fsmTxCreateLinkInode(txIno *TxInode) (resp *InodeResponse) {
	resp = NewInodeResponse()
	resp.Status = proto.OpOk
	//1.if mpID == -1, register transaction in transaction manager
	//if txIno.TxInfo.TxID != "" && txIno.TxInfo.TmID == -1 {
	_ = mp.txProcessor.txManager.registerTransaction(txIno.TxInfo)
	//}

	//2.register rollback item
	inodeInfo, ok := txIno.TxInfo.TxInodeInfos[txIno.Inode.Inode]
	if !ok {
		resp.Status = proto.OpTxInodeInfoNotExistErr
		return
	}

	rbInode := NewTxRollbackInode(txIno.Inode, []uint32{}, inodeInfo, TxUpdate)
	if resp.Status = mp.txProcessor.txResource.addTxRollbackInode(rbInode); resp.Status != proto.OpOk {
		//resp.Status = proto.OpTxConflictErr
		return
	}

	return mp.fsmCreateLinkInode(txIno.Inode)
}

func (mp *metaPartition) fsmCreateLinkInode(ino *Inode) (resp *InodeResponse) {
	resp = NewInodeResponse()
	resp.Status = proto.OpOk
	item := mp.inodeTree.CopyGet(ino)
	if item == nil {
		resp.Status = proto.OpNotExistErr
		return
	}
	i := item.(*Inode)
	if i.ShouldDelete() {
		resp.Status = proto.OpNotExistErr
		return
	}

	i.IncNLink(mp.verSeq)
	resp.Msg = i
	return
}

func (mp *metaPartition) getInodeByVer(ino *Inode) (i *Inode) {
	item := mp.inodeTree.Get(ino)
	if item == nil {
		log.LogDebugf("action[getInodeByVer] not found ino %v verseq %v", ino.Inode, ino.verSeq)
		return
	}
	log.LogDebugf("action[getInodeByVer] ino %v verseq %v hist len %v request ino ver %v",
		ino.Inode, item.(*Inode).verSeq, len(item.(*Inode).multiVersions), ino.verSeq)
	i, _ = item.(*Inode).getInoByVer(ino.verSeq, false)

	log.LogDebugf("action[getInodeByVer] ino %v verseq %v fin,i %v", ino.Inode, item.(*Inode).verSeq, i)
	return
}

func (mp *metaPartition) getInodeTopLayer(ino *Inode) (resp *InodeResponse) {
	resp = NewInodeResponse()
	resp.Status = proto.OpOk

	item := mp.inodeTree.Get(ino)
	if item == nil {
		resp.Status = proto.OpNotExistErr
		log.LogDebugf("action[getInodeTopLayer] not found ino %v verseq %v", ino.Inode, ino.verSeq)
		return
	}
	i := item.(*Inode)
	ctime := Now.GetCurrentTime().Unix()
	/*
	 * FIXME: not protected by lock yet, since nothing is depending on atime.
	 * Shall add inode lock in the future.
	 */
	if ctime > i.AccessTime {
		i.AccessTime = ctime
	}

	resp.Msg = i
	return
}

func (mp *metaPartition) getInode(ino *Inode, listAll bool) (resp *InodeResponse) {
	resp = NewInodeResponse()
	resp.Status = proto.OpOk

	i := mp.getInodeByVer(ino)
	if i == nil || (listAll == false && i.ShouldDelete()) {
		log.LogDebugf("action[getInode] ino  %v not found", ino)
		resp.Status = proto.OpNotExistErr
		return
	}

	ctime := Now.GetCurrentTime().Unix()
	/*
	 * FIXME: not protected by lock yet, since nothing is depending on atime.
	 * Shall add inode lock in the future.
	 */
	if ctime > i.AccessTime {
		i.AccessTime = ctime
	}

	resp.Msg = i
	return
}

func (mp *metaPartition) hasInode(ino *Inode) (ok bool) {
	item := mp.inodeTree.Get(ino)
	if item == nil {
		return
	}
	i := mp.getInodeByVer(ino)
	if i == nil || i.ShouldDelete() {
		return
	}
	ok = true
	return
}

// Ascend is the wrapper of inodeTree.Ascend
func (mp *metaPartition) Ascend(f func(i BtreeItem) bool) {
	mp.inodeTree.Ascend(f)
}

func (mp *metaPartition) fsmTxUnlinkInode(txIno *TxInode) (resp *InodeResponse) {
	resp = NewInodeResponse()
	resp.Status = proto.OpOk

	defer func() {
		if txIno.TxInfo.TxType == proto.TxTypeRename && resp.Status == proto.OpOk {
			mp.fsmEvictInode(txIno.Inode)
		}
	}()

	//1.if mpID == -1, register transaction in transaction manager
	//if txIno.TxInfo.TxID != "" && txIno.TxInfo.TmID == -1 {
	_ = mp.txProcessor.txManager.registerTransaction(txIno.TxInfo)
	//}
	//2.register rollback item
	inodeInfo, ok := txIno.TxInfo.TxInodeInfos[txIno.Inode.Inode]
	if !ok {
		resp.Status = proto.OpTxInodeInfoNotExistErr
		return
	}
	var quotaIds []uint32
	quotaIds, _ = mp.isExistQuota(txIno.Inode.Inode)

	rbInode := NewTxRollbackInode(txIno.Inode, quotaIds, inodeInfo, TxAdd)
	if resp.Status = mp.txProcessor.txResource.addTxRollbackInode(rbInode); resp.Status != proto.OpOk {
		//resp.Status = proto.OpTxConflictErr
		return
	}

	return mp.fsmUnlinkInode(txIno.Inode)
}

// normal unlink seq is 0
// snapshot unlink seq is snapshotVersion
// fsmUnlinkInode delete the specified inode from inode tree.
func (mp *metaPartition) fsmUnlinkInode(ino *Inode) (resp *InodeResponse) {
	log.LogDebugf("action[fsmUnlinkInode] pa ino %v", ino)
	var (
		ext2Del []proto.ExtentKey
	)

	resp = NewInodeResponse()
	resp.Status = proto.OpOk
	item := mp.inodeTree.CopyGet(ino)
	if item == nil {
		log.LogDebugf("action[fsmUnlinkInode] ino %v", ino)
		resp.Status = proto.OpNotExistErr
		return
	}
	inode := item.(*Inode)
	if ino.verSeq == 0 && inode.ShouldDelete() {
		log.LogDebugf("action[fsmUnlinkInode] ino %v", ino)
		resp.Status = proto.OpNotExistErr
		return
	}

	topLayerEmpty := inode.IsTopLayerEmptyDir()

	resp.Msg = inode
	log.LogDebugf("action[fsmUnlinkInode] get inode %v", inode)
	var (
		doMore bool
		status = proto.OpOk
	)

	if ino.verSeq == 0 {
		ext2Del, doMore, status = inode.unlinkTopLayer(ino, mp.verSeq, mp.multiVersionList)
	} else { // means drop snapshot
		log.LogDebugf("action[fsmUnlinkInode] req drop assigned snapshot reqseq %v inode seq %v", ino.verSeq, inode.verSeq)
		if ino.verSeq > inode.verSeq && ino.verSeq != math.MaxUint64 {
			log.LogDebugf("action[fsmUnlinkInode] inode %v unlink not exist snapshot and return do nothing.reqSeq %v larger than inode seq %v",
				ino.Inode, ino.verSeq, inode.verSeq)
			return
		} else {
			ext2Del, doMore, status = inode.unlinkVerInList(ino, mp.verSeq, mp.multiVersionList)
		}
	}
	if !doMore {
		resp.Status = status
		return
	}

	if topLayerEmpty && inode.IsEmptyDirAndNoSnapshot() {
		log.LogDebugf("action[fsmUnlinkInode] ino %v really be deleted, empty dir", ino)
		mp.inodeTree.Delete(inode)
		mp.updateUsedInfo(0, -1, inode.Inode)
	}

	//Fix#760: when nlink == 0, push into freeList and delay delete inode after 7 days
	if inode.IsTempFile() {
		mp.updateUsedInfo(-1*int64(inode.Size), -1, inode.Inode)
		inode.DoWriteFunc(func() {
			if inode.NLink == 0 {
				inode.AccessTime = time.Now().Unix()
				mp.freeList.Push(inode.Inode)
				mp.uidManager.doMinusUidSpace(inode.Uid, inode.Inode, inode.Size)
			}
		})

		// all snapshot between create to last deletion cleaned
		if inode.NLink == 0 && len(inode.multiVersions) == 0 {
			log.LogDebugf("action[fsmUnlinkInode] unlink inode %v and push to freeList", inode)
			inode.AccessTime = time.Now().Unix()
			mp.freeList.Push(inode.Inode)
			log.LogDebugf("action[fsmUnlinkInode] ino %v", inode)
		}
	}

	if len(ext2Del) > 0 {
		log.LogDebugf("action[fsmUnlinkInode] ino %v ext2Del %v", ino, ext2Del)
		mp.extDelCh <- ext2Del
	}
	log.LogDebugf("action[fsmUnlinkInode] ino %v left", ino)
	return
}

// fsmUnlinkInode delete the specified inode from inode tree.
func (mp *metaPartition) fsmUnlinkInodeBatch(ib InodeBatch) (resp []*InodeResponse) {
	for _, ino := range ib {
		resp = append(resp, mp.fsmUnlinkInode(ino))
	}
	return
}

func (mp *metaPartition) internalHasInode(ino *Inode) bool {
	return mp.inodeTree.Has(ino)
}

func (mp *metaPartition) internalDelete(val []byte) (err error) {
	if len(val) == 0 {
		return
	}
	buf := bytes.NewBuffer(val)
	ino := NewInode(0, 0)
	for {
		err = binary.Read(buf, binary.BigEndian, &ino.Inode)
		if err != nil {
			if err == io.EOF {
				err = nil
				return
			}
			return
		}
		log.LogDebugf("internalDelete: received internal delete: partitionID(%v) inode(%v)",
			mp.config.PartitionId, ino.Inode)
		mp.internalDeleteInode(ino)
	}
}

func (mp *metaPartition) internalDeleteBatch(val []byte) error {
	if len(val) == 0 {
		return nil
	}
	inodes, err := InodeBatchUnmarshal(val)
	if err != nil {
		return nil
	}

	for _, ino := range inodes {
		log.LogDebugf("internalDelete: received internal delete: partitionID(%v) inode(%v)",
			mp.config.PartitionId, ino.Inode)
		mp.internalDeleteInode(ino)
	}

	return nil
}

func (mp *metaPartition) internalDeleteInode(ino *Inode) {
	log.LogDebugf("action[internalDeleteInode] ino %v really be deleted", ino)
	mp.inodeTree.Delete(ino)
	mp.freeList.Remove(ino.Inode)
	mp.extendTree.Delete(&Extend{inode: ino.Inode}) // Also delete extend attribute.
	return
}

func (mp *metaPartition) fsmAppendExtents(ino *Inode) (status uint8) {
	status = proto.OpOk
	item := mp.inodeTree.CopyGet(ino)
	if item == nil {
		status = proto.OpNotExistErr
		return
	}
	ino2 := item.(*Inode)
	if ino2.ShouldDelete() {
		status = proto.OpNotExistErr
		return
	}
	oldSize := int64(ino2.Size)
	eks := ino.Extents.CopyExtents()
	delExtents := ino2.AppendExtents(eks, ino.ModifyTime, mp.volType)
	if status = mp.uidManager.addUidSpace(ino2.Uid, ino2.Inode, eks); status != proto.OpOk {
		mp.extDelCh <- eks
		return
	}
	mp.updateUsedInfo(int64(ino2.Size)-oldSize, 0, ino2.Inode)
	log.LogInfof("fsmAppendExtents inode(%v) deleteExtents(%v)", ino2.Inode, delExtents)
	mp.uidManager.minusUidSpace(ino2.Uid, ino2.Inode, delExtents)
	mp.extDelCh <- delExtents
	return
}

func (mp *metaPartition) fsmAppendExtentsWithCheck(ino *Inode, isSplit bool) (status uint8) {
	var (
		delExtents []proto.ExtentKey
	)
	status = proto.OpOk
	log.LogInfof("fsmAppendExtentsWithCheck ino %v", ino.Inode)
	item := mp.inodeTree.CopyGet(ino)

	if item == nil {
		status = proto.OpNotExistErr
		return
	}

	ino2 := item.(*Inode)
	log.LogDebugf("action[fsmAppendExtentsWithCheck] inode %v hist len %v", ino2.Inode, len(ino2.multiVersions))
	if ino2.ShouldDelete() {
		status = proto.OpNotExistErr
		return
	}
	var (
		discardExtentKey []proto.ExtentKey
	)
	oldSize := int64(ino2.Size)
	eks := ino.Extents.CopyExtents()
	log.LogDebugf("action[fsmAppendExtentsWithCheck] inode %v hist len %v,eks %v", ino2.Inode, len(ino2.multiVersions), eks)
	if len(eks) < 1 {
		return
	}
	if len(eks) > 1 {
		discardExtentKey = eks[1:]
	}

	if status = mp.uidManager.addUidSpace(ino2.Uid, ino2.Inode, eks[:1]); status != proto.OpOk {
		log.LogErrorf("fsmAppendExtentsWithCheck.addUidSpace status %v", status)
		return
	}

	log.LogDebugf("action[fsmAppendExtentsWithCheck] ino %v isSplit %v ek %v hist len %v", ino2.Inode, isSplit, eks[0], len(ino2.multiVersions))

	if !isSplit {
		delExtents, status = ino2.AppendExtentWithCheck(mp.verSeq, mp.multiVersionList, ino.verSeq, eks[0], ino.ModifyTime, discardExtentKey, mp.volType)
		if status == proto.OpOk {
			log.LogInfof("action[fsmAppendExtentsWithCheck] delExtents [%v]", delExtents)
			mp.extDelCh <- delExtents
		}
		// conflict need delete eks[0], to clear garbage data
		if status == proto.OpConflictExtentsErr {
			log.LogInfof("action[fsmAppendExtentsWithCheck] OpConflictExtentsErr [%v]", eks[:1])
			mp.extDelCh <- eks[:1]
		}
	} else {
		// only the ek itself will be moved to level before
		// ino verseq be set with mp ver before submit in case other mp be updated while on flight, which will lead to
		// inconsistent between raft pairs
		delExtents, status = ino2.SplitExtentWithCheck(mp.verSeq, mp.multiVersionList, ino.verSeq, eks[0], ino.ModifyTime, mp.volType)
		mp.extDelCh <- delExtents
		mp.uidManager.minusUidSpace(ino2.Uid, ino2.Inode, delExtents)
	}

	// conflict need delete eks[0], to clear garbage data
	if status == proto.OpConflictExtentsErr {
		log.LogInfof("action[fsmAppendExtentsWithCheck] OpConflictExtentsErr [%v]", eks[:1])
		mp.extDelCh <- eks[:1]
		mp.uidManager.minusUidSpace(ino2.Uid, ino2.Inode, eks[:1])
		log.LogDebugf("fsmAppendExtentsWithCheck delExtents inode(%v) ek(%v)", delExtents)

	}

	log.LogInfof("fsmAppendExtentWithCheck inode(%v) ek(%v) deleteExtents(%v) discardExtents(%v) status(%v) isSplit(%v)",
		ino2.Inode, eks[0], delExtents, discardExtentKey, status, isSplit)

	mp.updateUsedInfo(int64(ino2.Size)-oldSize, 0, ino2.Inode)
	log.LogInfof("fsmAppendExtentWithCheck inode(%v) ek(%v) deleteExtents(%v) discardExtents(%v) status(%v)", ino2.Inode, eks[0], delExtents, discardExtentKey, status)

	return
}

func (mp *metaPartition) fsmAppendObjExtents(ino *Inode) (status uint8) {
	status = proto.OpOk
	item := mp.inodeTree.CopyGet(ino)
	if item == nil {
		status = proto.OpNotExistErr
		return
	}

	inode := item.(*Inode)
	if inode.ShouldDelete() {
		status = proto.OpNotExistErr
		return
	}

	eks := ino.ObjExtents.CopyExtents()
	err := inode.AppendObjExtents(eks, ino.ModifyTime)

	// if err is not nil, means obj eks exist overlap.
	if err != nil {
		log.LogErrorf("fsmAppendExtents inode(%v) err(%v)", inode.Inode, err)
		status = proto.OpConflictExtentsErr
	}
	return
}

func (mp *metaPartition) fsmExtentsTruncate(ino *Inode) (resp *InodeResponse) {
	var err error
	resp = NewInodeResponse()
	log.LogDebugf("fsmExtentsTruncate. req ino %v", ino)
	resp.Status = proto.OpOk
	item := mp.inodeTree.Get(ino)
	if item == nil {
		resp.Status = proto.OpNotExistErr
		return
	}
	i := item.(*Inode)
	if i.ShouldDelete() {
		resp.Status = proto.OpNotExistErr
		return
	}
	if proto.IsDir(i.Type) {
		resp.Status = proto.OpArgMismatchErr
		return
	}

	doOnLastKey := func(lastKey *proto.ExtentKey) {
		var eks []proto.ExtentKey
		eks = append(eks, *lastKey)
		mp.uidManager.minusUidSpace(i.Uid, i.Inode, eks)
	}
	if i.verSeq != mp.verSeq {
		i.CreateVer(mp.verSeq)
	}
	i.Lock()
	defer i.Unlock()

	if err = i.CreateLowerVersion(i.verSeq, mp.multiVersionList); err != nil {
		return
	}
	oldSize := int64(i.Size)
	delExtents := i.ExtentsTruncate(ino.Size, ino.ModifyTime, doOnLastKey)

	if len(delExtents) == 0 {
		return
	}

	if delExtents, err = i.RestoreExts2NextLayer(delExtents, mp.verSeq, 0); err != nil {
		panic("RestoreExts2NextLayer should not be error")
	}
	mp.updateUsedInfo(int64(i.Size)-oldSize, 0, i.Inode)

	// now we should delete the extent
	log.LogInfof("fsmExtentsTruncate inode(%v) exts(%v)", i.Inode, delExtents)
	mp.extDelCh <- delExtents
	mp.uidManager.minusUidSpace(i.Uid, i.Inode, delExtents)
	return
}

func (mp *metaPartition) fsmEvictInode(ino *Inode) (resp *InodeResponse) {
	resp = NewInodeResponse()
	log.LogDebugf("action[fsmEvictInode] inode %v", ino)
	resp.Status = proto.OpOk
	item := mp.inodeTree.CopyGet(ino)
	if item == nil {
		resp.Status = proto.OpNotExistErr
		return
	}
	i := item.(*Inode)
	if i.ShouldDelete() {
		log.LogDebugf("action[fsmEvictInode] inode %v already be mark delete", ino)
		return
	}
	if proto.IsDir(i.Type) {
		if i.IsEmptyDirAndNoSnapshot() {
			i.SetDeleteMark()
		}
		return
	}

	if i.IsTempFile() {
		log.LogDebugf("action[fsmEvictInode] inode %v already linke zero and be set mark delete and be put to freelist", ino)
		i.SetDeleteMark()
		if i.isEmptyVerList() {
			mp.freeList.Push(i.Inode)
		}
	}
	return
}

func (mp *metaPartition) fsmBatchEvictInode(ib InodeBatch) (resp []*InodeResponse) {
	for _, ino := range ib {
		resp = append(resp, mp.fsmEvictInode(ino))
	}
	return
}

func (mp *metaPartition) checkAndInsertFreeList(ino *Inode) {
	if proto.IsDir(ino.Type) {
		return
	}
	if ino.ShouldDelete() {
		mp.freeList.Push(ino.Inode)
	} else if ino.IsTempFile() {
		ino.AccessTime = time.Now().Unix()
		mp.freeList.Push(ino.Inode)
	}
}

func (mp *metaPartition) fsmSetAttr(req *SetattrRequest) (err error) {
	log.LogDebugf("action[fsmSetAttr] req %v", req)
	ino := NewInode(req.Inode, req.Mode)
	item := mp.inodeTree.CopyGet(ino)
	if item == nil {
		return
	}
	ino = item.(*Inode)
	if ino.ShouldDelete() {
		return
	}
	ino.SetAttr(req)
	return
}

// fsmExtentsEmpty only use in datalake situation
func (mp *metaPartition) fsmExtentsEmpty(ino *Inode) (status uint8) {
	status = proto.OpOk
	item := mp.inodeTree.CopyGet(ino)
	if item == nil {
		status = proto.OpNotExistErr
		return
	}
	i := item.(*Inode)
	if i.ShouldDelete() {
		status = proto.OpNotExistErr
		return
	}
	if proto.IsDir(i.Type) {
		status = proto.OpArgMismatchErr
		return
	}
	log.LogDebugf("action[fsmExtentsEmpty] mp(%d) ino [%v],eks len [%v]", mp.config.PartitionId, ino.Inode, len(i.Extents.eks))
	tinyEks := i.CopyTinyExtents()
	log.LogDebugf("action[fsmExtentsEmpty] mp(%d) ino [%v],eks tiny len [%v]", mp.config.PartitionId, ino.Inode, len(tinyEks))

	if len(tinyEks) > 0 {
		mp.extDelCh <- tinyEks
		mp.uidManager.minusUidSpace(i.Uid, i.Inode, tinyEks)
		log.LogDebugf("fsmExtentsEmpty mp(%d) inode(%d) tinyEks(%v)", mp.config.PartitionId, ino.Inode, tinyEks)
	}

	i.EmptyExtents(ino.ModifyTime)

	return
}

// fsmExtentsEmpty only use in datalake situation
func (mp *metaPartition) fsmDelVerExtents(ino *Inode) (status uint8) {
	status = proto.OpOk
	item := mp.inodeTree.CopyGet(ino)
	if item == nil {
		status = proto.OpNotExistErr
		return
	}
	i := item.(*Inode)
	if i.ShouldDelete() {
		status = proto.OpNotExistErr
		return
	}
	if proto.IsDir(i.Type) {
		status = proto.OpArgMismatchErr
		return
	}
	log.LogDebugf("action[fsmExtentsEmpty] mp(%d) ino [%v],eks len [%v]", mp.config.PartitionId, ino.Inode, len(i.Extents.eks))
	tinyEks := i.CopyTinyExtents()
	log.LogDebugf("action[fsmExtentsEmpty] mp(%d) ino [%v],eks tiny len [%v]", mp.config.PartitionId, ino.Inode, len(tinyEks))

	if len(tinyEks) > 0 {
		mp.extDelCh <- tinyEks
		log.LogDebugf("fsmExtentsEmpty mp(%d) inode(%d) tinyEks(%v)", mp.config.PartitionId, ino.Inode, tinyEks)
	}

	i.EmptyExtents(ino.ModifyTime)

	return
}

func (mp *metaPartition) fsmClearInodeCache(ino *Inode) (status uint8) {
	status = proto.OpOk
	item := mp.inodeTree.Get(ino)
	if item == nil {
		status = proto.OpNotExistErr
		return
	}
	ino2 := item.(*Inode)
	if ino2.ShouldDelete() {
		status = proto.OpNotExistErr
		return
	}
	delExtents := ino2.EmptyExtents(ino.ModifyTime)
	log.LogInfof("fsmClearInodeCache inode(%v) delExtents(%v)", ino2.Inode, delExtents)
	if len(delExtents) > 0 {
		mp.extDelCh <- delExtents
	}
	return
}

// attion: unmarshal error will disard extent
func (mp *metaPartition) fsmSendToChan(val []byte, v3 bool) (status uint8) {
	sortExtents := NewSortedExtents()
	// ek for del don't need version info
	err := sortExtents.UnmarshalBinary(val, v3)
	if err != nil {
		panic(fmt.Errorf("[fsmDelExtents] unmarshal sortExtents error, mp(%d), err(%s)", mp.config.PartitionId, err.Error()))
	}

	log.LogInfof("fsmDelExtents mp(%d) delExtents(%v)", mp.config.PartitionId, len(sortExtents.eks))
	mp.extDelCh <- sortExtents.eks
	return
}
