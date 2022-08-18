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
func (mp *metaPartition) fsmCreateInode(ino *Inode) (status uint8) {
	status = proto.OpOk
	if _, ok := mp.inodeTree.ReplaceOrInsert(ino, false); !ok {
		status = proto.OpExistErr
	}
	return
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

	log.LogDebugf("action[getInodeByVer] ino %v verseq %v fin", ino.Inode, item.(*Inode).verSeq)
	return
}

func (mp *metaPartition) getInode(ino *Inode) (resp *InodeResponse) {
	resp = NewInodeResponse()
	resp.Status = proto.OpOk

	i := mp.getInodeByVer(ino)
	if i == nil || i.ShouldDelete() {
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

func (mp *metaPartition) getInodeTree() *BTree {
	return mp.inodeTree.GetTree()
}

// Ascend is the wrapper of inodeTree.Ascend
func (mp *metaPartition) Ascend(f func(i BtreeItem) bool) {
	mp.inodeTree.Ascend(f)
}
// normal unlink seq is 0
// snapshot unlink seq is snapshotVersion
// fsmUnlinkInode delete the specified inode from inode tree.
func (mp *metaPartition) fsmUnlinkInode(ino *Inode, verlist []*MetaMultiSnapshotInfo) (resp *InodeResponse) {
	log.LogDebugf("action[fsmUnlinkInode] ino %v", ino)
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

	resp.Msg = inode

	// create a version if the snapshot be depend on
	if ino.verSeq == 0  {
		// if there's no snapshot itself, nor have snapshot after inode's ver then need unlink directly and make no snapshot
		// just move to upper layer, the behavior looks like that the snapshot be dropped
		if len(inode.multiVersions) == 0 {
			var (
				found bool
				rspSeq uint64
			)
			log.LogDebugf("action[fsmUnlinkInode] check if have snapshot depends on the deleitng ino %v (with no snapshot itself) found seq %v and update to %v, verlist %v", ino, inode.verSeq, rspSeq, verlist)
			// no matter verSeq of inode is larger than zero,if not be depended then dropped
			rspSeq, found = inode.getLastestVer(inode.verSeq, true, verlist)
			if !found { // no snapshot depend on this inode
				log.LogDebugf("action[fsmUnlinkInode] no snapshot available depends on ino %v not found seq %v and return, verlist %v", ino, inode.verSeq, verlist)
				inode.DecNLink()
				log.LogDebugf("action[fsmUnlinkInode] inode %v be unlinked", ino.Inode)
				// operate inode directly
				goto end
			}
		}
		if inode.verSeq != mp.verSeq { // need create version
			log.LogDebugf("action[fsmUnlinkInode] need create version.ino %v withSeq %v not equal mp seq %v, verlist %v", ino, inode.verSeq, mp.verSeq, verlist)
			if proto.IsDir(inode.Type) { // dir is all info but inode is part,which is quit different
				inode.CreateVer(mp.verSeq)
				inode.DecNLink()
				log.LogDebugf("action[fsmUnlinkInode] inode %v be unlinked, Dir create ver 1st layer", ino.Inode)
			} else {
				inode.CreateUnlinkVer(mp.verSeq, verlist)
				inode.DecNLink()
				log.LogDebugf("action[fsmUnlinkInode] inode %v be unlinked, File create ver 1st layer", ino.Inode)
			}
			return
		} else {
			log.LogDebugf("action[fsmUnlinkInode] need restore.ino %v withSeq %v equal mp seq, verlist %v", ino, inode.verSeq, verlist)
			// need restore
			if !proto.IsDir(inode.Type) {
				var dIno *Inode
				if ext2Del, dIno = inode.getAndDelVer(ino.verSeq, mp.verSeq, verlist); dIno == nil {
					resp.Status = proto.OpNotExistErr
					log.LogDebugf("action[fsmUnlinkInode] ino %v", ino)
					return
				}
				log.LogDebugf("action[fsmUnlinkInode] inode %v be unlinked, File restore", ino.Inode)
				dIno.DecNLink() // dIno should be inode

			} else {
				log.LogDebugf("action[fsmUnlinkInode] inode %v be unlinked, Dir", ino.Inode)
				inode.DecNLink()
			}
		}
	} else { // means drop snapshot
		log.LogDebugf("action[fsmUnlinkInode] req drop assigned snapshot reqseq %v inode seq %v", ino.verSeq, inode.verSeq)
		if ino.verSeq > inode.verSeq && ino.verSeq != math.MaxUint64 {
			log.LogDebugf("action[fsmUnlinkInode] inode %v unlink not exist snapshot and return do nothing.reqSeq %v larger than inode seq %v",
				ino.Inode, ino.verSeq, inode.verSeq)
			return
		} else {
			log.LogDebugf("action[fsmUnlinkInode] ino %v try search seq %v isdir %v", ino, ino.verSeq, proto.IsDir(inode.Type))
			var dIno *Inode
			if proto.IsDir(inode.Type) { // snapshot dir deletion don't take link into consider, but considers the scope of snapshot contrast to verList
				var idx int
				if dIno, idx = inode.getInoByVer(ino.verSeq, false); dIno == nil {
					resp.Status = proto.OpNotExistErr
					log.LogDebugf("action[fsmUnlinkInode] ino %v not found", ino)
					return
				}
				if idx == 0 {
					// header layer do nothing and be depends on should not be dropped
					log.LogDebugf("action[fsmUnlinkInode] ino %v first layer do nothing", ino)
					return
				}
				// if any alive snapshot in mp dimension exist in seq scope from dino to next ascend neighbor, dio snapshot be keep or else drop
				var endSeq uint64
				realIdx := idx-1
				if realIdx == 0 {
					endSeq = inode.verSeq
				} else {
					endSeq = inode.multiVersions[realIdx-1].verSeq
				}

				log.LogDebugf("action[fsmUnlinkInode] inode %v try drop multiVersion idx %v effective seq scope [%v,%v) ",
					inode.Inode, realIdx, dIno.verSeq, endSeq)

				for vidx, info := range verlist {
					if info.VerSeq >= dIno.verSeq && info.VerSeq < endSeq {
						log.LogDebugf("action[fsmUnlinkInode] inode %v dir layer idx %v still have effective snapshot seq %v.so don't drop", inode.Inode, realIdx, info.VerSeq)
						return
					}
					if info.VerSeq >= endSeq || vidx == len(verlist)-1 {
						log.LogDebugf("action[fsmUnlinkInode] inode %v try drop multiVersion idx %v and return", inode.Inode, realIdx)
						inode.multiVersions = append(inode.multiVersions[:realIdx], inode.multiVersions[realIdx+1:]...)
						return
					}
					log.LogDebugf("action[fsmUnlinkInode] inode %v try drop scope [%v, %v), mp ver %v not suitable", inode.Inode, dIno.verSeq, endSeq, info.VerSeq)
				}
			} else {
				// special case, snapshot is the last one and be depended by upper version,update it's version to the right one
				// ascend search util to the curr unCommit version in the verList
				if ino.verSeq == inode.verSeq /*&& len(inode.multiVersions) == 0*/ {
					if len(verlist) == 0 {
						resp.Status = proto.OpNotExistErr
						log.LogErrorf("action[fsmUnlinkInode] inode %v verlist should be larger than 0, return not found", inode.Inode)
						return
					}

					// just move to upper layer,the request snapshot be dropped
					nVerSeq, found := inode.getLastestVer(inode.verSeq, false, verlist)
					if !found {
						resp.Status = proto.OpNotExistErr
						return
					}
					inode.verSeq = nVerSeq
					return
				} else {
					// don't unlink if no version satisfied
					if ext2Del, dIno = inode.getAndDelVer(ino.verSeq, mp.verSeq, verlist); dIno == nil {
						resp.Status = proto.OpNotExistErr
						log.LogDebugf("action[fsmUnlinkInode] ino %v", ino)
						return
					}
				}
			}
			dIno.DecNLink()
			log.LogDebugf("action[fsmUnlinkInode] inode %v snapshot layer be unlinked", ino.Inode)
		}
	}
end:
	if inode.IsEmptyDir() {
		log.LogDebugf("action[fsmUnlinkInode] ino %v really be deleted, empty dir", ino)
		mp.inodeTree.Delete(inode)
	}

	//Fix#760: when nlink == 0, push into freeList and delay delete inode after 7 days
	if inode.IsTempFile() {
		// all snapshot between create to last deletion cleaned
		if inode.NLink == 0 && len(inode.multiVersions) == 0 {
			log.LogDebugf("action[fsmUnlinkInode] unlink inode %v and push to freeList", inode)
			inode.AccessTime = time.Now().Unix()
			mp.freeList.Push(inode.Inode)
			log.LogDebugf("action[fsmUnlinkInode] ino %v", inode)
		}
	} else {
		log.LogDebugf("action[fsmUnlinkInode] ino %v ext2Del %v", ino, ext2Del)
		mp.extDelCh <- ext2Del
	}
	log.LogDebugf("action[fsmUnlinkInode] ino %v left", ino)
	return
}

// fsmUnlinkInode delete the specified inode from inode tree.
func (mp *metaPartition) fsmUnlinkInodeBatch(ib InodeBatch) (resp []*InodeResponse) {
	for _, ino := range ib {
		resp = append(resp, mp.fsmUnlinkInode(ino, mp.multiVersionList))
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
	eks := ino.Extents.CopyExtents()
	delExtents := ino2.AppendExtents(eks, ino.ModifyTime, mp.volType)
	log.LogInfof("fsmAppendExtents inode(%v) deleteExtents(%v)", ino2.Inode, delExtents)
	mp.extDelCh <- delExtents
	return
}

func (mp *metaPartition) fsmAppendExtentsWithCheck(ino *Inode, isSplit bool) (status uint8) {
	var (
		delExtents []proto.ExtentKey
	)
	status = proto.OpOk

	item := mp.inodeTree.Get(ino)
	if item == nil {
		status = proto.OpNotExistErr
		return
	}
	log.LogDebugf("action[fsmAppendExtentsWithCheck] inode %v hist len %v", ino.Inode, len(ino.multiVersions))

	ino2 := item.(*Inode)
	if ino2.ShouldDelete() {
		status = proto.OpNotExistErr
		return
	}
	var (
		discardExtentKey []proto.ExtentKey
	)
	eks := ino.Extents.CopyExtents()
	if len(eks) < 1 {
		return
	}
	if len(eks) > 1 {
		discardExtentKey = eks[1:]
	}

	log.LogDebugf("action[fsmAppendExtentWithCheck] ino %v isSplit %v ek %v hist len %v", ino2, isSplit, eks[0], len(ino2.multiVersions))
	if !isSplit {
		delExtents, status = ino2.AppendExtentWithCheck(mp.verSeq, ino.verSeq, eks[0], ino.ModifyTime, discardExtentKey, mp.volType)
		if status == proto.OpOk {
			log.LogInfof("action[fsmAppendExtentWithCheck] delExtents [%v]", delExtents)
			mp.extDelCh <- delExtents
		}
		// conflict need delete eks[0], to clear garbage data
		if status == proto.OpConflictExtentsErr {
			log.LogInfof("action[fsmAppendExtentWithCheck] OpConflictExtentsErr [%v]", eks[:1])
			mp.extDelCh <- eks[:1]
		}
	} else {
		// only the ek itself will be moved to level before
		// ino verseq be set with mp ver before submit in case other mp be updated while on flight, which will lead to
		// inconsistent between raft pairs
		delExtents, status = ino2.SplitExtentWithCheck(mp.verSeq, ino.verSeq, eks[0])
		mp.extDelCh <- delExtents
	}

	log.LogInfof("fsmAppendExtentWithCheck inode(%v) ek(%v) deleteExtents(%v) discardExtents(%v) status(%v) isSplit(%v), extents(%v)",
		ino2.Inode, eks[0], delExtents, discardExtentKey, status, isSplit, ino2.Extents.eks)

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

// ino is not point to the member of inodeTree
// it's inode is same with inodeTree,not the extent
// func (mp *metaPartition) fsmDelExtents(ino *Inode) (status uint8) {
// 	status = proto.OpOk
// 	item := mp.inodeTree.CopyGet(ino)
// 	if item == nil {
// 		status = proto.OpNotExistErr
// 		return
// 	}
// 	ino2 := item.(*Inode)
// 	if ino2.ShouldDelete() {
// 		status = proto.OpNotExistErr
// 		return
// 	}
// 	eks := ino.Extents.CopyExtents()
// 	delExtents := ino2.ReplaceExtents(eks, ino.ModifyTime)
// 	log.LogInfof("fsmDelExtents inode(%v) curExtent(%v) delExtents(%v)", ino2.Inode, eks, delExtents)
// 	mp.extDelCh <- delExtents
// 	return
// }

func (mp *metaPartition) fsmExtentsTruncate(ino *Inode) (resp *InodeResponse) {
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
	if proto.IsDir(i.Type) {
		resp.Status = proto.OpArgMismatchErr
		return
	}

	delExtents := i.ExtentsTruncate(ino.Size, ino.ModifyTime)

	// now we should delete the extent
	log.LogInfof("fsmExtentsTruncate inode(%v) exts(%v)", i.Inode, delExtents)
	mp.extDelCh <- delExtents
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
		if i.IsEmptyDir() {
			i.SetDeleteMark()
		}
		return
	}

	if i.IsTempFile() {
		log.LogDebugf("action[fsmEvictInode] inode %v already linke zero and be set mark delete and be put to freelist", ino)
		i.SetDeleteMark()
		if len(i.multiVersions) == 0 {
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
