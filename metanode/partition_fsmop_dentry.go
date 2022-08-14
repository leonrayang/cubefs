// Copyright 2018 The Chubao Authors.
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
	"strings"

	"github.com/cubefs/cubefs/proto"
	"github.com/cubefs/cubefs/util/btree"
	"github.com/cubefs/cubefs/util/log"
)

type DentryResponse struct {
	Status uint8
	Msg    *Dentry
}

func NewDentryResponse() *DentryResponse {
	return &DentryResponse{
		Msg: &Dentry{},
	}
}

// Insert a dentry into the dentry tree.
func (mp *metaPartition) fsmCreateDentry(dentry *Dentry,
	forceUpdate bool) (status uint8) {
	status = proto.OpOk
	item := mp.inodeTree.CopyGet(NewInode(dentry.ParentId, 0))
	log.LogDebugf("action[fsmCreateDentry] ParentId [%v] get nil, dentry name [%v], inode [%v], verseq [%v]", dentry.ParentId, dentry.Name, dentry.Inode, dentry.VerSeq)
	var parIno *Inode
	if !forceUpdate {
		if item == nil {
			log.LogErrorf("action[fsmCreateDentry] ParentId [%v] get nil, dentry name [%v], inode [%v]", dentry.ParentId, dentry.Name, dentry.Inode)
			status = proto.OpNotExistErr
			return
		}
		parIno = item.(*Inode)
		if parIno.ShouldDelete() {
			log.LogErrorf("action[fsmCreateDentry] ParentId [%v] get [%v] but should del, dentry name [%v], inode [%v]", dentry.ParentId, parIno, dentry.Name, dentry.Inode)
			status = proto.OpNotExistErr
			return
		}
		if !proto.IsDir(parIno.Type) {
			status = proto.OpArgMismatchErr
			return
		}
	}

	if item, ok := mp.dentryTree.ReplaceOrInsert(dentry, false); !ok {
		//do not allow directories and files to overwrite each
		// other when renaming
		d := item.(*Dentry)

		if d.isDeleted() {
			log.LogDebugf("action[fsmCreateDentry] newest dentry %v be set deleted flag", d)
			d.Inode = dentry.Inode
			d.VerSeq = dentry.VerSeq
			d.Type = dentry.Type
			d.ParentId = dentry.ParentId
			log.LogDebugf("action[fsmCreateDentry.ver] latest dentry already deleted.Now create new one [%v]", dentry)

			if !forceUpdate {
				parIno.IncNLink()
				parIno.SetMtime()
			}
			return
		} else if proto.OsModeType(dentry.Type) != proto.OsModeType(d.Type) {
			status = proto.OpArgMismatchErr
			return
		} else if dentry.ParentId == d.ParentId && strings.Compare(dentry.Name, d.Name) == 0 && dentry.Inode == d.Inode {
			log.LogDebugf("action[fsmCreateDentry.ver] no need repeat create new one [%v]", dentry)
			return
		}
		log.LogErrorf("action[fsmCreateDentry.ver] dentry already exist [%v] and diff with the request [%v]", d, dentry)
		status = proto.OpExistErr

	} else {
		if !forceUpdate {
			parIno.IncNLink()
			parIno.SetMtime()
		}
	}

	return
}

// Query a dentry from the dentry tree with specified dentry info.
func (mp *metaPartition) getDentry(dentry *Dentry) (*Dentry, uint8) {
	status := proto.OpOk
	item := mp.dentryTree.Get(dentry)
	if item == nil {
		status = proto.OpNotExistErr
		return nil, status
	}
	log.LogDebug("action[getDentry] dentry[%v] be set delete flag", dentry)

	den := mp.getDentryByVerSeq(item.(*Dentry), dentry.VerSeq)
	if den != nil && den.VerSeq == dentry.VerSeq {
		return den,  proto.OpOk
	}
	return den, proto.OpNotExistErr

	//verSeq := item.(*Dentry).getVerSeq()
	//if dentry.VerSeq == 0 || verSeq <= dentry.VerSeq {
	//	if item.(*Dentry).isDeleted() {
	//		log.LogDebug("action[getDentry] dentry[%v] be set delete flag", dentry)
	//		status = proto.OpNotExistErr
	//		return nil, status
	//	}
	//	log.LogDebug("action[getDentry] dentry[%v] be geted", item.(*Dentry))
	//	return item.(*Dentry), status
	//}
	//for _, d := range item.(*Dentry).dentryList {
	//	verSeq = d.getVerSeq()
	//	log.LogDebug("action[getDentry] dentry[%v] in the dentry list ", dentry)
	//	if verSeq == dentry.VerSeq {
	//		if d.isDeleted() {
	//			log.LogDebug("action[getDentry] dentry[%v] in the dentry list and be set delete flag", d)
	//			status = proto.OpNotExistErr
	//			return nil, status
	//		}
	//		return item.(*Dentry), status
	//	}
	//	if verSeq < dentry.VerSeq {
	//		return nil, proto.OpNotExistErr
	//	}
	//}
	//return nil, proto.OpNotExistErr
}

// Delete dentry from the dentry tree.
func (mp *metaPartition) fsmDeleteDentry(denParm *Dentry, checkInode bool) (resp *DentryResponse) {

	log.LogDebugf("action[fsmDeleteDentry] dentry(%v)", denParm)
	resp = NewDentryResponse()
	resp.Status = proto.OpOk

	var item interface{}
	if checkInode {
		log.LogDebugf("action[fsmDeleteDentry] dentry %v", denParm)
		item = mp.dentryTree.Execute(func(tree *btree.BTree) interface{} {
			d := tree.CopyGet(denParm)
			if d == nil {
				return nil
			}
			den := d.(*Dentry)
			if den.Inode != denParm.Inode {
				return nil
			}
			if mp.verSeq == 0 {
				return mp.dentryTree.tree.Delete(den)
			}
			return den.deleteVerSnapshot(denParm.VerSeq, mp.verSeq, mp.multiVersionList)
		})
	} else {
		log.LogDebugf("action[fsmDeleteDentry] dentry %v", denParm)
		if mp.verSeq == 0 {
			item = mp.dentryTree.tree.Delete(denParm)
		} else {
			item = mp.dentryTree.Get(denParm)
			if item != nil {
				item = item.(*Dentry).deleteVerSnapshot(denParm.VerSeq, mp.verSeq, mp.multiVersionList)
			}
		}
	}

	if item == nil {
		resp.Status = proto.OpNotExistErr
		log.LogErrorf("action[fsmDeleteDentry] not found dentry %v", denParm)
		return
	} else {
		mp.inodeTree.CopyFind(NewInode(denParm.ParentId, 0),
			func(item BtreeItem) {
				if item != nil { // no matter
					ino := item.(*Inode)
					if !ino.ShouldDelete() {
						item.(*Inode).DecNLink()
						item.(*Inode).SetMtime()
					}
				}
			})
	}
	resp.Msg = item.(*Dentry)
	return
}

// batch Delete dentry from the dentry tree.
func (mp *metaPartition) fsmBatchDeleteDentry(db DentryBatch) []*DentryResponse {
	result := make([]*DentryResponse, 0, len(db))
	for _, dentry := range db {
		result = append(result, mp.fsmDeleteDentry(dentry, true))
	}
	return result
}

func (mp *metaPartition) fsmUpdateDentry(dentry *Dentry) (
	resp *DentryResponse) {
	resp = NewDentryResponse()
	resp.Status = proto.OpOk
	mp.dentryTree.CopyFind(dentry, func(item BtreeItem) {
		if item == nil {
			resp.Status = proto.OpNotExistErr
			return
		}
		d := item.(*Dentry)
		d.Inode, dentry.Inode = dentry.Inode, d.Inode
		resp.Msg = dentry
	})
	return
}

func (mp *metaPartition) getDentryTree() *BTree {
	return mp.dentryTree.GetTree()
}

func (mp *metaPartition) getDentryByVerSeq(dy *Dentry, verSeq uint64) (d *Dentry){
	log.LogInfof("action[getDentryByVerSeq] verseq %v, tmp dentry %v, inode id %v, name %v", verSeq, dy.VerSeq, dy.Inode, dy.Name)
	d, _ =  dy.getVerSnapshotByVer(verSeq)
	return
}

func (mp *metaPartition) readDirOnly(req *ReadDirOnlyReq) (resp *ReadDirOnlyResp) {
	resp = &ReadDirOnlyResp{}
	begDentry := &Dentry{
		ParentId: req.ParentID,
	}
	endDentry := &Dentry{
		ParentId: req.ParentID + 1,
	}
	mp.dentryTree.AscendRange(begDentry, endDentry, func(i BtreeItem) bool {
		if proto.IsDir(i.(*Dentry).Type) {
			d := mp.getDentryByVerSeq(i.(*Dentry), req.VerSeq)
			if d == nil {
				return true
			}
			resp.Children = append(resp.Children, proto.Dentry{
				Inode: d.Inode,
				Type:  d.Type,
				Name:  d.Name,
			})
		}
		return true
	})
	return
}

func (mp *metaPartition) readDir(req *ReadDirReq) (resp *ReadDirResp) {
	resp = &ReadDirResp{}
	begDentry := &Dentry{
		ParentId: req.ParentID,
	}
	endDentry := &Dentry{
		ParentId: req.ParentID + 1,
	}
	mp.dentryTree.AscendRange(begDentry, endDentry, func(i BtreeItem) bool {
		d := mp.getDentryByVerSeq(i.(*Dentry), req.VerSeq)
		if d == nil {
			return true
		}
		resp.Children = append(resp.Children, proto.Dentry{
			Inode: d.Inode,
			Type:  d.Type,
			Name:  d.Name,
		})
		return true
	})
	return
}

// Read dentry from btree by limit count
// if req.Marker == "" and req.Limit == 0, it becomes readDir
// else if req.Marker != "" and req.Limit == 0, return dentries from pid:name to pid+1
// else if req.Marker == "" and req.Limit != 0, return dentries from pid with limit count
// else if req.Marker != "" and req.Limit != 0, return dentries from pid:marker to pid:xxxx with limit count
//
func (mp *metaPartition) readDirLimit(req *ReadDirLimitReq) (resp *ReadDirLimitResp) {
	resp = &ReadDirLimitResp{}
	startDentry := &Dentry{
		ParentId: req.ParentID,
	}
	if len(req.Marker) > 0 {
		startDentry.Name = req.Marker
	}
	endDentry := &Dentry{
		ParentId: req.ParentID + 1,
	}
	mp.dentryTree.AscendRange(startDentry, endDentry, func(i BtreeItem) bool {
		d := mp.getDentryByVerSeq(i.(*Dentry), req.VerSeq)
		if d == nil {
			return true
		}
		resp.Children = append(resp.Children, proto.Dentry{
			Inode: d.Inode,
			Type:  d.Type,
			Name:  d.Name,
		})
		// Limit == 0 means no limit.
		if req.Limit > 0 && uint64(len(resp.Children)) >= req.Limit {
			return false
		}
		return true
	})
	return
}
