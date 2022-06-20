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
	"bytes"
	"encoding/binary"
	"fmt"
	"github.com/cubefs/cubefs/util/log"
)

// Dentry wraps necessary properties of the `dentry` information in file system.
// Marshal exporterKey:
//  +-------+----------+------+
//  | item  | ParentId | Name |
//  +-------+----------+------+
//  | bytes |    8     | rest |
//  +-------+----------+------+
// Marshal value:
//  +-------+-------+------+
//  | item  | Inode | Type |
//  +-------+-------+------+
//  | bytes |   8   |   4  |
//  +-------+-------+------+
// Marshal entity:
//  +-------+-----------+--------------+-----------+--------------+
//  | item  | KeyLength | MarshaledKey | ValLength | MarshaledVal |
//  +-------+-----------+--------------+-----------+--------------+
//  | bytes |     4     |   KeyLength  |     4     |   ValLength  |
//  +-------+-----------+--------------+-----------+--------------+
type Dentry struct {
	ParentId uint64 // FileID value of the parent inode.
	Name     string // Name of the current dentry.
	Inode    uint64 // FileID value of the current inode.
	Type     uint32
	//snapshot
	VerSeq     uint64
	dentryList DentryBatch
}

func (d *Dentry) String() string {
	str := fmt.Sprintf("dentry(name:[%v],parentId:[%v],inode:[%v],type:[%v],seq:[%v],dentryList_len[%v]",
		d.ParentId, d.Name, d.Inode, d.Type, d.VerSeq, len(d.dentryList))

	for idx, den := range d.dentryList {
		str += fmt.Sprintf("idx:%v,content(%v)", idx, den)
	}
	return str
}

type DentryBatch []*Dentry
// todo(leon chang), buffer need alloc first before and write directly consider the space and performance
// Marshal marshals a dentry into a byte array.
func (d *Dentry) Marshal() (result []byte, err error) {
	keyBytes := d.MarshalKey()
	valBytes := d.MarshalValue()
	keyLen := uint32(len(keyBytes))
	valLen := uint32(len(valBytes))
	buff := bytes.NewBuffer(make([]byte, 0))
	buff.Grow(int(keyLen+valLen+8))

	log.LogInfof("action[dentry.Marshal] dentry name %v inode %v parent %v seq %v keyLen  %v valLen %v total len %v",
		d.Name, d.Inode, d.ParentId, d.VerSeq, keyLen, valLen, int(keyLen+valLen+8))

	if err = binary.Write(buff, binary.BigEndian, keyLen); err != nil {
		return
	}
	if _, err = buff.Write(keyBytes); err != nil {
		return
	}
	if err = binary.Write(buff, binary.BigEndian, valLen); err != nil {

	}
	if _, err = buff.Write(valBytes); err != nil {
		return
	}
	result = buff.Bytes()
	return
}

// Unmarshal unmarshals the dentry from a byte array.
func (d *Dentry) Unmarshal(raw []byte) (err error) {
	var (
		keyLen uint32
		valLen uint32
	)
	buff := bytes.NewBuffer(raw)
	if err = binary.Read(buff, binary.BigEndian, &keyLen); err != nil {
		return
	}
	keyBytes := make([]byte, keyLen)
	if _, err = buff.Read(keyBytes); err != nil {
		return
	}
	if err = d.UnmarshalKey(keyBytes); err != nil {
		return
	}
	if err = binary.Read(buff, binary.BigEndian, &valLen); err != nil {
		return
	}
	valBytes := make([]byte, valLen)
	if _, err = buff.Read(valBytes); err != nil {
		return
	}
	err = d.UnmarshalValue(valBytes)
	return
}

// Marshal marshals the dentryBatch into a byte array.
func (d DentryBatch) Marshal() ([]byte, error) {
	buff := bytes.NewBuffer(make([]byte, 0))
	if err := binary.Write(buff, binary.BigEndian, uint32(len(d))); err != nil {
		return nil, err
	}
	for _, dentry := range d {
		bs, err := dentry.Marshal()
		if err != nil {
			return nil, err
		}
		if err = binary.Write(buff, binary.BigEndian, uint32(len(bs))); err != nil {
			return nil, err
		}
		if _, err := buff.Write(bs); err != nil {
			return nil, err
		}
	}
	return buff.Bytes(), nil
}

// Unmarshal unmarshals the dentryBatch.
func DentryBatchUnmarshal(raw []byte) (DentryBatch, error) {
	buff := bytes.NewBuffer(raw)
	var batchLen uint32
	if err := binary.Read(buff, binary.BigEndian, &batchLen); err != nil {
		return nil, err
	}

	result := make(DentryBatch, 0, int(batchLen))

	var dataLen uint32
	for j := 0; j < int(batchLen); j++ {
		if err := binary.Read(buff, binary.BigEndian, &dataLen); err != nil {
			return nil, err
		}
		data := make([]byte, int(dataLen))
		if _, err := buff.Read(data); err != nil {
			return nil, err
		}
		den := &Dentry{}
		if err := den.Unmarshal(data); err != nil {
			return nil, err
		}
		result = append(result, den)
	}

	return result, nil
}

// Less tests whether the current dentry is less than the given one.
// This method is necessary fot B-Tree item implementation.
func (d *Dentry) Less(than BtreeItem) (less bool) {
	dentry, ok := than.(*Dentry)
	less = ok && ((d.ParentId < dentry.ParentId) || ((d.ParentId == dentry.ParentId) && (d.Name < dentry.Name)))
	return
}

func (d *Dentry) Copy() BtreeItem {
	newDentry := *d
	return &newDentry
}

// MarshalKey is the bytes version of the MarshalKey method which returns the byte slice result.
func (d *Dentry) MarshalKey() (k []byte) {
	buff := bytes.NewBuffer(make([]byte, 0))
	buff.Grow(32)
	if err := binary.Write(buff, binary.BigEndian, &d.ParentId); err != nil {
		panic(err)
	}
	buff.Write([]byte(d.Name))
	k = buff.Bytes()
	return
}

// UnmarshalKey unmarshals the exporterKey from bytes.
func (d *Dentry) UnmarshalKey(k []byte) (err error) {
	buff := bytes.NewBuffer(k)
	if err = binary.Read(buff, binary.BigEndian, &d.ParentId); err != nil {
		return
	}
	d.Name = string(buff.Bytes())
	return
}

// MarshalValue marshals the exporterKey to bytes.
func (d *Dentry) MarshalValue() (k []byte) {

	buff := bytes.NewBuffer(make([]byte, 0))
	buff.Grow(20 + len(d.dentryList) * 20)
	if err := binary.Write(buff, binary.BigEndian, &d.Inode); err != nil {
		panic(err)
	}
	if err := binary.Write(buff, binary.BigEndian, &d.Type); err != nil {
		panic(err)
	}
	if err := binary.Write(buff, binary.BigEndian, &d.VerSeq); err != nil {
		panic(err)
	}

	for _, dd := range d.dentryList {
		if err := binary.Write(buff, binary.BigEndian, &dd.Inode); err != nil {
			panic(err)
		}
		if err := binary.Write(buff, binary.BigEndian, &dd.Type); err != nil {
			panic(err)
		}
		if err := binary.Write(buff, binary.BigEndian, &dd.VerSeq); err != nil {
			panic(err)
		}
	}

	k = buff.Bytes()
	log.LogInfof("action[MarshalValue] dentry name %v, inode %v, parent inode %v, val len %v", d.Name, d.Inode, d.ParentId, len(k))
	return
}

func (d *Dentry) UnmarshalValue(val []byte) (err error) {
	buff := bytes.NewBuffer(val)
	if err = binary.Read(buff, binary.BigEndian, &d.Inode); err != nil {
		return
	}
	err = binary.Read(buff, binary.BigEndian, &d.Type)
	log.LogInfof("action[UnmarshalValue] dentry name %v, inode %v, parent inode %v, val len %v", d.Name, d.Inode, d.ParentId, len(val))
	if len(val) >= 20 {
		err = binary.Read(buff, binary.BigEndian, &d.VerSeq)
		if (len(val)-20) % 20 != 0 {
			return fmt.Errorf("action[UnmarshalSnapshotValue] left len %v after divide by dentry len", len(val)-20)
		}
		for i:=0; i < (len(val)-20)/20; i++ {
			//todo(leonchang) name and parentid should be removed to reduce space
			den := &Dentry{
				Name: d.Name,
				ParentId: d.ParentId,
			}
			if err = binary.Read(buff, binary.BigEndian, &den.Inode); err != nil {
				return
			}
			if err = binary.Read(buff, binary.BigEndian, &den.Type); err != nil {
				return
			}
			if err = binary.Read(buff, binary.BigEndian, &den.VerSeq); err != nil {
				return
			}
			d.dentryList = append(d.dentryList, den)
		}
	}
	return
}

