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

package sdk_gofuse

import (
	"sync"
	"time"

	"github.com/cubefs/cubefs/sdk/data/stream"
	"github.com/cubefs/cubefs/sdk/meta"
	"github.com/cubefs/cubefs/util/log"
)

// CubefsAdapter provides a go-fuse compatible interface to the Cubefs SDK
type CubefsAdapter struct {
	metaWrapper  *meta.MetaWrapper
	extentClient *stream.ExtentClient
	volumeName   string
	mu           sync.RWMutex
}

// NewCubefsAdapter creates a new adapter for go-fuse
func NewCubefsAdapter(volumeName string, masters []string) (*CubefsAdapter, error) {
	// Create meta config
	metaConfig := &meta.MetaConfig{
		Volume:          volumeName,
		Masters:         masters,
		Authenticate:    false,
		ValidateOwner:   false,
		MetaSendTimeout: 60,
	}

	// Create meta wrapper
	metaWrapper, err := meta.NewMetaWrapper(metaConfig)
	if err != nil {
		return nil, err
	}

	// Create extent client config
	extentConfig := &stream.ExtentConfig{
		Volume:                      volumeName,
		Masters:                     masters,
		FollowerRead:                true,
		NearRead:                    true,
		MaximallyRead:               true,
		Preload:                     true,
		ReadRate:                    -1,
		WriteRate:                   -1,
		BcacheEnable:                false,
		InnerReq:                    false,
		MaxStreamerLimit:            100000,
		MetaWrapper:                 metaWrapper,
		OnAppendExtentKey:           metaWrapper.AppendExtentKey,
		OnSplitExtentKey:            metaWrapper.SplitExtentKey,
		OnGetExtents:                metaWrapper.GetExtents,
		OnTruncate:                  metaWrapper.Truncate,
		OnEvictIcache:               nil, // Not available in MetaWrapper
		OnLoadBcache:                nil, // Disable block cache for now
		OnCacheBcache:               nil,
		OnEvictBcache:               nil,
		DisableMetaCache:            false,
		StreamRetryTimeout:          60,
		OnRenewalForbiddenMigration: metaWrapper.RenewalForbiddenMigration,
		OnForbiddenMigration:        metaWrapper.ForbiddenMigration,
		OnGetInodeInfo:              metaWrapper.InodeGet_ll,
		AheadReadEnable:             true,
		AheadReadTotalMem:           100 * 1024 * 1024, // 100MB
		AheadReadBlockTimeOut:       5,
		AheadReadWindowCnt:          10,
		NeedRemoteCache:             false,
		ForceRemoteCache:            false,
		HeartBeatPing:               false,
	}

	// Create extent client
	extentClient, err := stream.NewExtentClient(extentConfig)
	if err != nil {
		return nil, err
	}

	return &CubefsAdapter{
		metaWrapper:  metaWrapper,
		extentClient: extentClient,
		volumeName:   volumeName,
	}, nil
}

// GetInodeInfo retrieves inode information
func (ca *CubefsAdapter) GetInodeInfo(ino uint64) (*InodeInfo, error) {
	ca.mu.RLock()
	defer ca.mu.RUnlock()

	info, err := ca.metaWrapper.InodeGet_ll(ino)
	if err != nil {
		return nil, err
	}

	return &InodeInfo{
		Inode:      info.Inode,
		Mode:       info.Mode,
		Size:       info.Size,
		Generation: info.Generation,
		CreateTime: info.CreateTime,
		AccessTime: info.AccessTime,
		ModifyTime: info.ModifyTime,
		LinkTarget: "", // Not directly available in proto.InodeInfo
		Nlink:      info.Nlink,
		Uid:        info.Uid,
		Gid:        info.Gid,
	}, nil
}

// Lookup looks up a child inode by name in the parent directory
func (ca *CubefsAdapter) Lookup(parentIno uint64, name string) (uint64, error) {
	ca.mu.RLock()
	defer ca.mu.RUnlock()

	ino, _, err := ca.metaWrapper.Lookup_ll(parentIno, name)
	if err != nil {
		return 0, err
	}

	return ino, nil
}

// CreateInode creates a new inode
func (ca *CubefsAdapter) CreateInode(parentIno uint64, name string, mode uint32, uid, gid uint32) (*InodeInfo, error) {
	ca.mu.Lock()
	defer ca.mu.Unlock()

	info, err := ca.metaWrapper.Create_ll(parentIno, name, mode, uid, gid, nil, "", false)
	if err != nil {
		return nil, err
	}

	return &InodeInfo{
		Inode:      info.Inode,
		Mode:       info.Mode,
		Size:       info.Size,
		Generation: info.Generation,
		CreateTime: info.CreateTime,
		AccessTime: info.AccessTime,
		ModifyTime: info.ModifyTime,
		LinkTarget: "", // Not available in proto.InodeInfo
		Nlink:      info.Nlink,
		Uid:        info.Uid,
		Gid:        info.Gid,
	}, nil
}

// DeleteInode deletes an inode
func (ca *CubefsAdapter) DeleteInode(parentIno uint64, name string) error {
	ca.mu.Lock()
	defer ca.mu.Unlock()

	_, err := ca.metaWrapper.Delete_ll(parentIno, name, false, "")
	return err
}

// ReadDir reads directory entries
func (ca *CubefsAdapter) ReadDir(ino uint64) ([]*DirEntry, error) {
	ca.mu.RLock()
	defer ca.mu.RUnlock()

	entries, err := ca.metaWrapper.ReadDir_ll(ino)
	if err != nil {
		return nil, err
	}

	var dirEntries []*DirEntry
	for _, entry := range entries {
		dirEntries = append(dirEntries, &DirEntry{
			Inode: entry.Inode,
			Name:  entry.Name,
			Type:  entry.Type,
		})
	}

	return dirEntries, nil
}

// ReadDirLimit reads directory entries with a limit
func (ca *CubefsAdapter) ReadDirLimit(ino uint64, from string, limit uint64) ([]*DirEntry, error) {
	ca.mu.RLock()
	defer ca.mu.RUnlock()

	entries, err := ca.metaWrapper.ReadDirLimit_ll(ino, from, limit)
	if err != nil {
		return nil, err
	}

	var dirEntries []*DirEntry
	for _, entry := range entries {
		dirEntries = append(dirEntries, &DirEntry{
			Inode: entry.Inode,
			Name:  entry.Name,
			Type:  entry.Type,
		})
	}

	return dirEntries, nil
}

// Read reads data from a file
func (ca *CubefsAdapter) Read(ino uint64, data []byte, offset int, size int) (int, error) {
	ca.mu.RLock()
	defer ca.mu.RUnlock()

	// Open stream if not already open
	err := ca.extentClient.OpenStream(ino, false, false, "")
	if err != nil {
		log.LogErrorf("Failed to open stream for inode %d: %v", ino, err)
		return 0, err
	}

	// Read data
	return ca.extentClient.Read(ino, data, offset, size, 0, false)
}

// Write writes data to a file
func (ca *CubefsAdapter) Write(ino uint64, data []byte, offset int, flags int) (int, error) {
	ca.mu.Lock()
	defer ca.mu.Unlock()

	// Open stream for writing
	err := ca.extentClient.OpenStream(ino, true, false, "")
	if err != nil {
		log.LogErrorf("Failed to open stream for writing inode %d: %v", ino, err)
		return 0, err
	}

	// Write data
	return ca.extentClient.Write(ino, offset, data, flags, nil, 0, false)
}

// Truncate truncates a file
func (ca *CubefsAdapter) Truncate(ino uint64, size uint64) error {
	ca.mu.Lock()
	defer ca.mu.Unlock()

	return ca.extentClient.Truncate(ca.metaWrapper, 0, ino, int(size), "")
}

// Flush flushes file data
func (ca *CubefsAdapter) Flush(ino uint64) error {
	ca.mu.Lock()
	defer ca.mu.Unlock()

	return ca.extentClient.Flush(ino)
}

// Close closes the adapter
func (ca *CubefsAdapter) Close() error {
	ca.mu.Lock()
	defer ca.mu.Unlock()

	if ca.extentClient != nil {
		return ca.extentClient.Close()
	}
	return nil
}

// InodeInfo represents inode information
type InodeInfo struct {
	Inode      uint64
	Mode       uint32
	Size       uint64
	Generation uint64
	CreateTime time.Time
	AccessTime time.Time
	ModifyTime time.Time
	LinkTarget string
	Nlink      uint32
	Uid        uint32
	Gid        uint32
}

// DirEntry represents a directory entry
type DirEntry struct {
	Inode uint64
	Name  string
	Type  uint32
}
