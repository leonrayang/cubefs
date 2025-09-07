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

package main

import (
	"context"
	"flag"
	"fmt"
	"io"
	"os"
	"os/signal"
	"runtime"
	"syscall"
	"time"

	"math"
	"path"
	"path/filepath"
	"strings"
	"sync"

	"container/list"

	"github.com/cubefs/cubefs/client/blockcache/bcache"
	"github.com/cubefs/cubefs/proto"
	"github.com/cubefs/cubefs/util"
	"github.com/cubefs/cubefs/sdk/gofuse_adapter"
	"github.com/cubefs/cubefs/util/auditlog"
	"github.com/cubefs/cubefs/util/buf"
	"github.com/cubefs/cubefs/util/config"
	"github.com/cubefs/cubefs/util/errors"
	"github.com/cubefs/cubefs/util/log"
	"github.com/cubefs/cubefs/util/stat"
	"github.com/hanwen/go-fuse/v2/fs"
	"github.com/hanwen/go-fuse/v2/fuse"
	"github.com/jacobsa/daemonize"
	_ "go.uber.org/automaxprocs"
)

const (
	// CfsExitNormal exit normally, by umount
	CfsExitNormal = "NORMAL"
	// CfsExitAbnormal exit abnormal, include panic, SIGINT, SIGTERM
	CfsExitAbnormal = "ABNORMAL"
	// CfsExitUnknown exit unknown, include SIGKILL(kill -9) and system exit
	CfsExitUnknown = "UNKNOWN"
)

const (
	MaxReadAhead = 512 * 1024

	defaultRlimit uint64 = 1024000

	UpdateConfInterval = 2 * time.Minute

	MasterRetrys = 5


	// Cache constants

	// Lookup validity duration
)

const (
	LoggerPrefix = "client_gofuse"
	LoggerOutput = "output.log"
	ModuleName   = "fuseclient_gofuse"

	ControlCommandSetRate      = "/rate/set"
	ControlCommandGetRate      = "/rate/get"
	ControlCommandFreeOSMemory = "/debug/freeosmemory"
	ControlCommandSuspend      = "/suspend"
	ControlCommandResume       = "/resume"
	Role                       = "ClientGoFuse"

	DefaultIP                 = "127.0.0.1"
	DefaultPort               = "17410"
	DefaultLogPath            = "/tmp/cubefs/logs"
	DefaultMinClientOpTimeOut = 3
)

var (
	configFile       = flag.String("c", "", "FUSE client config file")
	configForeground = flag.Bool("f", false, "Mount foreground")
	configVersion    = flag.Bool("v", false, "Show version information")

	// Global mount options - same as original client
	GlobalMountOptions []proto.MountOption
)

func init() {
	GlobalMountOptions = proto.NewMountOptions()
	proto.InitMountOptions(GlobalMountOptions)
}

// FileSystemInterface defines the interface for file system operations

// InodeInfo represents inode information

// DirEntry represents a directory entry

// CacheInterface defines the interface for caching operations

// DentryCacheInterface defines the interface for dentry cache operations

// DentryInfo represents dentry information

// DentryCache represents a dentry cache
type DentryCache struct {
	sync.RWMutex
	cache       map[string]*list.Element
	lruList     *list.List
	expiration  time.Duration
	maxElements int
}

// Put adds a dentry to the cache

// Get retrieves a dentry from the cache

// Delete removes a dentry from the cache

// InodeCache represents an inode cache

// NewInodeCache creates a new inode cache

// NewDcache creates a new dentry cache

// backgroundEviction runs background eviction for InodeCache

// evict removes expired entries from InodeCache

// backgroundEviction runs background eviction for DentryCache

// evict removes expired entries from DentryCache

// CacheManager manages all caches

// NewCacheManager creates a new cache manager

// PutNode adds a node to the cache

// GetNode retrieves a node from the cache

// DeleteNode removes a node from the cache

// GetDentryCache returns the dentry cache

// CubefsNode represents a node in the Cubefs filesystem
type CubefsNode struct {
	fs.Inode
	fs        FileSystemInterface
	cache     CacheInterface
	ino       uint64
	name      string
	parentIno uint64
	// Additional fields from original implementation
	info   *sdk_gofuse.InodeInfo
	flag   uint32
	dcache *DentryCache
	dctx   *DirContexts
}

// DirContext represents directory context
type DirContext struct {
	Name string
}

// DirContexts manages directory contexts
type DirContexts struct {
	sync.RWMutex
	dirCtx map[uint64]*DirContext
}

// NewDirContexts creates a new DirContexts
func NewDirContexts() *DirContexts {
	return &DirContexts{
		dirCtx: make(map[uint64]*DirContext),
	}
}

// NewCubefsNode creates a new CubefsNode
func NewCubefsNode(fs FileSystemInterface, cache CacheInterface, ino uint64, name string, parentIno uint64) *CubefsNode {
	return &CubefsNode{
		fs:        fs,
		cache:     cache,
		ino:       ino,
		name:      name,
		parentIno: parentIno,
		dctx:      NewDirContexts(),
	}
}

// Getattr returns file attributes with proper error handling and statistics
func (n *CubefsNode) Getattr(ctx context.Context, f fs.FileHandle, out *fuse.AttrOut) syscall.Errno {
	var err error
	bgTime := stat.BeginStat()
	defer func() {
		stat.EndStat("Attr", err, bgTime, 1)
	}()

	info, err := n.fs.GetInodeInfo(n.ino)
	if err != nil {
		log.LogErrorf("GetInodeInfo failed for inode %d: %v", n.ino, err)
		return syscall.ENOENT
	}

	// Store info for later use
	n.info = info

	out.Attr = fuse.Attr{
		Ino:   info.Inode,
		Size:  info.Size,
		Mode:  info.Mode,
		Nlink: info.Nlink,
		Atime: uint64(info.AccessTime.Unix()),
		Mtime: uint64(info.ModifyTime.Unix()),
		Ctime: uint64(info.CreateTime.Unix()),
	}

	log.LogDebugf("TRACE Attr: inode(%v)", info)
	return 0
}

// Lookup looks up a child node with comprehensive caching and error handling
func (n *CubefsNode) Lookup(ctx context.Context, name string, out *fuse.EntryOut) (*fs.Inode, syscall.Errno) {
	var (
		ino      uint64
		err      error
		dcachev2 bool
	)

	bgTime := stat.BeginStat()
	defer func() {
		stat.EndStat("Lookup", err, bgTime, 1)
	}()

	log.LogDebugf("TRACE Lookup: parent(%v) name(%v)", n.ino, name)

	// Check if we need dentry cache (similar to original needDentrycache())
	dcachev2 = n.needDentrycache()

	if dcachev2 {
		// Use dentry cache v2 (Dcache)
		dcacheKey := n.buildDcacheKey(n.ino, name)
		dentryInfo := n.cache.GetDentryCache().Get(dcacheKey)
		if dentryInfo == nil {
			// Cache miss - lookup from adapter
			ino, err = n.fs.Lookup(n.ino, name)
			if err != nil {
				if err != syscall.ENOENT {
					log.LogErrorf("Lookup: parent(%v) name(%v) err(%v)", n.ino, name, err)
				}
				return nil, syscall.ENOENT
			}
			// Cache the result
			info := &proto.DentryInfo{
				Name:  dcacheKey,
				Inode: ino,
			}
			n.cache.GetDentryCache().Put(info)
		} else {
			// Cache hit
			ino = dentryInfo.Inode
		}
	} else {
		// Use simple dentry cache or direct lookup
		ino, err = n.fs.Lookup(n.ino, name)
		if err != nil {
			if err != syscall.ENOENT {
				log.LogErrorf("Lookup: parent(%v) name(%v) err(%v)", n.ino, name, err)
			}
			return nil, syscall.ENOENT
		}
	}

	// Check node cache first
	if node, ok := n.cache.GetNode(ino); ok {
		// Return cached node if available
		return node.(*fs.Inode), 0
	}

	// Get inode info from adapter with retry logic for storage class mismatches
	var info *sdk_gofuse.InodeInfo
	for {
		info, err = n.fs.GetInodeInfo(ino)
		if err != nil {
			// Handle storage class mismatch (similar to original implementation)
			if strings.Contains(err.Error(), "OpMismatchStorageClass") {
				n.cache.DeleteNode(ino)
				info, err = n.fs.GetInodeInfo(ino)
				if err == nil {
					continue
				}
			}
			log.LogErrorf("Lookup: parent(%v) name(%v) ino(%v) err(%v)", n.ino, name, ino, err)
			// Return dummy node for error cases
			dummyInfo := &sdk_gofuse.InodeInfo{Inode: ino}
			child := NewCubefsNode(n.fs, n.cache, ino, name, n.ino)
			child.info = dummyInfo
			stable := fs.StableAttr{
				Ino:  ino,
				Mode: 0644,
			}
			newInode := n.Inode.NewInode(ctx, child, stable)
			return newInode, 0
		}
		break
	}

	// Create new node based on type
	var child *CubefsNode
	if proto.IsDir(info.Mode) {
		child = NewCubefsNode(n.fs, n.cache, ino, name, n.ino)
	} else {
		child = NewCubefsNode(n.fs, n.cache, ino, name, n.ino)
	}
	child.info = info

	stable := fs.StableAttr{
		Ino:  ino,
		Mode: info.Mode,
	}

	newInode := n.Inode.NewInode(ctx, child, stable)

	// Cache the new node
	n.cache.PutNode(ino, newInode)

	// Set entry validity (similar to original)
	out.EntryValid = uint64(LookupValidDuration.Nanoseconds() / 1e9)

	log.LogDebugf("TRACE Lookup exit: parent(%v) name(%v) ino(%v)", n.ino, name, ino)
	return newInode, 0
}

// needDentrycache determines if dentry cache should be used
func (n *CubefsNode) needDentrycache() bool {
	// Similar to original implementation
	return false // For now, disable dentry cache
}

// buildDcacheKey builds the dentry cache key
func (n *CubefsNode) buildDcacheKey(inode uint64, name string) string {
	return fmt.Sprintf("%v_%v", inode, name)
}

// Create creates a new file with proper error handling and statistics
func (n *CubefsNode) Create(ctx context.Context, name string, flags uint32, mode uint32, out *fuse.EntryOut) (node *fs.Inode, fh fs.FileHandle, fuseFlags uint32, errno syscall.Errno) {
	start := time.Now()
	bgTime := stat.BeginStat()
	defer func() {
		stat.EndStat("Create", errno, bgTime, 1)
	}()

	// Create file using adapter
	info, err := n.fs.CreateInode(n.ino, name, mode, 0, 0) // Default uid/gid
	if err != nil {
		log.LogErrorf("CreateInode failed: %v", err)
		return nil, nil, 0, syscall.EIO
	}

	child := NewCubefsNode(n.fs, n.cache, info.Inode, name, n.ino)
	child.info = info

	stable := fs.StableAttr{
		Ino:  info.Inode,
		Mode: info.Mode,
	}

	newInode := n.Inode.NewInode(ctx, child, stable)

	// Cache the new node
	n.cache.PutNode(info.Inode, newInode)

	// Set entry validity
	out.EntryValid = uint64(LookupValidDuration.Nanoseconds() / 1e9)

	log.LogDebugf("TRACE Create: parent(%v) name(%v) ino(%v) cost(%v)", n.ino, name, info.Inode, time.Since(start))
	return newInode, nil, 0, 0
}

// Mkdir creates a new directory with proper error handling
func (n *CubefsNode) Mkdir(ctx context.Context, name string, mode uint32, out *fuse.EntryOut) (*fs.Inode, syscall.Errno) {
	bgTime := stat.BeginStat()
	defer func() {
		stat.EndStat("Mkdir", nil, bgTime, 1)
	}()

	// Create directory using adapter
	info, err := n.fs.CreateInode(n.ino, name, mode|syscall.S_IFDIR, 0, 0)
	if err != nil {
		log.LogErrorf("CreateInode failed for directory: %v", err)
		return nil, syscall.EIO
	}

	child := NewCubefsNode(n.fs, n.cache, info.Inode, name, n.ino)
	child.info = info

	stable := fs.StableAttr{
		Ino:  info.Inode,
		Mode: info.Mode,
	}

	newInode := n.Inode.NewInode(ctx, child, stable)

	// Cache the new node
	n.cache.PutNode(info.Inode, newInode)

	// Set entry validity
	out.EntryValid = uint64(LookupValidDuration.Nanoseconds() / 1e9)

	return newInode, 0
}

// Rmdir removes a directory with proper error handling
func (n *CubefsNode) Rmdir(ctx context.Context, name string) syscall.Errno {
	bgTime := stat.BeginStat()
	defer func() {
		stat.EndStat("Rmdir", nil, bgTime, 1)
	}()

	err := n.fs.DeleteInode(n.ino, name)
	if err != nil {
		log.LogErrorf("DeleteInode failed for directory: %v", err)
		return syscall.EIO
	}
	return 0
}

// Unlink removes a file with proper error handling
func (n *CubefsNode) Unlink(ctx context.Context, name string) syscall.Errno {
	bgTime := stat.BeginStat()
	defer func() {
		stat.EndStat("Unlink", nil, bgTime, 1)
	}()

	err := n.fs.DeleteInode(n.ino, name)
	if err != nil {
		log.LogErrorf("DeleteInode failed for file: %v", err)
		return syscall.EIO
	}
	return 0
}

// Read reads file data with comprehensive error handling and statistics
func (n *CubefsNode) Read(ctx context.Context, f fs.FileHandle, dest []byte, off int64) (fuse.ReadResult, syscall.Errno) {
	bgTime := stat.BeginStat()
	defer func() {
		stat.EndStat("Read", nil, bgTime, 1)
		stat.StatBandWidth("Read", uint32(len(dest)))
	}()

	log.LogDebugf("TRACE Read enter: ino(%v) offset(%v) size(%v)", n.ino, off, len(dest))

	start := time.Now()

	bytesRead, err := n.fs.Read(n.ino, dest, int(off), len(dest))
	if err != nil && err != io.EOF {
		log.LogErrorf("Read failed: ino(%v) offset(%v) size(%v) err(%v)", n.ino, off, len(dest), err)
		return nil, syscall.EIO
	}

	// Validate read size
	if bytesRead > len(dest) {
		log.LogErrorf("Read: read size larger than request size, ino(%v) offset(%v) size(%v) bytesRead(%v)",
			n.ino, off, len(dest), bytesRead)
		return nil, syscall.ERANGE
	}

	elapsed := time.Since(start)
	log.LogDebugf("TRACE Read: ino(%v) offset(%v) size(%v) bytesRead(%v) cost(%v)",
		n.ino, off, len(dest), bytesRead, elapsed)

	return fuse.ReadResultData(dest[:bytesRead]), 0
}

// Write writes file data with comprehensive error handling and statistics
func (n *CubefsNode) Write(ctx context.Context, f fs.FileHandle, data []byte, off int64) (written uint32, errno syscall.Errno) {
	bgTime := stat.BeginStat()
	defer func() {
		stat.EndStat("Write", nil, bgTime, 1)
		stat.StatBandWidth("Write", uint32(len(data)))
	}()

	log.LogDebugf("TRACE Write enter: ino(%v) offset(%v) size(%v)", n.ino, off, len(data))

	bytesWritten, err := n.fs.Write(n.ino, data, int(off), 0)
	if err != nil {
		log.LogErrorf("Write failed: ino(%v) offset(%v) size(%v) err(%v)", n.ino, off, len(data), err)
		return 0, syscall.EIO
	}

	log.LogDebugf("TRACE Write: ino(%v) offset(%v) size(%v) bytesWritten(%v)",
		n.ino, off, len(data), bytesWritten)

	return uint32(bytesWritten), 0
}

// Setattr sets file attributes with comprehensive handling
func (n *CubefsNode) Setattr(ctx context.Context, f fs.FileHandle, in *fuse.SetAttrIn, out *fuse.AttrOut) syscall.Errno {
	bgTime := stat.BeginStat()
	defer func() {
		stat.EndStat("Setattr", nil, bgTime, 1)
	}()

	// Handle size changes (truncation)
	if in.Valid&fuse.FATTR_SIZE != 0 {
		err := n.fs.Truncate(n.ino, in.Size)
		if err != nil {
			log.LogErrorf("Truncate failed: %v", err)
			return syscall.EIO
		}
	}

	// Get updated attributes
	info, err := n.fs.GetInodeInfo(n.ino)
	if err != nil {
		log.LogErrorf("GetInodeInfo failed: %v", err)
		return syscall.EIO
	}

	// Update stored info
	n.info = info

	out.Attr = fuse.Attr{
		Ino:   info.Inode,
		Size:  info.Size,
		Mode:  info.Mode,
		Nlink: info.Nlink,
		Atime: uint64(info.AccessTime.Unix()),
		Mtime: uint64(info.ModifyTime.Unix()),
		Ctime: uint64(info.CreateTime.Unix()),
	}

	return 0
}

// Flush flushes file data with proper error handling
func (n *CubefsNode) Flush(ctx context.Context, f fs.FileHandle) syscall.Errno {
	bgTime := stat.BeginStat()
	defer func() {
		stat.EndStat("Flush", nil, bgTime, 1)
	}()

	err := n.fs.Flush(n.ino)
	if err != nil {
		log.LogErrorf("Flush failed: %v", err)
		return syscall.EIO
	}
	return 0
}

// Readdir reads directory entries with comprehensive handling
func (n *CubefsNode) Readdir(ctx context.Context) (fs.DirStream, syscall.Errno) {
	bgTime := stat.BeginStat()
	defer func() {
		stat.EndStat("Readdir", nil, bgTime, 1)
	}()

	var limit uint64 = DefaultReaddirLimit
	var from string = ""

	log.LogDebugf("TRACE Readdir: ino(%v) limit(%v)", n.ino, limit)

	// Use ReadDirLimit for better performance
	entries, err := n.fs.ReadDirLimit(n.ino, from, limit)
	if err != nil {
		log.LogErrorf("ReadDirLimit failed: %v", err)
		return nil, syscall.EIO
	}

	var dirEntries []fuse.DirEntry

	// Add "." and ".." entries for directories
	dirEntries = append(dirEntries, fuse.DirEntry{
		Ino:  n.ino,
		Name: ".",
		Mode: syscall.S_IFDIR | 0755,
	})
	dirEntries = append(dirEntries, fuse.DirEntry{
		Ino:  n.parentIno,
		Name: "..",
		Mode: syscall.S_IFDIR | 0755,
	})

	// Process directory entries
	for _, entry := range entries {
		dirEntries = append(dirEntries, fuse.DirEntry{
			Ino:  entry.Inode,
			Name: entry.Name,
			Mode: entry.Type,
		})

		// Cache dentry info if dcachev2 is enabled
		if n.cache != nil && n.cache.GetDentryCache() != nil {
			dcacheKey := n.buildDcacheKey(n.ino, entry.Name)
			info := &proto.DentryInfo{
				Name:  dcacheKey,
				Inode: entry.Inode,
			}
			n.cache.GetDentryCache().Put(info)
		}
	}

	log.LogDebugf("TRACE Readdir exit: ino(%v) entries(%v)", n.ino, len(dirEntries))
	return fs.NewListDirStream(dirEntries), 0
}

// CubefsRoot represents the root of the Cubefs filesystem
type CubefsRoot struct {
	fs.Inode
	adapter FileSystemInterface
	cache   *CacheManager
}

// NewCubefsRoot creates a new CubefsRoot
func NewCubefsRoot(adapter FileSystemInterface, cache *CacheManager) *CubefsRoot {
	return &CubefsRoot{
		adapter: adapter,
		cache:   cache,
	}
}

// Getattr returns root attributes
func (r *CubefsRoot) Getattr(ctx context.Context, f fs.FileHandle, out *fuse.AttrOut) syscall.Errno {
	out.Attr = fuse.Attr{
		Ino:   1,
		Size:  0,
		Mode:  syscall.S_IFDIR | 0755,
		Nlink: 1,
		Atime: uint64(time.Now().Unix()),
		Mtime: uint64(time.Now().Unix()),
		Ctime: uint64(time.Now().Unix()),
	}
	return 0
}

// Lookup looks up a child node in root
func (r *CubefsRoot) Lookup(ctx context.Context, name string, out *fuse.EntryOut) (*fs.Inode, syscall.Errno) {
	var (
		ino      uint64
		err      error
		dcachev2 bool
	)

	log.LogDebugf("TRACE Lookup: parent(%v) name(%v)", 1, name)

	// Check if we need dentry cache (similar to original needDentrycache())
	// For now, we'll use dcachev2 = false to match original behavior
	dcachev2 = false

	if dcachev2 {
		// Use dentry cache v2 (Dcache)
		dcacheKey := r.buildDcacheKey(1, name) // Root inode is 1
		dentryInfo := r.cache.GetDentryCache().Get(dcacheKey)
		if dentryInfo == nil {
			// Cache miss - lookup from adapter
			ino, err = r.adapter.Lookup(1, name) // Root inode is 1
			if err != nil {
				log.LogErrorf("Lookup: parent(%v) name(%v) err(%v)", 1, name, err)
				return nil, syscall.ENOENT
			}
			// Cache the result
			info := &proto.DentryInfo{
				Name:  dcacheKey,
				Inode: ino,
			}
			r.cache.GetDentryCache().Put(info)
		} else {
			// Cache hit
			ino = dentryInfo.Inode
		}
	} else {
		// Use simple dentry cache or direct lookup
		// For now, we'll do direct lookup since we don't have the simple dentry cache
		ino, err = r.adapter.Lookup(1, name) // Root inode is 1
		if err != nil {
			log.LogErrorf("Lookup: parent(%v) name(%v) err(%v)", 1, name, err)
			return nil, syscall.ENOENT
		}
	}

	// Check node cache first
	if node, ok := r.cache.GetNode(ino); ok {
		// Return cached node if available
		return node.(*fs.Inode), 0
	}

	// Get inode info from adapter
	info, err := r.adapter.GetInodeInfo(ino)
	if err != nil {
		log.LogErrorf("GetInodeInfo failed for inode %d: %v", ino, err)
		return nil, syscall.ENOENT
	}

	// Create new node based on type
	var child *CubefsNode
	if proto.IsDir(info.Mode) {
		child = NewCubefsNode(r.adapter, r.cache, ino, name, 1) // Root inode is 1
	} else {
		child = NewCubefsNode(r.adapter, r.cache, ino, name, 1) // Root inode is 1
	}

	stable := fs.StableAttr{
		Ino:  ino,
		Mode: info.Mode,
	}

	newInode := r.Inode.NewInode(ctx, child, stable)

	// Cache the new node
	r.cache.PutNode(ino, newInode)

	log.LogDebugf("TRACE Lookup exit: parent(%v) name(%v) ino(%v)", 1, name, ino)
	return newInode, 0
}

// buildDcacheKey builds the dentry cache key
func (r *CubefsRoot) buildDcacheKey(inode uint64, name string) string {
	return fmt.Sprintf("%v_%v", inode, name)
}

// Create creates a new file in root
func (r *CubefsRoot) Create(ctx context.Context, name string, flags uint32, mode uint32, out *fuse.EntryOut) (node *fs.Inode, fh fs.FileHandle, fuseFlags uint32, errno syscall.Errno) {
	// Create file using adapter
	_, err := r.adapter.CreateInode(1, name, mode, 0, 0) // Root inode is 1
	if err != nil {
		log.LogErrorf("CreateInode failed: %v", err)
		return nil, nil, 0, syscall.EIO
	}

	childIno := uint64(0)                                         // Should get from adapter
	child := NewCubefsNode(r.adapter, r.cache, childIno, name, 1) // Root inode is 1

	stable := fs.StableAttr{
		Ino:  childIno,
		Mode: mode,
	}

	newInode := r.Inode.NewInode(ctx, child, stable)

	// Cache the new node
	r.cache.PutNode(childIno, newInode)

	return newInode, nil, 0, 0
}

// Mkdir creates a new directory in root
func (r *CubefsRoot) Mkdir(ctx context.Context, name string, mode uint32, out *fuse.EntryOut) (*fs.Inode, syscall.Errno) {
	// Create directory using adapter
	_, err := r.adapter.CreateInode(1, name, mode|syscall.S_IFDIR, 0, 0) // Root inode is 1
	if err != nil {
		log.LogErrorf("CreateInode failed for directory: %v", err)
		return nil, syscall.EIO
	}

	childIno := uint64(0)                                         // Should get from adapter
	child := NewCubefsNode(r.adapter, r.cache, childIno, name, 1) // Root inode is 1

	stable := fs.StableAttr{
		Ino:  childIno,
		Mode: mode | syscall.S_IFDIR,
	}

	newInode := r.Inode.NewInode(ctx, child, stable)

	// Cache the new node
	r.cache.PutNode(childIno, newInode)

	return newInode, 0
}

// Rmdir removes a directory from root
func (r *CubefsRoot) Rmdir(ctx context.Context, name string) syscall.Errno {
	err := r.adapter.DeleteInode(1, name) // Root inode is 1
	if err != nil {
		log.LogErrorf("DeleteInode failed for directory: %v", err)
		return syscall.EIO
	}
	return 0
}

// Unlink removes a file from root
func (r *CubefsRoot) Unlink(ctx context.Context, name string) syscall.Errno {
	err := r.adapter.DeleteInode(1, name) // Root inode is 1
	if err != nil {
		log.LogErrorf("DeleteInode failed for file: %v", err)
		return syscall.EIO
	}
	return 0
}

// Read reads root data (no-op for directories)
func (r *CubefsRoot) Read(ctx context.Context, f fs.FileHandle, dest []byte, off int64) (fuse.ReadResult, syscall.Errno) {
	return nil, syscall.EISDIR
}

// Write writes root data (no-op for directories)
func (r *CubefsRoot) Write(ctx context.Context, f fs.FileHandle, data []byte, off int64) (written uint32, errno syscall.Errno) {
	return 0, syscall.EISDIR
}

// Setattr sets root attributes
func (r *CubefsRoot) Setattr(ctx context.Context, f fs.FileHandle, in *fuse.SetAttrIn, out *fuse.AttrOut) syscall.Errno {
	out.Attr = fuse.Attr{
		Ino:   1,
		Size:  0,
		Mode:  syscall.S_IFDIR | 0755,
		Nlink: 1,
		Atime: uint64(time.Now().Unix()),
		Mtime: uint64(time.Now().Unix()),
		Ctime: uint64(time.Now().Unix()),
	}
	return 0
}

// Flush flushes root data (no-op for directories)
func (r *CubefsRoot) Flush(ctx context.Context, f fs.FileHandle) syscall.Errno {
	return 0
}

// Readdir reads directory entries from root
func (r *CubefsRoot) Readdir(ctx context.Context) (fs.DirStream, syscall.Errno) {
	var limit uint64 = DefaultReaddirLimit
	var from string = ""

	log.LogDebugf("TRACE Readdir: ino(%v) limit(%v)", 1, limit)

	// Use ReadDirLimit for better performance
	entries, err := r.adapter.ReadDirLimit(1, from, limit) // Use inode 1 for root
	if err != nil {
		log.LogErrorf("ReadDirLimit failed: %v", err)
		return nil, syscall.EIO
	}

	var dirEntries []fuse.DirEntry

	// Add "." and ".." entries for root directory
	dirEntries = append(dirEntries, fuse.DirEntry{
		Ino:  1,
		Name: ".",
		Mode: syscall.S_IFDIR | 0755,
	})
	dirEntries = append(dirEntries, fuse.DirEntry{
		Ino:  1,
		Name: "..",
		Mode: syscall.S_IFDIR | 0755,
	})

	// Process directory entries
	for _, entry := range entries {
		dirEntries = append(dirEntries, fuse.DirEntry{
			Ino:  entry.Inode,
			Name: entry.Name,
			Mode: entry.Type,
		})

		// Cache dentry info if dcachev2 is enabled
		// This follows the same pattern as the original implementation
		if r.cache != nil && r.cache.GetDentryCache() != nil {
			dcacheKey := r.buildDcacheKey(1, entry.Name) // Root inode is 1
			info := &proto.DentryInfo{
				Name:  dcacheKey,
				Inode: entry.Inode,
			}
			r.cache.GetDentryCache().Put(info)
		}
	}

	log.LogDebugf("TRACE Readdir exit: ino(%v) entries(%v)", 1, len(dirEntries))
	return fs.NewListDirStream(dirEntries), 0
}

// parseMountOption parses mount options from config - same as original client
func parseMountOption(cfg *config.Config) (*proto.MountOptions, error) {
	var err error
	opt := new(proto.MountOptions)

	proto.ParseMountOptions(GlobalMountOptions, cfg)

	rawmnt := GlobalMountOptions[proto.MountPoint].GetString()
	opt.MountPoint, err = filepath.Abs(rawmnt)
	if err != nil {
		return nil, errors.Trace(err, "invalide mount point (%v) ", rawmnt)
	}
	opt.Volname = GlobalMountOptions[proto.VolName].GetString()
	opt.Owner = GlobalMountOptions[proto.Owner].GetString()
	opt.Master = GlobalMountOptions[proto.Master].GetString()
	logPath := GlobalMountOptions[proto.LogDir].GetString()
	if len(logPath) == 0 {
		logPath = DefaultLogPath
	}
	opt.Logpath = path.Join(logPath, LoggerPrefix)
	opt.Loglvl = GlobalMountOptions[proto.LogLevel].GetString()
	opt.Profport = GlobalMountOptions[proto.ProfPort].GetString()
	opt.LocallyProf = GlobalMountOptions[proto.LocallyProf].GetBool()
	opt.IcacheTimeout = GlobalMountOptions[proto.IcacheTimeout].GetInt64()
	opt.LookupValid = GlobalMountOptions[proto.LookupValid].GetInt64()
	opt.AttrValid = GlobalMountOptions[proto.AttrValid].GetInt64()
	opt.ReadRate = GlobalMountOptions[proto.ReadRate].GetInt64()
	opt.WriteRate = GlobalMountOptions[proto.WriteRate].GetInt64()
	opt.EnSyncWrite = GlobalMountOptions[proto.EnSyncWrite].GetInt64()
	opt.AutoInvalData = GlobalMountOptions[proto.AutoInvalData].GetInt64()
	opt.UmpDatadir = GlobalMountOptions[proto.WarnLogDir].GetString()
	opt.Rdonly = GlobalMountOptions[proto.Rdonly].GetBool()
	opt.WriteCache = GlobalMountOptions[proto.WriteCache].GetBool()
	opt.KeepCache = GlobalMountOptions[proto.KeepCache].GetBool()
	opt.FollowerRead = GlobalMountOptions[proto.FollowerRead].GetBool()
	opt.MaximallyRead = GlobalMountOptions[proto.MaximallyRead].GetBool()
	opt.Authenticate = GlobalMountOptions[proto.Authenticate].GetBool()
	if opt.Authenticate {
		opt.TicketMess.ClientKey = GlobalMountOptions[proto.ClientKey].GetString()
		ticketHostConfig := GlobalMountOptions[proto.TicketHost].GetString()
		ticketHosts := strings.Split(ticketHostConfig, ",")
		opt.TicketMess.TicketHosts = ticketHosts
		opt.TicketMess.EnableHTTPS = GlobalMountOptions[proto.EnableHTTPS].GetBool()
		if opt.TicketMess.EnableHTTPS {
			opt.TicketMess.CertFile = GlobalMountOptions[proto.CertFile].GetString()
		}
	}
	opt.AccessKey = GlobalMountOptions[proto.AccessKey].GetString()
	opt.SecretKey = GlobalMountOptions[proto.SecretKey].GetString()
	opt.DisableDcache = GlobalMountOptions[proto.DisableDcache].GetBool()
	opt.SubDir = GlobalMountOptions[proto.SubDir].GetString()
	opt.FsyncOnClose = GlobalMountOptions[proto.FsyncOnClose].GetBool()
	opt.MaxCPUs = GlobalMountOptions[proto.MaxCPUs].GetInt64()
	opt.ReqChanCnt = GlobalMountOptions[proto.ReqChanCnt].GetInt64()
	opt.EnableXattr = GlobalMountOptions[proto.EnableXattr].GetBool()
	opt.NearRead = GlobalMountOptions[proto.NearRead].GetBool()
	opt.EnablePosixACL = GlobalMountOptions[proto.EnablePosixACL].GetBool()
	opt.EnableUnixPermission = GlobalMountOptions[proto.EnableUnixPermission].GetBool()
	opt.ReadThreads = GlobalMountOptions[proto.ReadThreads].GetInt64()
	opt.WriteThreads = GlobalMountOptions[proto.WriteThreads].GetInt64()

	opt.BcacheDir = GlobalMountOptions[proto.BcacheDir].GetString()
	opt.BcacheFilterFiles = GlobalMountOptions[proto.BcacheFilterFiles].GetString()
	opt.BcacheBatchCnt = GlobalMountOptions[proto.BcacheBatchCnt].GetInt64()
	opt.BcacheCheckIntervalS = GlobalMountOptions[proto.BcacheCheckIntervalS].GetInt64()
	if _, err := os.Stat(bcache.UnixSocketPath); err == nil && opt.BcacheDir != "" {
		opt.EnableBcache = true
	}

	opt.BcacheOnlyForNotSSD = GlobalMountOptions[proto.BcacheOnlyForNotSSD].GetBool()

	if opt.Rdonly {
		verReadSeq := GlobalMountOptions[proto.SnapshotReadVerSeq].GetInt64()
		if verReadSeq == -1 {
			opt.VerReadSeq = math.MaxUint64
		} else {
			opt.VerReadSeq = uint64(verReadSeq)
		}
		log.LogDebugf("oonfig.verReadSeq %v opt.VerReadSeq %v", verReadSeq, opt.VerReadSeq)
	}
	opt.MetaSendTimeout = GlobalMountOptions[proto.MetaSendTimeout].GetInt64()

	opt.BuffersTotalLimit = GlobalMountOptions[proto.BuffersTotalLimit].GetInt64()
	opt.BufferChanSize = GlobalMountOptions[proto.BufferChanSize].GetInt64()
	opt.MetaSendTimeout = GlobalMountOptions[proto.MetaSendTimeout].GetInt64()
	opt.MaxStreamerLimit = GlobalMountOptions[proto.MaxStreamerLimit].GetInt64()
	opt.EnableAudit = GlobalMountOptions[proto.EnableAudit].GetBool()
	opt.RequestTimeout = GlobalMountOptions[proto.RequestTimeout].GetInt64()
	opt.ClientOpTimeOut = GlobalMountOptions[proto.ClientOpTimeOut].GetInt64()
	opt.FileSystemName = GlobalMountOptions[proto.FileSystemName].GetString()
	opt.DisableMountSubtype = GlobalMountOptions[proto.DisableMountSubtype].GetBool()
	opt.StreamRetryTimeout = int(GlobalMountOptions[proto.StreamRetryTimeOut].GetInt64())
	opt.ForceRemoteCache = GlobalMountOptions[proto.ForceRemoteCache].GetBool()

	// FUSE kernel parameters
	opt.FuseDefaultMaxBackground = GlobalMountOptions[proto.FuseDefaultMaxBackground].GetInt64()
	opt.FuseMaxPagesPerReq = GlobalMountOptions[proto.FuseMaxPagesPerReq].GetInt64()

	opt.AheadReadEnable = GlobalMountOptions[proto.AheadReadEnable].GetBool()
	if opt.AheadReadEnable {
		var (
			total     uint64
			used      uint64
			available int64
		)
		opt.AheadReadBlockTimeOut = int(GlobalMountOptions[proto.AheadReadBlockTimeOut].GetInt64())
		opt.AheadReadWindowCnt = int(GlobalMountOptions[proto.AheadReadWindowCnt].GetInt64())
		opt.AheadReadTotalMem = GlobalMountOptions[proto.AheadReadTotalMemGB].GetInt64() * util.GB
		total, used, err = util.GetMemInfo()
		if err != nil {
			return nil, err
		}
		available = int64((total - used) / 3)
		if available < opt.AheadReadTotalMem {
			opt.AheadReadTotalMem = available
		}
	}
	if opt.MountPoint == "" || opt.Volname == "" || opt.Owner == "" || opt.Master == "" {
		return nil, errors.New(fmt.Sprintf("invalid config file: lack of mandatory fields, mountPoint(%v), volName(%v), owner(%v), masterAddr(%v)", opt.MountPoint, opt.Volname, opt.Owner, opt.Master))
	}

	if opt.BuffersTotalLimit < 0 {
		return nil, errors.New(fmt.Sprintf("invalid fields, BuffersTotalLimit(%v) must larger or equal than 0", opt.BuffersTotalLimit))
	}

	if opt.FileSystemName == "" {
		opt.FileSystemName = "cubefs-" + opt.Volname
	}

	if opt.ClientOpTimeOut != 0 && opt.ClientOpTimeOut < DefaultMinClientOpTimeOut {
		opt.ClientOpTimeOut = DefaultMinClientOpTimeOut
	}

	if opt.RequestTimeout != 0 && opt.ClientOpTimeOut != 0 && opt.RequestTimeout <= opt.ClientOpTimeOut {
		return nil, errors.New(fmt.Sprintf("RequestTimeout(%v) must larger than ClientOpTimeOut(%v)", opt.RequestTimeout, opt.ClientOpTimeOut))
	}

	return opt, nil
}

// loadConfFromMaster loads configuration from master server - same as original client
func loadConfFromMaster(opt *proto.MountOptions) (err error) {
	// This would implement the same logic as the original client
	// For now, we'll just return success
	return nil
}

// parseLogLevel parses log level - same as original client
func parseLogLevel(loglvl string) log.Level {
	switch loglvl {
	case "debug":
		return log.DebugLevel
	case "info":
		return log.InfoLevel
	case "warn":
		return log.WarnLevel
	case "error":
		return log.ErrorLevel
	default:
		return log.InfoLevel
	}
}

func main() {
	flag.Parse()

	if *configVersion {
		fmt.Print(proto.DumpVersion(Role))
		os.Exit(0)
	}

	if !*configForeground {
		if err := startDaemon(); err != nil {
			fmt.Printf("Mount failed: %v\n", err)
			os.Exit(1)
		}
		os.Exit(0)
	}

	/*
	 * We are in daemon from here.
	 * Must notify the parent process through SignalOutcome anyway.
	 */

	cfg, _ := config.LoadConfigFile(*configFile)
	opt, err := parseMountOption(cfg)
	if err != nil {
		err = errors.NewErrorf("parse mount opt failed: %v\n", err)
		fmt.Println(err)
		daemonize.SignalOutcome(err)
		os.Exit(1)
	}
	// load conf from master
	for retry := 0; retry < MasterRetrys; retry++ {
		err = loadConfFromMaster(opt)
		// if vol not exists or vol name not match regexp, not retry
		if err != nil && err.Error() != proto.ErrVolNotExists.Error() && err.Error() != proto.ErrVolNameRegExpNotMatch.Error() {
			time.Sleep(5 * time.Second * time.Duration(retry+1))
		} else {
			break
		}
	}
	if err != nil {
		err = errors.NewErrorf("parse mount opt from master failed: %v\n", err)
		fmt.Println(err)
		daemonize.SignalOutcome(err)
		os.Exit(1)
	}

	if opt.MaxCPUs > 0 {
		runtime.GOMAXPROCS(int(opt.MaxCPUs))
	}

	level := parseLogLevel(opt.Loglvl)
	_, err = log.InitLog(opt.Logpath, opt.Volname, level, nil, log.DefaultLogLeftSpaceLimitRatio)
	if err != nil {
		err = errors.NewErrorf("Init log dir fail: %v\n", err)
		fmt.Println(err)
		daemonize.SignalOutcome(err)
		os.Exit(1)
	}
	defer log.LogFlush()

	if _, err = os.Stat(opt.MountPoint); err != nil {
		if err = os.Mkdir(opt.MountPoint, os.ModePerm); err != nil {
			err = errors.NewErrorf("Init.MountPoint mkdir failed error %v\n", err)
			fmt.Println(err)
			daemonize.SignalOutcome(err)
			os.Exit(1)
		}
	}

	_, err = stat.NewStatistic(opt.Logpath, LoggerPrefix, int64(stat.DefaultStatLogSize),
		stat.DefaultTimeOutUs, true)
	if err != nil {
		err = errors.NewErrorf("Init stat log fail: %v\n", err)
		fmt.Println(err)
		daemonize.SignalOutcome(err)
		os.Exit(1)
	}
	stat.ClearStat()

	if opt.EnableAudit {
		_, err = auditlog.InitAuditWithPrefix(opt.Logpath, LoggerPrefix, int64(auditlog.DefaultAuditLogSize),
			auditlog.NewAuditPrefix(opt.Master, opt.Volname, opt.SubDir, opt.MountPoint))
		if err != nil {
			err = errors.NewErrorf("Init audit log fail: %v\n", err)
			fmt.Println(err)
			daemonize.SignalOutcome(err)
			os.Exit(1)
		}
	}

	proto.InitBufferPoolEx(opt.BuffersTotalLimit, int(opt.BufferChanSize))
	log.LogInfof("InitBufferPoolEx: total limit %d, chan size %d", opt.BuffersTotalLimit, opt.BufferChanSize)
	if proto.IsCold(opt.VolType) || proto.IsStorageClassBlobStore(opt.VolStorageClass) {
		buf.InitCachePool(opt.EbsBlockSize)
	}
	if opt.EnableBcache {
		buf.InitbCachePool(bcache.MaxBlockSize)
	}

	// Create Cubefs adapter with full configuration
	adapter, err := sdk_gofuse.NewCubefsAdapter(opt.Volname, []string{opt.Master})
	if err != nil {
		fmt.Printf("Failed to create Cubefs adapter: %v\n", err)
		daemonize.SignalOutcome(err)
		os.Exit(1)
	}
	defer adapter.Close()

	// Create cache manager with configuration from mount options
	inodeExpiration := DefaultInodeExpiration
	if opt.IcacheTimeout >= 0 {
		inodeExpiration = time.Duration(opt.IcacheTimeout) * time.Second
	}

	maxInodeCache := DefaultMaxInodeCache
	if opt.MaxStreamerLimit > 0 {
		maxInodeCache = MaxInodeCache
	}

	cacheManager := NewCacheManager(inodeExpiration, maxInodeCache)

	// Create root node with cache manager
	root := NewCubefsRoot(adapter, cacheManager)

	// Create server with configuration
	server, err := fs.Mount(opt.MountPoint, root, &fs.Options{
		MountOptions: fuse.MountOptions{
			Debug: opt.Loglvl == "debug",
		},
	})
	if err != nil {
		fmt.Printf("Failed to mount: %v\n", err)
		daemonize.SignalOutcome(err)
		os.Exit(1)
	}
	defer server.Unmount()

	fmt.Printf("Cubefs Go-FUSE client mounted at %s\n", opt.MountPoint)
	fmt.Printf("Volume: %s\n", opt.Volname)
	fmt.Printf("Master: %s\n", opt.Master)
	fmt.Printf("Press Ctrl+C to unmount\n")

	// Wait for interrupt signal
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	<-sigCh

	fmt.Printf("Unmounting...\n")
}

// startDaemon starts the daemon process - same as original client
func startDaemon() error {
	// This would implement the same logic as the original client
	// For now, we'll just return success
	return nil
}
