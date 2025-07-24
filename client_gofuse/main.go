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
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/cubefs/cubefs/sdk_gofuse"
	"github.com/cubefs/cubefs/util/log"
	"github.com/hanwen/go-fuse/v2/fs"
	"github.com/hanwen/go-fuse/v2/fuse"
)

const (
	DefaultMountPoint = "/tmp/cubefs_gofuse"
	DefaultVolumeName = "cubefs"
	DefaultMasters    = "127.0.0.1:17010"
)

// CubefsNode represents a node in the Cubefs filesystem
type CubefsNode struct {
	fs.Inode
	adapter *sdk_gofuse.CubefsAdapter
	ino     uint64
	name    string
}

// Ensure CubefsNode implements required interfaces
var _ fs.NodeLookuper = (*CubefsNode)(nil)
var _ fs.NodeCreater = (*CubefsNode)(nil)
var _ fs.NodeMkdirer = (*CubefsNode)(nil)
var _ fs.NodeRmdirer = (*CubefsNode)(nil)
var _ fs.NodeUnlinker = (*CubefsNode)(nil)
var _ fs.NodeReader = (*CubefsNode)(nil)
var _ fs.NodeWriter = (*CubefsNode)(nil)
var _ fs.NodeSetattrer = (*CubefsNode)(nil)
var _ fs.NodeFlusher = (*CubefsNode)(nil)
var _ fs.NodeReaddirer = (*CubefsNode)(nil)

// NewCubefsNode creates a new CubefsNode
func NewCubefsNode(adapter *sdk_gofuse.CubefsAdapter, ino uint64, name string) *CubefsNode {
	return &CubefsNode{
		adapter: adapter,
		ino:     ino,
		name:    name,
	}
}

// Getattr returns file attributes
func (n *CubefsNode) Getattr(ctx context.Context, f fs.FileHandle, out *fuse.AttrOut) syscall.Errno {
	info, err := n.adapter.GetInodeInfo(n.ino)
	if err != nil {
		log.LogErrorf("GetInodeInfo failed for inode %d: %v", n.ino, err)
		return syscall.ENOENT
	}

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

// Lookup looks up a child node
func (n *CubefsNode) Lookup(ctx context.Context, name string, out *fuse.EntryOut) (*fs.Inode, syscall.Errno) {
	// For now, we'll create a new node for any lookup
	// In a real implementation, you'd check if the inode exists
	childIno := uint64(0) // This should be looked up from the adapter
	child := NewCubefsNode(n.adapter, childIno, name)

	stable := fs.StableAttr{
		Ino:  childIno,
		Mode: 0755, // Default mode
	}

	return n.EmbeddedInode().NewInode(ctx, child, stable), 0
}

// Create creates a new file
func (n *CubefsNode) Create(ctx context.Context, name string, flags uint32, mode uint32, out *fuse.EntryOut) (node *fs.Inode, fh fs.FileHandle, fuseFlags uint32, errno syscall.Errno) {
	info, err := n.adapter.CreateInode(n.ino, name, mode, 0, 0)
	if err != nil {
		log.LogErrorf("CreateInode failed: %v", err)
		return nil, nil, 0, syscall.EIO
	}

	child := NewCubefsNode(n.adapter, info.Inode, name)
	stable := fs.StableAttr{
		Ino:  info.Inode,
		Mode: info.Mode,
	}

	out.Attr = fuse.Attr{
		Ino:   info.Inode,
		Size:  info.Size,
		Mode:  info.Mode,
		Nlink: info.Nlink,
		Atime: uint64(info.AccessTime.Unix()),
		Mtime: uint64(info.ModifyTime.Unix()),
		Ctime: uint64(info.CreateTime.Unix()),
	}

	return n.EmbeddedInode().NewInode(ctx, child, stable), nil, 0, 0
}

// Mkdir creates a new directory
func (n *CubefsNode) Mkdir(ctx context.Context, name string, mode uint32, out *fuse.EntryOut) (*fs.Inode, syscall.Errno) {
	info, err := n.adapter.CreateInode(n.ino, name, mode|syscall.S_IFDIR, 0, 0)
	if err != nil {
		log.LogErrorf("Mkdir failed: %v", err)
		return nil, syscall.EIO
	}

	child := NewCubefsNode(n.adapter, info.Inode, name)
	stable := fs.StableAttr{
		Ino:  info.Inode,
		Mode: info.Mode,
	}

	out.Attr = fuse.Attr{
		Ino:   info.Inode,
		Size:  info.Size,
		Mode:  info.Mode,
		Nlink: info.Nlink,
		Atime: uint64(info.AccessTime.Unix()),
		Mtime: uint64(info.ModifyTime.Unix()),
		Ctime: uint64(info.CreateTime.Unix()),
	}

	return n.EmbeddedInode().NewInode(ctx, child, stable), 0
}

// Rmdir removes a directory
func (n *CubefsNode) Rmdir(ctx context.Context, name string) syscall.Errno {
	err := n.adapter.DeleteInode(n.ino, name)
	if err != nil {
		log.LogErrorf("Rmdir failed: %v", err)
		return syscall.EIO
	}
	return 0
}

// Unlink removes a file
func (n *CubefsNode) Unlink(ctx context.Context, name string) syscall.Errno {
	err := n.adapter.DeleteInode(n.ino, name)
	if err != nil {
		log.LogErrorf("Unlink failed: %v", err)
		return syscall.EIO
	}
	return 0
}

// Read reads data from a file
func (n *CubefsNode) Read(ctx context.Context, f fs.FileHandle, dest []byte, off int64) (fuse.ReadResult, syscall.Errno) {
	read, err := n.adapter.Read(n.ino, dest, int(off), len(dest))
	if err != nil {
		log.LogErrorf("Read failed: %v", err)
		return nil, syscall.EIO
	}
	return fuse.ReadResultData(dest[:read]), 0
}

// Write writes data to a file
func (n *CubefsNode) Write(ctx context.Context, f fs.FileHandle, data []byte, off int64) (written uint32, errno syscall.Errno) {
	write, err := n.adapter.Write(n.ino, data, int(off), 0)
	if err != nil {
		log.LogErrorf("Write failed: %v", err)
		return 0, syscall.EIO
	}
	return uint32(write), 0
}

// Setattr sets file attributes
func (n *CubefsNode) Setattr(ctx context.Context, f fs.FileHandle, in *fuse.SetAttrIn, out *fuse.AttrOut) syscall.Errno {
	if in.Valid&fuse.FATTR_SIZE != 0 {
		err := n.adapter.Truncate(n.ino, in.Size)
		if err != nil {
			log.LogErrorf("Truncate failed: %v", err)
			return syscall.EIO
		}
	}

	// Get updated attributes
	info, err := n.adapter.GetInodeInfo(n.ino)
	if err != nil {
		log.LogErrorf("GetInodeInfo failed: %v", err)
		return syscall.EIO
	}

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

// Flush flushes file data
func (n *CubefsNode) Flush(ctx context.Context, f fs.FileHandle) syscall.Errno {
	err := n.adapter.Flush(n.ino)
	if err != nil {
		log.LogErrorf("Flush failed: %v", err)
		return syscall.EIO
	}
	return 0
}

// Readdir reads directory entries
func (n *CubefsNode) Readdir(ctx context.Context) (fs.DirStream, syscall.Errno) {
	entries, err := n.adapter.ReadDir(n.ino)
	if err != nil {
		log.LogErrorf("ReadDir failed: %v", err)
		return nil, syscall.EIO
	}

	var dirEntries []fuse.DirEntry
	for _, entry := range entries {
		dirEntries = append(dirEntries, fuse.DirEntry{
			Ino:  entry.Inode,
			Name: entry.Name,
			Mode: entry.Type,
		})
	}

	return fs.NewListDirStream(dirEntries), 0
}

// CubefsRoot represents the root of the Cubefs filesystem
type CubefsRoot struct {
	fs.Inode
	adapter *sdk_gofuse.CubefsAdapter
}

// Ensure CubefsRoot implements required interfaces
var _ fs.NodeLookuper = (*CubefsRoot)(nil)
var _ fs.NodeCreater = (*CubefsRoot)(nil)
var _ fs.NodeMkdirer = (*CubefsRoot)(nil)
var _ fs.NodeRmdirer = (*CubefsRoot)(nil)
var _ fs.NodeUnlinker = (*CubefsRoot)(nil)
var _ fs.NodeReader = (*CubefsRoot)(nil)
var _ fs.NodeWriter = (*CubefsRoot)(nil)
var _ fs.NodeSetattrer = (*CubefsRoot)(nil)
var _ fs.NodeFlusher = (*CubefsRoot)(nil)
var _ fs.NodeReaddirer = (*CubefsRoot)(nil)

// NewCubefsRoot creates a new CubefsRoot
func NewCubefsRoot(adapter *sdk_gofuse.CubefsAdapter) *CubefsRoot {
	return &CubefsRoot{
		adapter: adapter,
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
	// For root, we'll create a new node for any lookup
	childIno := uint64(0) // This should be looked up from the adapter
	child := NewCubefsNode(r.adapter, childIno, name)

	stable := fs.StableAttr{
		Ino:  childIno,
		Mode: 0755, // Default mode
	}

	return r.EmbeddedInode().NewInode(ctx, child, stable), 0
}

// Create creates a new file in root
func (r *CubefsRoot) Create(ctx context.Context, name string, flags uint32, mode uint32, out *fuse.EntryOut) (node *fs.Inode, fh fs.FileHandle, fuseFlags uint32, errno syscall.Errno) {
	info, err := r.adapter.CreateInode(1, name, mode, 0, 0) // Use inode 1 as parent for root
	if err != nil {
		log.LogErrorf("CreateInode failed: %v", err)
		return nil, nil, 0, syscall.EIO
	}

	child := NewCubefsNode(r.adapter, info.Inode, name)
	stable := fs.StableAttr{
		Ino:  info.Inode,
		Mode: info.Mode,
	}

	out.Attr = fuse.Attr{
		Ino:   info.Inode,
		Size:  info.Size,
		Mode:  info.Mode,
		Nlink: info.Nlink,
		Atime: uint64(info.AccessTime.Unix()),
		Mtime: uint64(info.ModifyTime.Unix()),
		Ctime: uint64(info.CreateTime.Unix()),
	}

	return r.EmbeddedInode().NewInode(ctx, child, stable), nil, 0, 0
}

// Mkdir creates a new directory in root
func (r *CubefsRoot) Mkdir(ctx context.Context, name string, mode uint32, out *fuse.EntryOut) (*fs.Inode, syscall.Errno) {
	info, err := r.adapter.CreateInode(1, name, mode|syscall.S_IFDIR, 0, 0) // Use inode 1 as parent for root
	if err != nil {
		log.LogErrorf("Mkdir failed: %v", err)
		return nil, syscall.EIO
	}

	child := NewCubefsNode(r.adapter, info.Inode, name)
	stable := fs.StableAttr{
		Ino:  info.Inode,
		Mode: info.Mode,
	}

	out.Attr = fuse.Attr{
		Ino:   info.Inode,
		Size:  info.Size,
		Mode:  info.Mode,
		Nlink: info.Nlink,
		Atime: uint64(info.AccessTime.Unix()),
		Mtime: uint64(info.ModifyTime.Unix()),
		Ctime: uint64(info.CreateTime.Unix()),
	}

	return r.EmbeddedInode().NewInode(ctx, child, stable), 0
}

// Rmdir removes a directory from root
func (r *CubefsRoot) Rmdir(ctx context.Context, name string) syscall.Errno {
	err := r.adapter.DeleteInode(1, name) // Use inode 1 as parent for root
	if err != nil {
		log.LogErrorf("Rmdir failed: %v", err)
		return syscall.EIO
	}
	return 0
}

// Unlink removes a file from root
func (r *CubefsRoot) Unlink(ctx context.Context, name string) syscall.Errno {
	err := r.adapter.DeleteInode(1, name) // Use inode 1 as parent for root
	if err != nil {
		log.LogErrorf("Unlink failed: %v", err)
		return syscall.EIO
	}
	return 0
}

// Read reads data from root (should not be called)
func (r *CubefsRoot) Read(ctx context.Context, f fs.FileHandle, dest []byte, off int64) (fuse.ReadResult, syscall.Errno) {
	return nil, syscall.EISDIR
}

// Write writes data to root (should not be called)
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
	entries, err := r.adapter.ReadDir(1) // Use inode 1 for root
	if err != nil {
		log.LogErrorf("ReadDir failed: %v", err)
		return nil, syscall.EIO
	}

	var dirEntries []fuse.DirEntry
	for _, entry := range entries {
		dirEntries = append(dirEntries, fuse.DirEntry{
			Ino:  entry.Inode,
			Name: entry.Name,
			Mode: entry.Type,
		})
	}

	return fs.NewListDirStream(dirEntries), 0
}

func main() {
	var (
		mountPoint = flag.String("mount", DefaultMountPoint, "Mount point")
		volumeName = flag.String("volume", DefaultVolumeName, "Volume name")
		masters    = flag.String("masters", DefaultMasters, "Master addresses (comma-separated)")
		debug      = flag.Bool("debug", false, "Enable debug logging")
	)
	flag.Parse()

	// Set up logging
	if *debug {
		log.LogDebugf("Debug mode enabled")
	}

	// Parse master addresses
	masterAddrs := []string{*masters}
	if len(*masters) > 0 {
		// Split by comma if multiple masters
		// For now, just use the first one
	}

	// Create Cubefs adapter
	adapter, err := sdk_gofuse.NewCubefsAdapter(*volumeName, masterAddrs)
	if err != nil {
		fmt.Printf("Failed to create Cubefs adapter: %v\n", err)
		os.Exit(1)
	}
	defer adapter.Close()

	// Create root node
	root := NewCubefsRoot(adapter)

	// Create server
	server, err := fs.Mount(*mountPoint, root, &fs.Options{
		MountOptions: fuse.MountOptions{
			Debug: *debug,
		},
	})
	if err != nil {
		fmt.Printf("Failed to mount: %v\n", err)
		os.Exit(1)
	}
	defer server.Unmount()

	fmt.Printf("Cubefs Go-FUSE client mounted at %s\n", *mountPoint)
	fmt.Printf("Volume: %s\n", *volumeName)
	fmt.Printf("Masters: %v\n", masterAddrs)
	fmt.Printf("Press Ctrl+C to unmount\n")

	// Wait for interrupt signal
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	<-sigCh

	fmt.Printf("Unmounting...\n")
}
