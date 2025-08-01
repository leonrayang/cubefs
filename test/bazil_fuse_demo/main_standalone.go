// Copyright 2025 The CubeFS Authors.
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
	"path/filepath"
	"sync"
	"time"

	"syscall"

	"github.com/cubefs/cubefs/depends/bazil.org/fuse"
	"github.com/cubefs/cubefs/depends/bazil.org/fuse/fs"
)

const (
	// Default mount point
	DefaultMountPoint = "/tmp/cubefs_demo"
	// Default data directory for local storage
	DefaultDataDir = "/tmp/cubefs_demo_data"
)

// LocalFileSystem implements a simple local file system
type LocalFileSystem struct {
	mu      sync.RWMutex
	dataDir string
	root    *LocalDir
	nextIno uint64
	handles map[fuse.HandleID]*LocalFile
	nextHID fuse.HandleID
}

// LocalDir represents a directory in the local file system
type LocalDir struct {
	fs       *LocalFileSystem
	ino      uint64
	name     string
	parent   *LocalDir
	children map[string]*LocalNode
	mu       sync.RWMutex
}

// LocalFile represents a file in the local file system
type LocalFile struct {
	fs       *LocalFileSystem
	ino      uint64
	name     string
	parent   *LocalDir
	dataPath string // Path to the actual file on disk
	size     uint64
	mu       sync.RWMutex
	handleID fuse.HandleID
}

// LocalNode is a union of LocalDir and LocalFile
type LocalNode struct {
	Dir  *LocalDir
	File *LocalFile
}

// NewLocalFileSystem creates a new local file system
func NewLocalFileSystem(dataDir string) *LocalFileSystem {
	fs := &LocalFileSystem{
		dataDir: dataDir,
		handles: make(map[fuse.HandleID]*LocalFile),
		nextIno: 1,
		nextHID: 1,
	}

	// Create root directory
	fs.root = &LocalDir{
		fs:       fs,
		ino:      fs.nextIno,
		name:     "",
		parent:   nil,
		children: make(map[string]*LocalNode),
	}
	fs.nextIno++

	// Create data directory if it doesn't exist
	if err := os.MkdirAll(dataDir, 0755); err != nil {
		fmt.Printf("Failed to create data directory: %v\n", err)
	}

	return fs
}

// Root returns the root directory
func (lfs *LocalFileSystem) Root() (fs.Node, error) {
	return lfs.root, nil
}

// Node returns a node by inode number
func (lfs *LocalFileSystem) Node(ino, pino uint64, mode uint32) (fs.Node, error) {
	// For this simple demo, we'll just return the root
	// In a real implementation, you'd look up the node by inode
	return lfs.root, nil
}

// State returns the filesystem state
func (lfs *LocalFileSystem) State() (fs.FSStatType, string) {
	return fs.FSStatResume, "running"
}

// Notify sends a notification
func (lfs *LocalFileSystem) Notify(stat fs.FSStatType, msg interface{}) {
	// For this simple demo, we don't need to do anything
}

// Attr returns file attributes
func (d *LocalDir) Attr(ctx context.Context, a *fuse.Attr) error {
	d.mu.RLock()
	defer d.mu.RUnlock()

	a.Inode = d.ino
	a.Mode = os.ModeDir | 0755
	a.Size = 0
	a.Blocks = 0
	a.Atime = time.Now()
	a.Mtime = time.Now()
	a.Ctime = time.Now()
	a.Crtime = time.Now()
	a.Nlink = 2 // . and ..

	return nil
}

// Attr returns file attributes
func (f *LocalFile) Attr(ctx context.Context, a *fuse.Attr) error {
	f.mu.RLock()
	defer f.mu.RUnlock()

	// Get file info from disk
	info, err := os.Stat(f.dataPath)
	if err != nil {
		// If file doesn't exist, return default attributes
		a.Inode = f.ino
		a.Mode = 0644
		a.Size = 0
		a.Blocks = 0
		a.Atime = time.Now()
		a.Mtime = time.Now()
		a.Ctime = time.Now()
		a.Crtime = time.Now()
		a.Nlink = 1
		return nil
	}

	a.Inode = f.ino
	a.Mode = info.Mode()
	a.Size = uint64(info.Size())
	a.Blocks = (uint64(info.Size()) + 511) / 512
	a.Atime = info.ModTime()
	a.Mtime = info.ModTime()
	a.Ctime = info.ModTime()
	a.Crtime = info.ModTime()
	a.Nlink = 1

	// Update cached size
	f.size = uint64(info.Size())

	return nil
}

// Lookup finds a file or directory in the directory
func (d *LocalDir) Lookup(ctx context.Context, req *fuse.LookupRequest, resp *fuse.LookupResponse) (fs.Node, error) {
	d.mu.RLock()
	defer d.mu.RUnlock()

	if child, exists := d.children[req.Name]; exists {
		if child.Dir != nil {
			return child.Dir, nil
		}
		return child.File, nil
	}

	return nil, fuse.ENOENT
}

// Create creates a new file
func (d *LocalDir) Create(ctx context.Context, req *fuse.CreateRequest, resp *fuse.CreateResponse) (fs.Node, fs.Handle, error) {
	d.mu.Lock()
	defer d.mu.Unlock()

	// Check if file already exists
	if child, exists := d.children[req.Name]; exists {
		if child.File != nil {
			// File exists, return it
			return child.File, child.File, nil
		}
		// Directory exists, can't create file
		return nil, nil, fuse.EEXIST
	}

	// Generate file path on disk
	filePath := filepath.Join(d.fs.dataDir, fmt.Sprintf("file_%d", d.fs.nextIno))

	// Create new file
	file := &LocalFile{
		fs:       d.fs,
		ino:      d.fs.nextIno,
		name:     req.Name,
		parent:   d,
		dataPath: filePath,
		size:     0,
	}
	d.fs.nextIno++

	// Create empty file on disk
	if err := os.WriteFile(filePath, []byte{}, 0644); err != nil {
		return nil, nil, err
	}

	// Add to parent's children
	d.children[req.Name] = &LocalNode{File: file}

	return file, file, nil
}

// Mkdir creates a new directory
func (d *LocalDir) Mkdir(ctx context.Context, req *fuse.MkdirRequest) (fs.Node, error) {
	d.mu.Lock()
	defer d.mu.Unlock()

	// Check if directory already exists
	if child, exists := d.children[req.Name]; exists {
		if child.Dir != nil {
			return child.Dir, nil
		}
		return nil, fuse.EEXIST
	}

	// Create new directory
	dir := &LocalDir{
		fs:       d.fs,
		ino:      d.fs.nextIno,
		name:     req.Name,
		parent:   d,
		children: make(map[string]*LocalNode),
	}
	d.fs.nextIno++

	// Add to parent's children
	d.children[req.Name] = &LocalNode{Dir: dir}

	return dir, nil
}

// Remove removes a file or directory
func (d *LocalDir) Remove(ctx context.Context, req *fuse.RemoveRequest) error {
	d.mu.Lock()
	defer d.mu.Unlock()

	child, exists := d.children[req.Name]
	if !exists {
		return fuse.ENOENT
	}

	if req.Dir && child.File != nil {
		return syscall.ENOTDIR
	}
	if !req.Dir && child.Dir != nil {
		return syscall.EISDIR
	}

	// If it's a file, delete the actual file on disk
	if child.File != nil {
		if err := os.Remove(child.File.dataPath); err != nil {
			return err
		}
	}

	delete(d.children, req.Name)
	return nil
}

// ReadDir reads directory entries
func (d *LocalDir) ReadDir(ctx context.Context, req *fuse.ReadRequest, resp *fuse.ReadResponse) ([]fuse.Dirent, error) {
	d.mu.RLock()
	defer d.mu.RUnlock()

	var entries []fuse.Dirent

	// Add . and ..
	entries = append(entries, fuse.Dirent{
		Inode: d.ino,
		Name:  ".",
		Type:  fuse.DT_Dir,
	})

	if d.parent != nil {
		entries = append(entries, fuse.Dirent{
			Inode: d.parent.ino,
			Name:  "..",
			Type:  fuse.DT_Dir,
		})
	}

	// Add children
	for name, child := range d.children {
		if child.Dir != nil {
			entries = append(entries, fuse.Dirent{
				Inode: child.Dir.ino,
				Name:  name,
				Type:  fuse.DT_Dir,
			})
		} else if child.File != nil {
			entries = append(entries, fuse.Dirent{
				Inode: child.File.ino,
				Name:  name,
				Type:  fuse.DT_File,
			})
		}
	}

	return entries, nil
}

// Open opens a file
func (f *LocalFile) Open(ctx context.Context, req *fuse.OpenRequest, resp *fuse.OpenResponse) (fs.Handle, error) {
	f.mu.Lock()
	defer f.mu.Unlock()

	// Assign handle ID
	f.handleID = f.fs.nextHID
	f.fs.nextHID++
	f.fs.handles[f.handleID] = f

	return f, nil
}

// Release closes a file
func (f *LocalFile) Release(ctx context.Context, req *fuse.ReleaseRequest) error {
	f.mu.Lock()
	defer f.mu.Unlock()

	delete(f.fs.handles, f.handleID)
	return nil
}

// Read reads data from a file
func (f *LocalFile) Read(ctx context.Context, req *fuse.ReadRequest, resp *fuse.ReadResponse) error {
	f.mu.RLock()
	defer f.mu.RUnlock()

	// Open file for reading
	file, err := os.OpenFile(f.dataPath, os.O_RDONLY, 0644)
	if err != nil {
		return err
	}
	defer file.Close()

	// Seek to the requested offset
	if _, err := file.Seek(req.Offset, io.SeekStart); err != nil {
		return err
	}

	// Read the requested data
	resp.Data = make([]byte, req.Size)
	n, err := file.Read(resp.Data)
	if err != nil && err != io.EOF {
		return err
	}

	// Resize the response data to the actual bytes read
	resp.Data = resp.Data[:n]

	return nil
}

// Write writes data to a file
func (f *LocalFile) Write(ctx context.Context, req *fuse.WriteRequest, resp *fuse.WriteResponse) error {
	f.mu.Lock()
	defer f.mu.Unlock()

	// Open file for writing (create if doesn't exist, append if exists)
	file, err := os.OpenFile(f.dataPath, os.O_WRONLY|os.O_CREATE, 0644)
	if err != nil {
		return err
	}
	defer file.Close()

	// Seek to the requested offset
	if _, err := file.Seek(req.Offset, io.SeekStart); err != nil {
		return err
	}

	// Write the data
	n, err := file.Write(req.Data)
	if err != nil {
		return err
	}

	// Update file size
	if req.Offset+int64(n) > int64(f.size) {
		f.size = uint64(req.Offset + int64(n))
	}

	resp.Size = n
	return nil
}

// Flush flushes file data
func (f *LocalFile) Flush(ctx context.Context, req *fuse.FlushRequest) error {
	// In this simple implementation, we don't need to do anything
	return nil
}

// Fsync synchronizes file data
func (f *LocalFile) Fsync(ctx context.Context, req *fuse.FsyncRequest) error {
	// In this simple implementation, we don't need to do anything
	return nil
}

// Setattr sets file attributes
func (f *LocalFile) Setattr(ctx context.Context, req *fuse.SetattrRequest, resp *fuse.SetattrResponse) error {
	f.mu.Lock()
	defer f.mu.Unlock()

	if req.Valid.Size() {
		// Open file for truncation
		file, err := os.OpenFile(f.dataPath, os.O_WRONLY, 0644)
		if err != nil {
			return err
		}
		defer file.Close()

		// Truncate the file
		if err := file.Truncate(int64(req.Size)); err != nil {
			return err
		}

		f.size = req.Size
	}

	return nil
}

func main() {
	var (
		mountPoint = flag.String("mount", DefaultMountPoint, "Mount point")
		dataDir    = flag.String("data", DefaultDataDir, "Data directory for local storage")
		debug      = flag.Bool("debug", false, "Enable debug logging")
	)
	flag.Parse()

	// Set up logging
	if *debug {
		fmt.Printf("Debug mode enabled\n")
	}

	// Create file system
	lfs := NewLocalFileSystem(*dataDir)

	// Create mount point if it doesn't exist
	if err := os.MkdirAll(*mountPoint, 0755); err != nil {
		fmt.Printf("Failed to create mount point: %v\n", err)
		os.Exit(1)
	}

	// Mount the file system
	conn, err := fuse.Mount(*mountPoint, false, fuse.FSName("cubefs_demo"), fuse.Subtype("cubefs_demo"))
	if err != nil {
		fmt.Printf("Failed to mount: %v\n", err)
		os.Exit(1)
	}
	defer conn.Close()

	fmt.Printf("Cubefs Demo mounted at %s\n", *mountPoint)
	fmt.Printf("Data directory: %s\n", *dataDir)
	fmt.Printf("Press Ctrl+C to unmount\n")

	// Serve the file system
	err = fs.Serve(conn, lfs, nil)
	if err != nil {
		fmt.Printf("Failed to serve: %v\n", err)
		os.Exit(1)
	}

	// Check if the mount process has an error to report
	<-conn.Ready
	if err := conn.MountError; err != nil {
		fmt.Printf("Mount error: %v\n", err)
		os.Exit(1)
	}
}
