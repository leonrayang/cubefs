package main

import (
	"context"
	"syscall"

	"github.com/hanwen/go-fuse/v2/fs"
	"github.com/hanwen/go-fuse/v2/fuse"
	"github.com/cubefs/cubefs/sdk/gofuse_adapter"
	"github.com/cubefs/cubefs/proto"
)

// FileSystemInterface defines the interface for file system operations
type FileSystemInterface interface {
	// Metadata operations
	Lookup(parentIno uint64, name string) (uint64, error)
	GetInodeInfo(ino uint64) (*sdk_gofuse.InodeInfo, error)
	CreateInode(parentIno uint64, name string, mode uint32, uid, gid uint32) (*sdk_gofuse.InodeInfo, error)
	DeleteInode(parentIno uint64, name string) error

	// Data operations
	Read(ino uint64, data []byte, offset int, size int) (int, error)
	Write(ino uint64, data []byte, offset int, flags int) (int, error)
	Truncate(ino uint64, size uint64) error
	Setattr(ino uint64, mode uint32, uid, gid uint32, atime, mtime time.Time) error
	Flush(ino uint64) error

	// Directory operations
	ReadDir(ino uint64) ([]*sdk_gofuse.DirEntry, error)
	ReadDirLimit(ino uint64, from string, limit uint64) ([]*sdk_gofuse.DirEntry, error)

	// Cleanup
	Close() error
}



// CacheInterface defines the interface for caching operations
type CacheInterface interface {
	// Node cache operations
	PutNode(ino uint64, node interface{})
	GetNode(ino uint64) (interface{}, bool)
	DeleteNode(ino uint64)

	// Dentry cache operations
	GetDentryCache() DentryCacheInterface
}

// DentryCacheInterface defines the interface for dentry cache operations
type DentryCacheInterface interface {
	Put(dentry *proto.DentryInfo)
	Get(key string) *proto.DentryInfo
	Delete(key string)
}


// FuseNodeInterface defines the interface for FUSE node operations
type FuseNodeInterface interface {
	// FUSE operations
	Getattr(ctx context.Context, f FileHandle, out *fuse.AttrOut) syscall.Errno
	Lookup(ctx context.Context, name string, out *fuse.EntryOut) (*fs.Inode, syscall.Errno)
	Create(ctx context.Context, name string, flags uint32, mode uint32, out *fuse.EntryOut) (*fs.Inode, FileHandle, uint32, syscall.Errno)
	Mkdir(ctx context.Context, name string, mode uint32, out *fuse.EntryOut) (*fs.Inode, syscall.Errno)
	Rmdir(ctx context.Context, name string) syscall.Errno
	Unlink(ctx context.Context, name string) syscall.Errno
	Read(ctx context.Context, f FileHandle, dest []byte, off int64) (fuse.ReadResult, syscall.Errno)
	Write(ctx context.Context, f FileHandle, data []byte, off int64) (written uint32, errno syscall.Errno)
	Setattr(ctx context.Context, f FileHandle, in *fuse.SetAttrIn, out *fuse.AttrOut) syscall.Errno
	Flush(ctx context.Context, f FileHandle) syscall.Errno
	Readdir(ctx context.Context) (fs.DirStream, syscall.Errno)
}

// FileHandle represents a file handle
type FileHandle interface {
	// File handle operations can be added here if needed
}

// FuseRootInterface defines the interface for FUSE root operations
type FuseRootInterface interface {
	FuseNodeInterface
	// Additional root-specific operations can be added here
}
