# Go-FUSE Library Structure

## Overview

The go-fuse library is located in the Go module cache and has been copied to the `vendor/` directory for better IDE support.

## Library Locations

### Primary Location (Go Module Cache)
```
/home/leonchang/go/pkg/mod/github.com/hanwen/go-fuse/v2@v2.1.0/
```

### Local Copy (for IDE support)
```
client_gofuse/vendor/github.com/hanwen/go-fuse/v2/
```

## Key Directories

### `fs/` - File System Interfaces
Contains the core file system interfaces and implementations:

- **`api.go`**: Main file system API definitions
- **`bridge.go`**: Bridge between FUSE and file system
- **`constants.go`**: FUSE constants and flags
- **`inode.go`**: Inode management
- **`node.go`**: Node interface definitions

### `fuse/` - FUSE Protocol
Contains FUSE protocol definitions:

- **`attr.go`**: Attribute structures
- **`const.go`**: FUSE constants
- **`types.go`**: FUSE data types

## Key Interfaces

### FileHandle Interface
```go
type FileHandle interface {
}
```

The `FileHandle` is an empty interface that serves as a base for file handle implementations.

### Node Interface
```go
type Node interface {
    // Core node operations
    Lookup(ctx context.Context, name string, out *fuse.EntryOut) (*Inode, syscall.Errno)
    Getattr(ctx context.Context, f FileHandle, out *fuse.AttrOut) syscall.Errno
    Setattr(ctx context.Context, f FileHandle, in *fuse.SetAttrIn, out *fuse.AttrOut) syscall.Errno
}
```

### File Operation Interfaces

#### FileReader
```go
type FileReader interface {
    Read(ctx context.Context, dest []byte, off int64) (fuse.ReadResult, syscall.Errno)
}
```

#### FileWriter
```go
type FileWriter interface {
    Write(ctx context.Context, data []byte, off int64) (written uint32, errno syscall.Errno)
}
```

#### FileReleaser
```go
type FileReleaser interface {
    Release(ctx context.Context) syscall.Errno
}
```

## Important Structures

### fuse.Attr
```go
type Attr struct {
    Ino       uint64
    Size      uint64
    Blocks    uint64
    Atime     uint64
    Mtime     uint64
    Ctime     uint64
    Mode      uint32
    Nlink     uint32
    // Note: Uid and Gid are embedded, not direct fields
}
```

### fs.StableAttr
```go
type StableAttr struct {
    Ino  uint64
    Gen  uint64
    Mode uint32
}
```

## Usage in Our Implementation

### CubefsNode Structure
```go
type CubefsNode struct {
    fs.Inode           // Embedded for FUSE functionality
    adapter *sdk_gofuse.CubefsAdapter
    ino     uint64
    name    string
}
```

### Key Methods Implementation
```go
// Getattr - Get file attributes
func (n *CubefsNode) Getattr(ctx context.Context, f fs.FileHandle, out *fuse.AttrOut) syscall.Errno

// Read - Read file data
func (n *CubefsNode) Read(ctx context.Context, f fs.FileHandle, dest []byte, off int64) (fuse.ReadResult, syscall.Errno)

// Write - Write file data
func (n *CubefsNode) Write(ctx context.Context, f fs.FileHandle, data []byte, off int64) (written uint32, errno syscall.Errno)
```

## IDE Navigation

With the library copied to `vendor/`, you can now:

1. **Jump to Definition**: Ctrl+Click on `fs.FileHandle` to see the interface definition
2. **Browse Source**: Navigate through the go-fuse source code in `vendor/`
3. **Auto-completion**: Get proper IDE support for go-fuse types

## Key Files for Reference

### Core API (`vendor/github.com/hanwen/go-fuse/v2/fs/api.go`)
- Contains all the main interfaces and type definitions
- Lines 502-520: FileHandle and related interfaces

### FUSE Types (`vendor/github.com/hanwen/go-fuse/v2/fuse/types.go`)
- Contains FUSE protocol type definitions
- Important for understanding data structures

### Examples (`vendor/github.com/hanwen/go-fuse/v2/example/`)
- Contains working examples of FUSE implementations
- Good reference for implementation patterns

## Common Patterns

### 1. Node Implementation
```go
type MyNode struct {
    fs.Inode
    // Your fields here
}

// Implement required interfaces
func (n *MyNode) Getattr(ctx context.Context, f fs.FileHandle, out *fuse.AttrOut) syscall.Errno
func (n *MyNode) Lookup(ctx context.Context, name string, out *fuse.EntryOut) (*fs.Inode, syscall.Errno)
```

### 2. File Handle Implementation
```go
type MyFileHandle struct {
    // Your file handle fields
}

// Implement file operations
func (fh *MyFileHandle) Read(ctx context.Context, dest []byte, off int64) (fuse.ReadResult, syscall.Errno)
func (fh *MyFileHandle) Write(ctx context.Context, data []byte, off int64) (written uint32, errno syscall.Errno)
func (fh *MyFileHandle) Release(ctx context.Context) syscall.Errno
```

### 3. Mount and Serve
```go
server, err := fs.Mount(mountPoint, root, &fs.Options{
    MountOptions: fuse.MountOptions{
        Debug: debug,
    },
})
```

## Error Handling

Go-FUSE uses `syscall.Errno` for error codes:

```go
import "syscall"

// Common error codes
return syscall.ENOENT  // No such file or directory
return syscall.EIO     // Input/output error
return syscall.EISDIR  // Is a directory
return syscall.ENOTDIR // Not a directory
return 0               // Success
```

## Performance Considerations

1. **Memory Management**: Go-FUSE has better memory allocation than bazil.org/fuse
2. **Concurrency**: Better handling of concurrent operations
3. **I/O Optimization**: More efficient read/write operations
4. **Context Switches**: Reduced kernel-user communication overhead

## Debugging

Enable debug mode for detailed logging:

```go
server, err := fs.Mount(mountPoint, root, &fs.Options{
    MountOptions: fuse.MountOptions{
        Debug: true,
    },
})
```

This will provide detailed FUSE operation logs for debugging. 