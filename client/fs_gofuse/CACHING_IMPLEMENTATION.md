# Caching System Implementation for client_gofuse

## Overview

The caching system for `client_gofuse` has been successfully implemented by reusing the original Cubefs caching code. This implementation provides the same caching functionality as the original client, including dentry cache, inode cache, and node cache.

## Components Implemented

### 1. Cache Manager (`CacheManager`)

The `CacheManager` is the central component that manages all three types of caches:

- **Inode Cache**: Caches inode information for better performance
- **Dentry Cache**: Caches directory entry information 
- **Node Cache**: Caches FUSE node objects to avoid recreation

### 2. Cache Types

#### InodeCache
- **Purpose**: Caches `proto.InodeInfo` objects
- **Key Features**:
  - LRU eviction policy
  - Background eviction with configurable intervals
  - Expiration-based cleanup
  - Thread-safe operations with RWMutex

#### Dcache (Dentry Cache)
- **Purpose**: Caches `proto.DentryInfo` objects
- **Key Features**:
  - LRU eviction policy
  - Background eviction
  - Expiration-based cleanup
  - Thread-safe operations

#### Node Cache
- **Purpose**: Caches FUSE node objects
- **Key Features**:
  - Simple map-based storage
  - Thread-safe operations with RWMutex
  - Automatic cleanup on node deletion

## Configuration

The caching system is configured using the same parameters as the original client:

```go
// Cache configuration from mount options
inodeExpiration := DefaultInodeExpiration
if opt.IcacheTimeout >= 0 {
    inodeExpiration = time.Duration(opt.IcacheTimeout) * time.Second
}

maxInodeCache := DefaultMaxInodeCache
if opt.MaxStreamerLimit > 0 {
    maxInodeCache = MaxInodeCache
}

cacheManager := NewCacheManager(inodeExpiration, maxInodeCache)
```

## Integration with FUSE Operations

### Lookup Operation
The `Lookup` method now includes caching:

```go
func (n *CubefsNode) Lookup(ctx context.Context, name string, out *fuse.EntryOut) (*fs.Inode, syscall.Errno) {
    // Check node cache first
    if node, ok := n.cache.GetNode(n.ino); ok {
        return node.(*fs.Inode), 0
    }
    
    // Create new node and cache it
    child := NewCubefsNode(n.adapter, n.cache, childIno, name)
    newInode := n.NewInode(ctx, child, stable)
    n.cache.PutNode(childIno, newInode)
    
    return newInode, 0
}
```

### Create/Mkdir Operations
These operations also cache the newly created nodes:

```go
func (n *CubefsNode) Create(ctx context.Context, name string, flags uint32, mode uint32, out *fuse.EntryOut) (node *fs.Inode, fh fs.FileHandle, fuseFlags uint32, errno syscall.Errno) {
    // Create file using adapter
    _, err := n.adapter.CreateInode(n.ino, name, mode, 0, 0)
    if err != nil {
        return nil, nil, 0, syscall.EIO
    }
    
    // Create and cache new node
    child := NewCubefsNode(n.adapter, n.cache, childIno, name)
    newInode := n.NewInode(ctx, child, stable)
    n.cache.PutNode(childIno, newInode)
    
    return newInode, nil, 0, 0
}
```

## Constants and Configuration

The implementation uses the same constants as the original client:

```go
const (
    DefaultInodeExpiration = 120 * time.Second
    MaxInodeCache          = 10000000
    DefaultMaxInodeCache   = 2000000
    
    MinInodeCacheEvictNum = 10
    MaxInodeCacheEvictNum = 200000
    
    MinDentryCacheEvictNum = 10
    MaxDentryCacheEvictNum = 200000
    
    BgEvictionInterval = 2 * time.Minute
)
```

## Background Eviction

Both inode and dentry caches implement background eviction:

- **Foreground Eviction**: Evicts up to `MinInodeCacheEvictNum` items when cache is full
- **Background Eviction**: Runs every `BgEvictionInterval` and evicts up to `MaxInodeCacheEvictNum` expired items

## Thread Safety

All cache operations are thread-safe:

- **InodeCache**: Uses `sync.RWMutex` for read/write operations
- **Dcache**: Uses `sync.RWMutex` for read/write operations  
- **NodeCache**: Uses `sync.RWMutex` for read/write operations

## Performance Benefits

The caching system provides several performance benefits:

1. **Reduced Metadata Lookups**: Cached inode and dentry information reduces calls to the metadata service
2. **Faster Node Creation**: Cached FUSE nodes avoid recreation overhead
3. **Improved Read Performance**: Cached directory entries speed up directory listings
4. **Memory Efficiency**: LRU eviction and background cleanup prevent memory leaks

## Usage

The caching system is automatically initialized in the main function:

```go
// Create cache manager with configuration from mount options
cacheManager := NewCacheManager(inodeExpiration, maxInodeCache)

// Create root node with cache manager
root := NewCubefsRoot(adapter, cacheManager)
```

All FUSE operations automatically use the cache through the `CubefsNode` and `CubefsRoot` structs.

## Compatibility

This implementation maintains full compatibility with the original Cubefs client's caching behavior while adapting to the `go-fuse` library's API requirements. 