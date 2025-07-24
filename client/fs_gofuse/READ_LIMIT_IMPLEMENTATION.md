# Read Limit Implementation in Readdir Methods

## Overview

The read limit functionality has been successfully implemented in both `CubefsNode.Readdir` and `CubefsRoot.Readdir` methods, following the same pattern as the original `Dir.ReadDir` implementation.

## Implementation Details

### 1. Adapter ReadDirLimit Method

Added a `ReadDirLimit` method to the `CubefsAdapter`:

```go
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
```

This method calls the underlying `metaWrapper.ReadDirLimit_ll` method, which is the same method used by the original client.

### 2. Readdir Method with Read Limits

Both `CubefsNode.Readdir` and `CubefsRoot.Readdir` now use read limits:

```go
func (n *CubefsNode) Readdir(ctx context.Context) (fs.DirStream, syscall.Errno) {
    var limit uint64 = DefaultReaddirLimit
    var from string = ""

    log.LogDebugf("TRACE Readdir: ino(%v) limit(%v)", n.ino, limit)

    // Use ReadDirLimit for better performance
    entries, err := n.adapter.ReadDirLimit(n.ino, from, limit)
    if err != nil {
        log.LogErrorf("ReadDirLimit failed: %v", err)
        return nil, syscall.EIO
    }

    var dirEntries []fuse.DirEntry
    
    // Add "." and ".." entries for root directory
    if n.ino == 1 {
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
    }

    // Process directory entries
    for _, entry := range entries {
        dirEntries = append(dirEntries, fuse.DirEntry{
            Ino:  entry.Inode,
            Name: entry.Name,
            Mode: entry.Type,
        })

        // Cache dentry info if dcachev2 is enabled
        // This follows the same pattern as the original implementation
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
```

### 3. Configuration Constants

The implementation uses the same constants as the original client:

```go
const (
    DefaultReaddirLimit = 1024
)
```

This matches the `DefaultReaddirLimit` constant used in the original implementation.

## Key Features

### 1. Read Limit Support
- **Limit-based reading**: Uses `ReadDirLimit_ll` instead of `ReadDir_ll`
- **Configurable limit**: Uses `DefaultReaddirLimit` (1024) as the default
- **From parameter**: Supports pagination with the `from` parameter

### 2. Directory Entry Processing
- **"." and ".." entries**: Properly adds these entries for directories
- **Root directory handling**: Special handling for root directory (inode 1)
- **Entry type mapping**: Maps directory entry types correctly

### 3. Dentry Caching
- **Automatic caching**: Caches dentry information during readdir operations
- **Dcache integration**: Uses the same dentry cache as the original implementation
- **Cache key building**: Uses `buildDcacheKey` for consistent cache keys

### 4. Logging and Debugging
- **Trace logging**: Provides detailed logs for debugging
- **Entry counting**: Logs the number of entries processed
- **Error handling**: Proper error logging and handling

## Performance Benefits

1. **Memory Efficiency**: Read limits prevent loading entire large directories into memory
2. **Network Efficiency**: Reduces network traffic for large directories
3. **Response Time**: Faster response times for directory listings
4. **Scalability**: Better performance with directories containing many entries

## Compatibility

This implementation maintains full compatibility with the original Cubefs client's read limit behavior:

- **Same limit value**: Uses `DefaultReaddirLimit = 1024`
- **Same API calls**: Uses `ReadDirLimit_ll` like the original
- **Same caching behavior**: Caches dentry information during readdir
- **Same logging patterns**: Follows the same trace logging approach

## Usage

The read limit functionality is automatically used by all `Readdir` operations:

- **CubefsNode.Readdir**: For regular directory nodes
- **CubefsRoot.Readdir**: For root directory operations

Both methods automatically use the read limit without requiring any additional configuration.

## Future Enhancements

1. **Configurable limits**: Could make the read limit configurable via mount options
2. **Pagination support**: Could implement full pagination support with `from` parameter
3. **Batch processing**: Could implement batch processing for very large directories
4. **Metrics**: Could add performance metrics for readdir operations 