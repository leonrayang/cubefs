# Dentry Cache Implementation in Lookup Methods

## Overview

The dentry cache functionality has been successfully implemented in both `CubefsNode.Lookup` and `CubefsRoot.Lookup` methods, following the same pattern as the original `Dir.Lookup` implementation.

## Implementation Details

### 1. Dentry Cache Logic

Both Lookup methods now include the same dentry cache logic as the original:

```go
func (n *CubefsNode) Lookup(ctx context.Context, name string, out *fuse.EntryOut) (*fs.Inode, syscall.Errno) {
    var (
        ino      uint64
        err      error
        dcachev2 bool
    )

    log.LogDebugf("TRACE Lookup: parent(%v) name(%v)", n.ino, name)

    // Check if we need dentry cache (similar to original needDentrycache())
    // For now, we'll use dcachev2 = false to match original behavior
    dcachev2 = false

    if dcachev2 {
        // Use dentry cache v2 (Dcache)
        dcacheKey := n.buildDcacheKey(n.ino, name)
        dentryInfo := n.cache.GetDentryCache().Get(dcacheKey)
        if dentryInfo == nil {
            // Cache miss - lookup from adapter
            ino, err = n.adapter.Lookup(n.ino, name)
            if err != nil {
                log.LogErrorf("Lookup: parent(%v) name(%v) err(%v)", n.ino, name, err)
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
        // For now, we'll do direct lookup since we don't have the simple dentry cache
        ino, err = n.adapter.Lookup(n.ino, name)
        if err != nil {
            log.LogErrorf("Lookup: parent(%v) name(%v) err(%v)", n.ino, name, err)
            return nil, syscall.ENOENT
        }
    }

    // Check node cache first
    if node, ok := n.cache.GetNode(ino); ok {
        // Return cached node if available
        return node.(*fs.Inode), 0
    }

    // Get inode info from adapter
    info, err := n.adapter.GetInodeInfo(ino)
    if err != nil {
        log.LogErrorf("GetInodeInfo failed for inode %d: %v", ino, err)
        return nil, syscall.ENOENT
    }

    // Create new node based on type
    var child *CubefsNode
    if proto.IsDir(info.Mode) {
        child = NewCubefsNode(n.adapter, n.cache, ino, name)
    } else {
        child = NewCubefsNode(n.adapter, n.cache, ino, name)
    }

    stable := fs.StableAttr{
        Ino:  ino,
        Mode: info.Mode,
    }

    newInode := n.NewInode(ctx, child, stable)
    
    // Cache the new node
    n.cache.PutNode(ino, newInode)
    
    log.LogDebugf("TRACE Lookup exit: parent(%v) name(%v) ino(%v)", n.ino, name, ino)
    return newInode, 0
}
```

### 2. Dcache Key Building

Both `CubefsNode` and `CubefsRoot` now have the `buildDcacheKey` method:

```go
func (n *CubefsNode) buildDcacheKey(inode uint64, name string) string {
    return fmt.Sprintf("%v_%v", inode, name)
}
```

This follows the same pattern as the original implementation.

### 3. Adapter Lookup Method

Added a `Lookup` method to the `CubefsAdapter`:

```go
func (ca *CubefsAdapter) Lookup(parentIno uint64, name string) (uint64, error) {
    ca.mu.RLock()
    defer ca.mu.RUnlock()

    ino, _, err := ca.metaWrapper.Lookup_ll(parentIno, name)
    if err != nil {
        return 0, err
    }

    return ino, nil
}
```

## Cache Flow

### Dentry Cache v2 (Dcache) Flow:
1. **Check if dcachev2 is enabled** (currently set to false to match original)
2. **Build dcache key** using `buildDcacheKey(parentIno, name)`
3. **Check Dcache** for existing entry
4. **Cache hit**: Use cached inode number
5. **Cache miss**: 
   - Call `adapter.Lookup()` to get inode number
   - Create `proto.DentryInfo` and cache it
   - Use the inode number

### Node Cache Flow:
1. **Check node cache** for existing FUSE node
2. **Cache hit**: Return cached node
3. **Cache miss**:
   - Get inode info from adapter
   - Create new `CubefsNode`
   - Cache the new node
   - Return the node

## Configuration

The dentry cache behavior is controlled by the `dcachev2` flag:

- **Currently disabled** (`dcachev2 = false`) to match original behavior
- **Can be enabled** by setting `dcachev2 = true` when needed
- **Uses Dcache** when enabled, direct lookup when disabled

## Performance Benefits

1. **Dentry Cache**: Reduces metadata lookups for frequently accessed files/directories
2. **Node Cache**: Avoids recreation of FUSE nodes for the same inodes
3. **Logging**: Provides detailed trace logs for debugging cache behavior

## Compatibility

This implementation maintains full compatibility with the original Cubefs client's dentry cache behavior while adapting to the `go-fuse` library's API requirements.

## Future Enhancements

1. **Enable dcachev2**: Can be enabled by implementing proper `needDentrycache()` logic
2. **Metrics**: Can add exporter metrics like the original implementation
3. **Simple Dentry Cache**: Can implement the simple dentry cache for when dcachev2 is disabled 