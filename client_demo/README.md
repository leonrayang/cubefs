# Cubefs Client Demo

This is a simple FUSE client demo that processes FUSE messages and writes to **local disk storage** instead of the distributed backend. This allows you to measure FUSE performance with realistic disk I/O overhead.

## Features

- **Local Disk Storage**: All data is stored on actual disk files for realistic performance
- **FUSE Interface**: Full FUSE interface support for file operations
- **Performance Testing**: Measure FUSE overhead with real disk I/O
- **Configurable Storage**: Specify custom data directory for storage
- **Simple Implementation**: Easy to understand and modify

## How It Works

The demo creates a FUSE filesystem that:
1. **Processes FUSE messages** from the kernel (just like real Cubefs)
2. **Maps to local disk files** in the specified data directory
3. **Performs actual file I/O** operations (read/write/truncate)
4. **Maintains file metadata** in memory for fast lookups

### Storage Layout

```
Data Directory: /tmp/cubefs_demo_data/
├── file_1    # Actual file on disk for inode 1
├── file_2    # Actual file on disk for inode 2
├── file_3    # Actual file on disk for inode 3
└── ...
```

Each file in the FUSE filesystem corresponds to an actual file on disk, providing realistic I/O performance.

## Building

```bash
# Build the demo
go build -o cubefs_demo_standalone main_standalone.go

# Or build manually
go build -o cubefs_demo_standalone main_standalone.go
```

## Usage

### Basic Usage

```bash
# Run with default settings
./cubefs_demo_standalone

# Run with custom mount point and data directory
./cubefs_demo_standalone -mount /tmp/my_mount -data /tmp/my_data -debug

# Run in background
./cubefs_demo_standalone -mount /tmp/cubefs_demo &
```

### Command Line Options

- `-mount`: Mount point (default: `/tmp/cubefs_demo`)
- `-data`: Data directory for local storage (default: `/tmp/cubefs_demo_data`)
- `-debug`: Enable debug logging

### Testing

```bash
# Run automated test
./test_demo.sh

# Manual testing
# 1. Start the demo
./cubefs_demo_standalone -mount /tmp/cubefs_demo -debug

# 2. In another terminal, test operations
echo "Hello World" > /tmp/cubefs_demo/test.txt
cat /tmp/cubefs_demo/test.txt
ls -la /tmp/cubefs_demo/

# 3. Check actual files on disk
ls -la /tmp/cubefs_demo_data/

# 4. Create directories
mkdir /tmp/cubefs_demo/mydir
echo "test" > /tmp/cubefs_demo/mydir/file.txt

# 5. Unmount when done
sudo umount /tmp/cubefs_demo
```

## Performance Testing

### Benchmark Commands

```bash
# File creation benchmark
time for i in {1..1000}; do echo "test" > /tmp/cubefs_demo/file$i.txt; done

# File read benchmark
time for i in {1..1000}; do cat /tmp/cubefs_demo/file$i.txt > /dev/null; done

# Directory listing benchmark
time for i in {1..100}; do ls /tmp/cubefs_demo/ > /dev/null; done

# Large file write benchmark
dd if=/dev/zero of=/tmp/cubefs_demo/large_file bs=1M count=100

# Large file read benchmark
dd if=/tmp/cubefs_demo/large_file of=/dev/null bs=1M
```

### Performance Comparison

```bash
# Compare with local filesystem
time dd if=/dev/zero of=/tmp/local_test.bin bs=1M count=100
time dd if=/tmp/cubefs_demo/large_file.bin of=/dev/null bs=1M

# Compare with real Cubefs (when available)
# 1. Start Cubefs cluster
# 2. Mount real Cubefs
# 3. Run same benchmarks
# 4. Compare performance
```

## Implementation Details

### Architecture

- **LocalFileSystem**: Main file system implementation
- **LocalDir**: Directory implementation with children tracking
- **LocalFile**: File implementation with disk-based storage
- **FUSE Interface**: Full FUSE protocol support

### Key Features

1. **Disk-Based Storage**: All file data is stored on actual disk files
2. **Thread-Safe**: Uses mutexes for concurrent access
3. **FUSE Compliance**: Implements all required FUSE interfaces
4. **Realistic I/O**: Performs actual file system operations
5. **Configurable Storage**: Specify custom data directory

### File Operations

- **Read**: Reads from actual disk files using `os.OpenFile` and `file.Read`
- **Write**: Writes to actual disk files using `os.OpenFile` and `file.Write`
- **Truncate**: Uses `file.Truncate` for file size changes
- **Delete**: Uses `os.Remove` to delete actual files
- **Attributes**: Gets real file info using `os.Stat`

### Limitations

- No advanced features like symlinks, hard links, or extended attributes
- No quota or permission management
- No data compression or encryption
- Simple file naming scheme (file_<inode>)

## Development

### Adding Features

To add more features, modify the appropriate methods:

- **Symlinks**: Implement `Symlink` method in `LocalDir`
- **Hard Links**: Add link count tracking in `LocalFile`
- **Extended Attributes**: Implement `Getxattr`, `Setxattr`, etc.
- **Better File Naming**: Use hash-based or hierarchical file naming

### Debugging

Enable debug logging with the `-debug` flag:

```bash
./cubefs_demo_standalone -debug
```

This will show detailed FUSE operation logs.

## Troubleshooting

### Common Issues

1. **Permission Denied**: Make sure you have permission to create mount points and data directories
2. **Mount Point Busy**: Unmount any existing filesystem at the mount point
3. **FUSE Not Available**: Install FUSE kernel module and user tools
4. **Disk Space**: Ensure sufficient disk space in the data directory

### Debug Commands

```bash
# Check if FUSE is available
lsmod | grep fuse

# Check mount points
mount | grep cubefs

# Check file system
df -h /tmp/cubefs_demo

# Check actual files on disk
ls -la /tmp/cubefs_demo_data/

# Check logs
dmesg | tail -20
```

## Performance Analysis

### What This Demo Measures

1. **FUSE Overhead**: Time spent in FUSE message processing
2. **File System Overhead**: Time spent in file system operations
3. **Disk I/O Performance**: Actual disk read/write performance
4. **Memory Usage**: Memory overhead of the FUSE implementation

### Comparison Points

- **vs Local Filesystem**: Measures FUSE overhead
- **vs In-Memory Storage**: Measures disk I/O impact
- **vs Real Cubefs**: Measures network/distributed overhead

## License

This demo is part of the Cubefs project and follows the same license terms. 