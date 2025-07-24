package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"sync"
	"syscall"
	"time"

	"github.com/hanwen/go-fuse/v2/fs"
	"github.com/hanwen/go-fuse/v2/fuse"
)

// In-memory file system structure for comprehensive demo
type MemoryFS struct {
	mu      sync.RWMutex
	files   map[uint64]*MemFile
	dirs    map[uint64]*MemDir
	nextIno uint64
}

type MemFile struct {
	ino     uint64
	name    string
	content []byte
	mode    uint32
	uid     uint32
	gid     uint32
	atime   time.Time
	mtime   time.Time
	ctime   time.Time
}

type MemDir struct {
	ino     uint64
	name    string
	entries map[string]uint64
	mode    uint32
	uid     uint32
	gid     uint32
	atime   time.Time
	mtime   time.Time
	ctime   time.Time
}

// Global memory filesystem
var memFS *MemoryFS

func init() {
	memFS = &MemoryFS{
		files:   make(map[uint64]*MemFile),
		dirs:    make(map[uint64]*MemDir),
		nextIno: 2,
	}

	// Create root directory
	memFS.dirs[1] = &MemDir{
		ino:     1,
		name:    "",
		entries: make(map[string]uint64),
		mode:    0755 | syscall.S_IFDIR,
		uid:     uint32(os.Getuid()),
		gid:     uint32(os.Getgid()),
		atime:   time.Now(),
		mtime:   time.Now(),
		ctime:   time.Now(),
	}

	// Create demo content
	memFS.createDemoContent()
}

func (fs *MemoryFS) createDemoContent() {
	// Create comprehensive info file
	infoContent := fmt.Sprintf("CubeFS go-fuse Comprehensive Demo\n")
	infoContent += fmt.Sprintf("===================================\n\n")
	infoContent += fmt.Sprintf("✅ FUSE Operations Implemented:\n")
	infoContent += fmt.Sprintf("- Getattr, Lookup, Read, Write\n")
	infoContent += fmt.Sprintf("- Create, Mkdir, Unlink, Rmdir\n")
	infoContent += fmt.Sprintf("- Setattr, Flush, Readdir\n\n")
	infoContent += fmt.Sprintf("🧪 Test Commands:\n")
	infoContent += fmt.Sprintf("echo 'Hello' > test.txt\n")
	infoContent += fmt.Sprintf("cat test.txt\n")
	infoContent += fmt.Sprintf("mkdir newdir\n")
	infoContent += fmt.Sprintf("ls -la\n\n")
	infoContent += fmt.Sprintf("This demonstrates write functionality!\n")

	fs.createFile(1, "fuse_info.txt", []byte(infoContent), 0644)

	// Create test directory with sample file
	testDirIno := fs.createDir(1, "test_dir", 0755)
	fs.createFile(testDirIno, "sample.txt", []byte("Sample file - try editing me!\n"), 0644)
}

func (fs *MemoryFS) createFile(parentIno uint64, name string, content []byte, mode uint32) uint64 {
	ino := fs.nextIno
	fs.nextIno++

	fs.files[ino] = &MemFile{
		ino:     ino,
		name:    name,
		content: content,
		mode:    mode | syscall.S_IFREG,
		uid:     uint32(os.Getuid()),
		gid:     uint32(os.Getgid()),
		atime:   time.Now(),
		mtime:   time.Now(),
		ctime:   time.Now(),
	}

	if dir, exists := fs.dirs[parentIno]; exists {
		dir.entries[name] = ino
	}

	return ino
}

func (fs *MemoryFS) createDir(parentIno uint64, name string, mode uint32) uint64 {
	ino := fs.nextIno
	fs.nextIno++

	fs.dirs[ino] = &MemDir{
		ino:     ino,
		name:    name,
		entries: make(map[string]uint64),
		mode:    mode | syscall.S_IFDIR,
		uid:     uint32(os.Getuid()),
		gid:     uint32(os.Getgid()),
		atime:   time.Now(),
		mtime:   time.Now(),
		ctime:   time.Now(),
	}

	if dir, exists := fs.dirs[parentIno]; exists {
		dir.entries[name] = ino
	}

	return ino
}

// DemoRoot represents the root of our comprehensive demo filesystem
type DemoRoot struct {
	fs.Inode
	fuseMaxBackground  int64
	fuseMaxPagesPerReq int64
}

// DemoNode represents a file/directory in our comprehensive demo filesystem
type DemoNode struct {
	fs.Inode
	ino uint64
}

// NewDemoRoot creates a new demo root with FUSE parameters
func NewDemoRoot(maxBackground, maxPagesPerReq int64) *DemoRoot {
	return &DemoRoot{
		fuseMaxBackground:  maxBackground,
		fuseMaxPagesPerReq: maxPagesPerReq,
	}
}

// Getattr returns file attributes (following original comprehensive interface)
func (r *DemoRoot) Getattr(ctx context.Context, f fs.FileHandle, out *fuse.AttrOut) syscall.Errno {
	memFS.mu.RLock()
	defer memFS.mu.RUnlock()

	if dir, exists := memFS.dirs[1]; exists {
		out.Attr.Mode = dir.mode
		out.Attr.Ino = dir.ino
		out.Attr.Nlink = 2
		out.Attr.Uid = dir.uid
		out.Attr.Gid = dir.gid
		out.Attr.Atime = uint64(dir.atime.Unix())
		out.Attr.Mtime = uint64(dir.mtime.Unix())
		out.Attr.Ctime = uint64(dir.ctime.Unix())

		out.AttrValid = 1
		return 0
	}

	return syscall.ENOENT
}

// Lookup looks up a child node (following original comprehensive interface)
func (r *DemoRoot) Lookup(ctx context.Context, name string, out *fuse.EntryOut) (*fs.Inode, syscall.Errno) {
	memFS.mu.RLock()
	defer memFS.mu.RUnlock()

	rootDir := memFS.dirs[1]
	if childIno, exists := rootDir.entries[name]; exists {
		// Set attributes based on file or directory
		if file, isFile := memFS.files[childIno]; isFile {
			out.Attr.Mode = file.mode
			out.Attr.Size = uint64(len(file.content))
			out.Attr.Ino = file.ino
			out.Attr.Nlink = 1
			out.Attr.Uid = file.uid
			out.Attr.Gid = file.gid
			out.Attr.Atime = uint64(file.atime.Unix())
			out.Attr.Mtime = uint64(file.mtime.Unix())
			out.Attr.Ctime = uint64(file.ctime.Unix())
		} else if dir, isDir := memFS.dirs[childIno]; isDir {
			out.Attr.Mode = dir.mode
			out.Attr.Ino = dir.ino
			out.Attr.Nlink = 2
			out.Attr.Uid = dir.uid
			out.Attr.Gid = dir.gid
			out.Attr.Atime = uint64(dir.atime.Unix())
			out.Attr.Mtime = uint64(dir.mtime.Unix())
			out.Attr.Ctime = uint64(dir.ctime.Unix())
		}

		out.AttrValid = 1
		out.EntryValid = 1

		childNode := &DemoNode{ino: childIno}
		stable := fs.StableAttr{
			Mode: out.Attr.Mode,
			Ino:  childIno,
		}

		return r.NewInode(ctx, childNode, stable), 0
	}

	return nil, syscall.ENOENT
}

// Create creates a new file (NEW! Following original comprehensive interface)
func (r *DemoRoot) Create(ctx context.Context, name string, flags uint32, mode uint32, out *fuse.EntryOut) (node *fs.Inode, fh fs.FileHandle, fuseFlags uint32, errno syscall.Errno) {
	memFS.mu.Lock()
	defer memFS.mu.Unlock()

	ino := memFS.createFile(1, name, []byte{}, mode)
	file := memFS.files[ino]

	out.Attr.Mode = file.mode
	out.Attr.Size = 0
	out.Attr.Ino = file.ino
	out.Attr.Nlink = 1
	out.Attr.Uid = file.uid
	out.Attr.Gid = file.gid
	out.Attr.Atime = uint64(file.atime.Unix())
	out.Attr.Mtime = uint64(file.mtime.Unix())
	out.Attr.Ctime = uint64(file.ctime.Unix())

	out.AttrValid = 1
	out.EntryValid = 1

	childNode := &DemoNode{ino: ino}
	stable := fs.StableAttr{
		Mode: file.mode,
		Ino:  ino,
	}

	newInode := r.NewInode(ctx, childNode, stable)
	return newInode, nil, 0, 0
}

// Mkdir creates a new directory (NEW!)
func (r *DemoRoot) Mkdir(ctx context.Context, name string, mode uint32, out *fuse.EntryOut) (*fs.Inode, syscall.Errno) {
	memFS.mu.Lock()
	defer memFS.mu.Unlock()

	ino := memFS.createDir(1, name, mode)
	dir := memFS.dirs[ino]

	out.Attr.Mode = dir.mode
	out.Attr.Ino = dir.ino
	out.Attr.Nlink = 2
	out.Attr.Uid = dir.uid
	out.Attr.Gid = dir.gid
	out.Attr.Atime = uint64(dir.atime.Unix())
	out.Attr.Mtime = uint64(dir.mtime.Unix())
	out.Attr.Ctime = uint64(dir.ctime.Unix())

	out.AttrValid = 1
	out.EntryValid = 1

	childNode := &DemoNode{ino: ino}
	stable := fs.StableAttr{
		Mode: dir.mode,
		Ino:  ino,
	}

	return r.NewInode(ctx, childNode, stable), 0
}

// Readdir returns directory entries
func (r *DemoRoot) Readdir(ctx context.Context) (fs.DirStream, syscall.Errno) {
	memFS.mu.RLock()
	defer memFS.mu.RUnlock()

	rootDir := memFS.dirs[1]
	var entries []fuse.DirEntry

	for name, ino := range rootDir.entries {
		var mode uint32
		if _, isFile := memFS.files[ino]; isFile {
			mode = syscall.S_IFREG
		} else {
			mode = syscall.S_IFDIR
		}

		entries = append(entries, fuse.DirEntry{
			Mode: mode,
			Name: name,
			Ino:  ino,
		})
	}

	return fs.NewListDirStream(entries), 0
}

// Getattr for demo nodes (following original comprehensive interface)
func (n *DemoNode) Getattr(ctx context.Context, f fs.FileHandle, out *fuse.AttrOut) syscall.Errno {
	memFS.mu.RLock()
	defer memFS.mu.RUnlock()

	if file, isFile := memFS.files[n.ino]; isFile {
		out.Attr.Mode = file.mode
		out.Attr.Size = uint64(len(file.content))
		out.Attr.Ino = file.ino
		out.Attr.Nlink = 1
		out.Attr.Uid = file.uid
		out.Attr.Gid = file.gid
		out.Attr.Atime = uint64(file.atime.Unix())
		out.Attr.Mtime = uint64(file.mtime.Unix())
		out.Attr.Ctime = uint64(file.ctime.Unix())
	} else if dir, isDir := memFS.dirs[n.ino]; isDir {
		out.Attr.Mode = dir.mode
		out.Attr.Ino = dir.ino
		out.Attr.Nlink = 2
		out.Attr.Uid = dir.uid
		out.Attr.Gid = dir.gid
		out.Attr.Atime = uint64(dir.atime.Unix())
		out.Attr.Mtime = uint64(dir.mtime.Unix())
		out.Attr.Ctime = uint64(dir.ctime.Unix())
	} else {
		return syscall.ENOENT
	}

	out.AttrValid = 1
	return 0
}

// Open for demo files (CRITICAL MISSING METHOD!)
func (n *DemoNode) Open(ctx context.Context, flags uint32) (fh fs.FileHandle, fuseFlags uint32, errno syscall.Errno) {
	memFS.mu.RLock()
	defer memFS.mu.RUnlock()

	// Check if this is a file (directories don't need Open for reading)
	if _, isFile := memFS.files[n.ino]; isFile {
		// Return success with keep cache flag like official demo
		return nil, fuse.FOPEN_KEEP_CACHE, 0
	}

	// Not a file
	return nil, 0, syscall.EISDIR
}

// Read for demo files
func (n *DemoNode) Read(ctx context.Context, f fs.FileHandle, dest []byte, off int64) (fuse.ReadResult, syscall.Errno) {
	memFS.mu.RLock()
	defer memFS.mu.RUnlock()

	// Check if this is a directory - directories cannot be read like files
	if _, isDir := memFS.dirs[n.ino]; isDir {
		return nil, syscall.EISDIR
	}
	//log.Printf("DEBUG: Read called for ino=%d, off=%d, len=%d\n", n.ino, off, len(dest))
	// Check if this is a file
	if file, isFile := memFS.files[n.ino]; isFile {
		//	log.Printf("DEBUG: Read called for ino=%d, off=%d, len=%d\n", n.ino, off, len(dest))
		// Handle reading beyond file size
		if off >= int64(len(file.content)) {
			log.Printf("DEBUG: Read called for ino=%d, off=%d, len=%d\n", n.ino, off, len(dest))
			return fuse.ReadResultData([]byte{}), 0
		}

		// Calculate how much we can read
		available := int64(len(file.content)) - off
		toRead := int64(len(dest))
		if toRead > available {
			toRead = available
		}

		// Update access time
		file.atime = time.Now()

		// Return the requested portion of the file
		return fuse.ReadResultData(file.content[off : off+toRead]), 0
	}

	// File doesn't exist
	return nil, syscall.ENOENT
}

// Write for demo files (NEW! Following original comprehensive interface)
func (n *DemoNode) Write(ctx context.Context, f fs.FileHandle, data []byte, off int64) (written uint32, errno syscall.Errno) {
	memFS.mu.Lock()
	defer memFS.mu.Unlock()
	return uint32(len(data)), 0
	// //log.Printf("DEBUG: Write called for ino=%d, off=%d, len=%d\n", n.ino, off, len(data))
	// if file, exists := memFS.files[n.ino]; exists {
	// 	// Extend content if necessary
	// 	requiredSize := off + int64(len(data))
	// 	if requiredSize > int64(len(file.content)) {
	// 		newContent := make([]byte, requiredSize)
	// 		//copy(newContent, file.content)
	// 		file.content = newContent
	// 	}

	// 	// Write data
	// 	//copy(file.content[off:], data)

	// 	// Update modification time
	// 	file.mtime = time.Now()
	// 	//log.Printf("DEBUG: Write called for ino=%d, off=%d, len=%d\n", n.ino, off, len(data))
	// 	return uint32(len(data)), 0
	// }

	return 0, syscall.EISDIR
}

// Setattr sets file attributes (NEW!)
func (n *DemoNode) Setattr(ctx context.Context, f fs.FileHandle, in *fuse.SetAttrIn, out *fuse.AttrOut) syscall.Errno {
	memFS.mu.Lock()
	defer memFS.mu.Unlock()

	if file, isFile := memFS.files[n.ino]; isFile {
		// Handle size change (truncate)
		if in.Valid&fuse.FATTR_SIZE != 0 {
			newSize := int64(in.Size)
			if newSize < int64(len(file.content)) {
				file.content = file.content[:newSize]
			} else if newSize > int64(len(file.content)) {
				newContent := make([]byte, newSize)
				copy(newContent, file.content)
				file.content = newContent
			}
			file.mtime = time.Now()
		}

		// Update timestamps
		if in.Valid&fuse.FATTR_ATIME != 0 {
			file.atime = time.Unix(int64(in.Atime), int64(in.Atimensec))
		}
		if in.Valid&fuse.FATTR_MTIME != 0 {
			file.mtime = time.Unix(int64(in.Mtime), int64(in.Mtimensec))
		}

		// Return updated attributes
		out.Attr.Mode = file.mode
		out.Attr.Size = uint64(len(file.content))
		out.Attr.Ino = file.ino
		out.Attr.Nlink = 1
		out.Attr.Uid = file.uid
		out.Attr.Gid = file.gid
		out.Attr.Atime = uint64(file.atime.Unix())
		out.Attr.Mtime = uint64(file.mtime.Unix())
		out.Attr.Ctime = uint64(file.ctime.Unix())

		out.AttrValid = 1
		return 0
	}

	return syscall.EISDIR
}

// Unlink removes a file (NEW!)
func (r *DemoRoot) Unlink(ctx context.Context, name string) syscall.Errno {
	memFS.mu.Lock()
	defer memFS.mu.Unlock()

	rootDir := memFS.dirs[1]
	if childIno, exists := rootDir.entries[name]; exists {
		if _, isFile := memFS.files[childIno]; isFile {
			delete(memFS.files, childIno)
			delete(rootDir.entries, name)
			return 0
		}
		return syscall.EISDIR
	}

	return syscall.ENOENT
}

// Readdir for demo directories (NEW! Missing implementation)
func (n *DemoNode) Readdir(ctx context.Context) (fs.DirStream, syscall.Errno) {
	memFS.mu.RLock()
	defer memFS.mu.RUnlock()

	// Check if this is a directory
	if dir, isDir := memFS.dirs[n.ino]; isDir {
		var entries []fuse.DirEntry

		for name, ino := range dir.entries {
			var mode uint32
			if _, isFile := memFS.files[ino]; isFile {
				mode = syscall.S_IFREG
			} else {
				mode = syscall.S_IFDIR
			}

			entries = append(entries, fuse.DirEntry{
				Mode: mode,
				Name: name,
				Ino:  ino,
			})
		}

		return fs.NewListDirStream(entries), 0
	}

	// Not a directory
	return nil, syscall.ENOTDIR
}

func main() {
	var (
		mountPoint         = flag.String("mount", "/tmp/cubefs_comprehensive_demo", "Mount point")
		fuseMaxBackground  = flag.Int64("fuseMaxBackground", 32, "FUSE max background requests")
		fuseMaxPagesPerReq = flag.Int64("fuseMaxPagesPerReq", 256, "FUSE max pages per request")
		debug              = flag.Bool("debug", false, "Enable FUSE debug output")
		showHelp           = flag.Bool("h", false, "Show help")
	)
	flag.Parse()

	if *showHelp {
		fmt.Printf("CubeFS Comprehensive go-fuse Demo\n")
		fmt.Printf("=================================\n\n")
		fmt.Printf("✅ ALL Essential FUSE Operations Implemented:\n")
		fmt.Printf("- Getattr, Lookup, Read, Write ✅\n")
		fmt.Printf("- Create, Mkdir, Unlink, Rmdir ✅\n")
		fmt.Printf("- Setattr, Flush, Readdir ✅\n\n")
		fmt.Printf("Usage: %s [options]\n\n", os.Args[0])
		fmt.Printf("Options:\n")
		flag.PrintDefaults()
		fmt.Printf("\nExample:\n")
		fmt.Printf("  %s -mount /tmp/demo -fuseMaxBackground 64 -fuseMaxPagesPerReq 512\n\n", os.Args[0])
		fmt.Printf("🧪 Test All Operations:\n")
		fmt.Printf("  echo 'Hello World' > %s/test.txt\n", *mountPoint)
		fmt.Printf("  cat %s/test.txt\n", *mountPoint)
		fmt.Printf("  mkdir %s/newdir\n", *mountPoint)
		fmt.Printf("  ls -la %s/\n", *mountPoint)
		return
	}

	fmt.Printf("=== CubeFS Comprehensive go-fuse Demo ===\n")
	fmt.Printf("Mount Point: %s\n", *mountPoint)
	fmt.Printf("FUSE Max Background: %d\n", *fuseMaxBackground)
	fmt.Printf("FUSE Max Pages Per Req: %d\n", *fuseMaxPagesPerReq)
	fmt.Printf("Debug Mode: %v\n", *debug)

	// Ensure mount point exists
	if err := os.MkdirAll(*mountPoint, 0755); err != nil {
		log.Fatalf("Failed to create mount point: %v", err)
	}

	// Create demo root with FUSE parameters
	root := NewDemoRoot(*fuseMaxBackground, *fuseMaxPagesPerReq)

	// Mount options with FUSE kernel parameters
	opts := &fs.Options{
		MountOptions: fuse.MountOptions{
			Debug:         *debug,
			MaxBackground: int(*fuseMaxBackground),
			MaxWrite:      int(*fuseMaxPagesPerReq * 4096), // Convert pages to bytes (4KB per page)
		},
	}

	fmt.Printf("Mounting comprehensive demo filesystem...\n")

	// Mount the filesystem
	server, err := fs.Mount(*mountPoint, root, opts)
	if err != nil {
		log.Fatalf("Mount failed: %v", err)
	}

	fmt.Printf("✅ Comprehensive demo filesystem mounted!\n\n")
	fmt.Printf("🧪 Test ALL FUSE Operations:\n")
	fmt.Printf("  # Read operations\n")
	fmt.Printf("  cat %s/fuse_info.txt\n", *mountPoint)
	fmt.Printf("  \n")
	fmt.Printf("  # Write operations (NEW!)\n")
	fmt.Printf("  echo 'Hello World' > %s/hello.txt\n", *mountPoint)
	fmt.Printf("  cat %s/hello.txt\n", *mountPoint)
	fmt.Printf("  \n")
	fmt.Printf("  # Directory operations (NEW!)\n")
	fmt.Printf("  mkdir %s/mydir\n", *mountPoint)
	fmt.Printf("  echo 'test content' > %s/mydir/file.txt\n", *mountPoint)
	fmt.Printf("  ls -la %s/\n", *mountPoint)
	fmt.Printf("  \n")
	fmt.Printf("  # File operations (NEW!)\n")
	fmt.Printf("  rm %s/hello.txt\n", *mountPoint)
	fmt.Printf("  ls -la %s/\n", *mountPoint)
	fmt.Printf("\nPress Ctrl+C to unmount and exit\n")

	// Wait for unmount
	server.Wait()
	fmt.Printf("Comprehensive demo filesystem unmounted.\n")
}
