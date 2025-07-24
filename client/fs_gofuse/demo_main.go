//go:build demo

package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"syscall"
	"time"

	"github.com/hanwen/go-fuse/v2/fs"
	"github.com/hanwen/go-fuse/v2/fuse"
)

// DemoRoot represents the root of our demo filesystem
type DemoRoot struct {
	fs.Inode
	fuseMaxBackground  int64
	fuseMaxPagesPerReq int64
}

// DemoNode represents a file/directory in our demo filesystem
type DemoNode struct {
	fs.Inode
	name    string
	content []byte
	isDir   bool
	modTime time.Time
}

// NewDemoRoot creates a new demo root with FUSE parameters
func NewDemoRoot(maxBackground, maxPagesPerReq int64) *DemoRoot {
	return &DemoRoot{
		fuseMaxBackground:  maxBackground,
		fuseMaxPagesPerReq: maxPagesPerReq,
	}
}

// Getattr returns file attributes for the root
func (r *DemoRoot) Getattr(ctx context.Context, f fs.FileHandle, out *fuse.AttrOut) syscall.Errno {
	out.Attr.Mode = 0755 | syscall.S_IFDIR
	out.Attr.Ino = 1
	out.Attr.Nlink = 2
	out.Attr.Uid = uint32(os.Getuid())
	out.Attr.Gid = uint32(os.Getgid())
	out.Attr.Mtime = uint64(time.Now().Unix())
	out.Attr.Atime = uint64(time.Now().Unix())
	out.Attr.Ctime = uint64(time.Now().Unix())

	out.AttrValid = 1
	return 0
}

// Lookup handles directory lookups
func (r *DemoRoot) Lookup(ctx context.Context, name string, out *fuse.EntryOut) (*fs.Inode, syscall.Errno) {
	switch name {
	case "fuse_info.txt":
		content := fmt.Sprintf("CubeFS go-fuse Demo\n==================\n\n")
		content += fmt.Sprintf("FUSE Kernel Parameters:\n")
		content += fmt.Sprintf("- fuseDefaultMaxBackground: %d\n", r.fuseMaxBackground)
		content += fmt.Sprintf("- fuseMaxPagesPerReq: %d\n", r.fuseMaxPagesPerReq)
		content += fmt.Sprintf("- Mount Time: %s\n", time.Now().Format(time.RFC3339))
		content += fmt.Sprintf("\nThis demo tests go-fuse performance and functionality.\n")
		content += fmt.Sprintf("You can adjust FUSE kernel parameters to test performance impact.\n")

		node := &DemoNode{
			name:    name,
			content: []byte(content),
			isDir:   false,
			modTime: time.Now(),
		}

		out.Attr.Mode = 0644 | syscall.S_IFREG
		out.Attr.Size = uint64(len(content))
		out.Attr.Ino = 2
		out.Attr.Nlink = 1
		out.Attr.Uid = uint32(os.Getuid())
		out.Attr.Gid = uint32(os.Getgid())
		out.Attr.Mtime = uint64(node.modTime.Unix())
		out.Attr.Atime = uint64(node.modTime.Unix())
		out.Attr.Ctime = uint64(node.modTime.Unix())

		out.AttrValid = 1
		out.EntryValid = 1

		stable := fs.StableAttr{
			Mode: 0644 | syscall.S_IFREG,
			Ino:  2,
		}

		return r.NewInode(ctx, node, stable), 0

	case "benchmark":
		node := &DemoNode{
			name:    name,
			isDir:   true,
			modTime: time.Now(),
		}

		out.Attr.Mode = 0755 | syscall.S_IFDIR
		out.Attr.Ino = 3
		out.Attr.Nlink = 2
		out.Attr.Uid = uint32(os.Getuid())
		out.Attr.Gid = uint32(os.Getgid())
		out.Attr.Mtime = uint64(node.modTime.Unix())
		out.Attr.Atime = uint64(node.modTime.Unix())
		out.Attr.Ctime = uint64(node.modTime.Unix())

		out.AttrValid = 1
		out.EntryValid = 1

		stable := fs.StableAttr{
			Mode: 0755 | syscall.S_IFDIR,
			Ino:  3,
		}

		return r.NewInode(ctx, node, stable), 0
	}

	return nil, syscall.ENOENT
}

// Readdir returns directory entries for the root
func (r *DemoRoot) Readdir(ctx context.Context) (fs.DirStream, syscall.Errno) {
	entries := []fuse.DirEntry{
		{Mode: syscall.S_IFREG, Name: "fuse_info.txt", Ino: 2},
		{Mode: syscall.S_IFDIR, Name: "benchmark", Ino: 3},
	}
	return fs.NewListDirStream(entries), 0
}

// Getattr for demo nodes
func (n *DemoNode) Getattr(ctx context.Context, f fs.FileHandle, out *fuse.AttrOut) syscall.Errno {
	if n.isDir {
		out.Attr.Mode = 0755 | syscall.S_IFDIR
		out.Attr.Nlink = 2
	} else {
		out.Attr.Mode = 0644 | syscall.S_IFREG
		out.Attr.Size = uint64(len(n.content))
		out.Attr.Nlink = 1
	}

	out.Attr.Uid = uint32(os.Getuid())
	out.Attr.Gid = uint32(os.Getgid())
	out.Attr.Mtime = uint64(n.modTime.Unix())
	out.Attr.Atime = uint64(n.modTime.Unix())
	out.Attr.Ctime = uint64(n.modTime.Unix())
	out.AttrValid = 1
	return 0
}

// Read for demo files
func (n *DemoNode) Read(ctx context.Context, f fs.FileHandle, dest []byte, off int64) (fuse.ReadResult, syscall.Errno) {
	if n.isDir {
		return nil, syscall.EISDIR
	}

	end := off + int64(len(dest))
	if end > int64(len(n.content)) {
		end = int64(len(n.content))
	}

	if off >= int64(len(n.content)) {
		return fuse.ReadResultData([]byte{}), 0
	}

	return fuse.ReadResultData(n.content[off:end]), 0
}

func main() {
	var (
		mountPoint         = flag.String("mount", "/tmp/cubefs_gofuse_demo", "Mount point")
		fuseMaxBackground  = flag.Int64("fuseMaxBackground", 32, "FUSE max background requests")
		fuseMaxPagesPerReq = flag.Int64("fuseMaxPagesPerReq", 256, "FUSE max pages per request")
		debug              = flag.Bool("debug", false, "Enable FUSE debug output")
		showHelp           = flag.Bool("h", false, "Show help")
	)
	flag.Parse()

	if *showHelp {
		fmt.Printf("CubeFS go-fuse Demo\n")
		fmt.Printf("===================\n\n")
		fmt.Printf("This is a standalone demo for testing go-fuse performance and functionality.\n")
		fmt.Printf("It creates an in-memory filesystem with configurable FUSE kernel parameters.\n\n")
		fmt.Printf("Usage: %s [options]\n\n", os.Args[0])
		fmt.Printf("Options:\n")
		flag.PrintDefaults()
		fmt.Printf("\nExample:\n")
		fmt.Printf("  %s -mount /tmp/demo -fuseMaxBackground 64 -fuseMaxPagesPerReq 512 -debug\n\n", os.Args[0])
		fmt.Printf("After mounting, try:\n")
		fmt.Printf("  cat %s/fuse_info.txt\n", *mountPoint)
		fmt.Printf("  ls -la %s/\n", *mountPoint)
		return
	}

	fmt.Printf("=== CubeFS go-fuse Demo ===\n")
	fmt.Printf("Mount Point: %s\n", *mountPoint)
	fmt.Printf("FUSE Max Background: %d\n", *fuseMaxBackground)
	fmt.Printf("FUSE Max Pages Per Req: %d\n", *fuseMaxPagesPerReq)
	fmt.Printf("Debug Mode: %v\n", *debug)
	fmt.Printf("\nCreating mount point...\n")

	// Ensure mount point exists
	if err := os.MkdirAll(*mountPoint, 0755); err != nil {
		log.Fatalf("Failed to create mount point: %v", err)
	}

	// Create demo root with FUSE parameters
	root := NewDemoRoot(*fuseMaxBackground, *fuseMaxPagesPerReq)

	// Mount options
	opts := &fs.Options{
		MountOptions: fuse.MountOptions{
			Debug: *debug,
		},
	}

	fmt.Printf("Mounting demo filesystem...\n")

	// Mount the filesystem
	server, err := fs.Mount(*mountPoint, root, opts)
	if err != nil {
		log.Fatalf("Mount failed: %v", err)
	}

	fmt.Printf("✅ Demo filesystem mounted successfully!\n\n")
	fmt.Printf("Test commands:\n")
	fmt.Printf("  cat %s/fuse_info.txt\n", *mountPoint)
	fmt.Printf("  ls -la %s/\n", *mountPoint)
	fmt.Printf("  ls %s/benchmark/\n", *mountPoint)
	fmt.Printf("\nPress Ctrl+C to unmount and exit\n")

	// Wait for unmount
	server.Wait()
	fmt.Printf("Demo filesystem unmounted.\n")
}
