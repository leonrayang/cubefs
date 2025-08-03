package stream

import (
	"os"
	"syscall"
)

// MMapBuffer provides memory-mapped buffer for high-performance operations
type MMapBuffer struct {
	data     []byte
	file     *os.File
	size     int
	offset   int64
	isMapped bool
}

// NewMMapBuffer creates a new memory-mapped buffer
func NewMMapBuffer(size int) (*MMapBuffer, error) {
	// Create temporary file for memory mapping
	tmpFile, err := os.CreateTemp("", "cubefs_mmap_*")
	if err != nil {
		return nil, err
	}

	// Pre-allocate file size
	if err := tmpFile.Truncate(int64(size)); err != nil {
		tmpFile.Close()
		os.Remove(tmpFile.Name())
		return nil, err
	}

	// Memory map the file
	data, err := syscall.Mmap(int(tmpFile.Fd()), 0, size, syscall.PROT_READ|syscall.PROT_WRITE, syscall.MAP_SHARED)
	if err != nil {
		tmpFile.Close()
		os.Remove(tmpFile.Name())
		return nil, err
	}

	return &MMapBuffer{
		data:     data,
		file:     tmpFile,
		size:     size,
		offset:   0,
		isMapped: true,
	}, nil
}

// WriteAt writes data to the memory-mapped buffer
func (mmb *MMapBuffer) WriteAt(offset int, data []byte) {
	if offset+len(data) <= len(mmb.data) {
		copy(mmb.data[offset:offset+len(data)], data)
	}
}

// ReadAt reads data from the memory-mapped buffer
func (mmb *MMapBuffer) ReadAt(offset int, length int) []byte {
	if offset+length <= len(mmb.data) {
		return mmb.data[offset : offset+length]
	}
	return nil
}

// GetData returns the entire buffer data
func (mmb *MMapBuffer) GetData() []byte {
	return mmb.data
}

// GetSlice returns a slice of the buffer
func (mmb *MMapBuffer) GetSlice(offset, length int) []byte {
	if offset+length <= len(mmb.data) {
		return mmb.data[offset : offset+length]
	}
	return nil
}

// Sync flushes the memory-mapped buffer to disk
func (mmb *MMapBuffer) Sync() error {
	if mmb.isMapped {
		// Use platform-specific sync
		return mmb.file.Sync()
	}
	return nil
}

// Close unmaps and closes the buffer
func (mmb *MMapBuffer) Close() error {
	if mmb.isMapped {
		syscall.Munmap(mmb.data)
		mmb.isMapped = false
	}
	if mmb.file != nil {
		name := mmb.file.Name()
		mmb.file.Close()
		os.Remove(name)
	}
	return nil
}

// Size returns the buffer size
func (mmb *MMapBuffer) Size() int {
	return mmb.size
}
