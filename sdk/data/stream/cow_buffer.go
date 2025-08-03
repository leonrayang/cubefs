package stream

import (
	"sync/atomic"
)

// COWBuffer implements copy-on-write semantics for byte slices
type COWBuffer struct {
	data        []byte
	refCount    int32
	isShared    bool
	writeOffset int
}

// NewCOWBuffer creates a new copy-on-write buffer
func NewCOWBuffer(size int) *COWBuffer {
	return &COWBuffer{
		data:        make([]byte, size),
		refCount:    1,
		isShared:    false,
		writeOffset: 0,
	}
}

// NewCOWBufferFromSlice creates a COW buffer from an existing slice
func NewCOWBufferFromSlice(data []byte) *COWBuffer {
	return &COWBuffer{
		data:        data,
		refCount:    1,
		isShared:    false,
		writeOffset: 0,
	}
}

// Copy creates a shallow copy that shares the underlying data
func (cb *COWBuffer) Copy() *COWBuffer {
	atomic.AddInt32(&cb.refCount, 1)
	return &COWBuffer{
		data:        cb.data,
		refCount:    cb.refCount,
		isShared:    true,
		writeOffset: cb.writeOffset,
	}
}

// WriteAt copies data only when necessary (copy-on-write)
func (cb *COWBuffer) WriteAt(offset int, data []byte) {
	if cb.isShared {
		// Create a new copy if this buffer is shared
		cb.data = make([]byte, len(cb.data))
		copy(cb.data, cb.data) // Copy existing data
		cb.isShared = false
	}

	// Now safe to write directly
	copy(cb.data[offset:offset+len(data)], data)
}

// GetData returns the underlying slice - use with caution
func (cb *COWBuffer) GetData() []byte {
	return cb.data
}

// GetSlice returns a slice from offset to offset+length
func (cb *COWBuffer) GetSlice(offset, length int) []byte {
	return cb.data[offset : offset+length]
}

// Release decrements the reference count and frees memory if count reaches 0
func (cb *COWBuffer) Release() {
	if atomic.AddInt32(&cb.refCount, -1) == 0 {
		cb.data = nil
	}
}

// Size returns the size of the buffer
func (cb *COWBuffer) Size() int {
	return len(cb.data)
}

// IsShared returns whether this buffer is shared with other references
func (cb *COWBuffer) IsShared() bool {
	return cb.isShared
}
