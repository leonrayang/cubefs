package stream

import (
	"sync"

	"github.com/cubefs/cubefs/util"
)

// ZeroCopyBuffer implements zero-copy buffer operations using slice views
type ZeroCopyBuffer struct {
	pool    *sync.Pool
	buffers map[int]*BufferPool
}

// BufferPool manages buffers of a specific size
type BufferPool struct {
	size    int
	pool    *sync.Pool
	buffers chan []byte
}

// NewZeroCopyBuffer creates a new zero-copy buffer manager
func NewZeroCopyBuffer() *ZeroCopyBuffer {
	zcb := &ZeroCopyBuffer{
		buffers: make(map[int]*BufferPool),
	}

	// Initialize pools for common sizes
	zcb.buffers[util.DefaultTinySizeLimit] = newBufferPool(util.DefaultTinySizeLimit, 100)
	zcb.buffers[util.BlockSize] = newBufferPool(util.BlockSize, 50)

	return zcb
}

func newBufferPool(size, capacity int) *BufferPool {
	return &BufferPool{
		size:    size,
		buffers: make(chan []byte, capacity),
		pool: &sync.Pool{
			New: func() interface{} {
				return make([]byte, size)
			},
		},
	}
}

// GetBuffer returns a buffer of the specified size
func (zcb *ZeroCopyBuffer) GetBuffer(size int) []byte {
	if pool, exists := zcb.buffers[size]; exists {
		select {
		case buf := <-pool.buffers:
			return buf
		default:
			return pool.pool.Get().([]byte)
		}
	}

	// Fallback to direct allocation
	return make([]byte, size)
}

// PutBuffer returns a buffer to the pool
func (zcb *ZeroCopyBuffer) PutBuffer(buf []byte) {
	size := len(buf)
	if pool, exists := zcb.buffers[size]; exists {
		// Reset buffer
		for i := range buf {
			buf[i] = 0
		}

		select {
		case pool.buffers <- buf:
		default:
			// Pool is full, let it be garbage collected
		}
	}
}

// SliceView represents a view into a larger buffer
type SliceView struct {
	buffer []byte
	start  int
	end    int
}

// NewSliceView creates a view into a buffer
func NewSliceView(buffer []byte, start, length int) *SliceView {
	return &SliceView{
		buffer: buffer,
		start:  start,
		end:    start + length,
	}
}

// Data returns the slice view data
func (sv *SliceView) Data() []byte {
	return sv.buffer[sv.start:sv.end]
}

// CopyTo copies data to the slice view without allocating new memory
func (sv *SliceView) CopyTo(data []byte) {
	copy(sv.buffer[sv.start:sv.end], data)
}

// Length returns the length of the slice view
func (sv *SliceView) Length() int {
	return sv.end - sv.start
}

// ExtentHandlerBuffer wraps the packet buffer with zero-copy operations
type ExtentHandlerBuffer struct {
	buffer   []byte
	writePos int
	zcb      *ZeroCopyBuffer
}

// NewExtentHandlerBuffer creates a new buffer for extent handler
func NewExtentHandlerBuffer(size int, zcb *ZeroCopyBuffer) *ExtentHandlerBuffer {
	return &ExtentHandlerBuffer{
		buffer:   zcb.GetBuffer(size),
		writePos: 0,
		zcb:      zcb,
	}
}

// WriteData writes data to the buffer using zero-copy when possible
func (ehb *ExtentHandlerBuffer) WriteData(data []byte) int {
	available := len(ehb.buffer) - ehb.writePos
	writeSize := len(data)
	if writeSize > available {
		writeSize = available
	}

	// Use slice view for zero-copy operation
	view := NewSliceView(ehb.buffer, ehb.writePos, writeSize)
	view.CopyTo(data[:writeSize])

	ehb.writePos += writeSize
	return writeSize
}

// GetData returns the current buffer data
func (ehb *ExtentHandlerBuffer) GetData() []byte {
	return ehb.buffer[:ehb.writePos]
}

// Reset resets the buffer for reuse
func (ehb *ExtentHandlerBuffer) Reset() {
	ehb.writePos = 0
}

// Release returns the buffer to the pool
func (ehb *ExtentHandlerBuffer) Release() {
	ehb.zcb.PutBuffer(ehb.buffer)
	ehb.buffer = nil
}
