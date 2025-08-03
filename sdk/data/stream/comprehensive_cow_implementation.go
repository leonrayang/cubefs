package stream

import (
	"sync"
	"sync/atomic"

	"github.com/cubefs/cubefs/proto"
	"github.com/cubefs/cubefs/util"
	"github.com/cubefs/cubefs/util/log"
)

// ComprehensiveCOWBuffer implements copy-on-write for the entire data flow
type ComprehensiveCOWBuffer struct {
	data        []byte
	refCount    int32
	isShared    bool
	writeOffset int
	readOffset  int
	mu          sync.RWMutex
	origin      string   // Track where this buffer originated
	callPath    []string // Track the call path
}

// NewComprehensiveCOWBuffer creates a new comprehensive COW buffer
func NewComprehensiveCOWBuffer(size int, origin string) *ComprehensiveCOWBuffer {
	return &ComprehensiveCOWBuffer{
		data:        make([]byte, size),
		refCount:    1,
		isShared:    false,
		writeOffset: 0,
		readOffset:  0,
		origin:      origin,
		callPath:    []string{origin},
	}
}

// NewComprehensiveCOWBufferFromSlice creates a COW buffer from an existing slice
func NewComprehensiveCOWBufferFromSlice(data []byte, origin string) *ComprehensiveCOWBuffer {
	return &ComprehensiveCOWBuffer{
		data:        data,
		refCount:    1,
		isShared:    false,
		writeOffset: 0,
		readOffset:  0,
		origin:      origin,
		callPath:    []string{origin},
	}
}

// Copy creates a shallow copy that shares the underlying data
func (cb *ComprehensiveCOWBuffer) Copy(origin string) *ComprehensiveCOWBuffer {
	cb.mu.Lock()
	defer cb.mu.Unlock()

	atomic.AddInt32(&cb.refCount, 1)
	newCallPath := make([]string, len(cb.callPath)+1)
	copy(newCallPath, cb.callPath)
	newCallPath[len(cb.callPath)] = origin

	return &ComprehensiveCOWBuffer{
		data:        cb.data,
		refCount:    cb.refCount,
		isShared:    true,
		writeOffset: cb.writeOffset,
		readOffset:  cb.readOffset,
		origin:      origin,
		callPath:    newCallPath,
	}
}

// WriteAt copies data only when necessary (copy-on-write)
func (cb *ComprehensiveCOWBuffer) WriteAt(offset int, data []byte, origin string) {
	cb.mu.Lock()
	defer cb.mu.Unlock()

	if cb.isShared {
		// Create a new copy if this buffer is shared
		newData := make([]byte, len(cb.data))
		copy(newData, cb.data)
		cb.data = newData
		cb.isShared = false
		cb.callPath = append(cb.callPath, origin+"_COW_COPY")
	}

	// Now safe to write directly
	copy(cb.data[offset:offset+len(data)], data)
	cb.callPath = append(cb.callPath, origin+"_WRITE")
}

// ReadAt reads data from the buffer
func (cb *ComprehensiveCOWBuffer) ReadAt(offset int, length int, origin string) []byte {
	cb.mu.RLock()
	defer cb.mu.RUnlock()

	cb.callPath = append(cb.callPath, origin+"_READ")
	return cb.data[offset : offset+length]
}

// GetData returns the underlying slice - use with caution
func (cb *ComprehensiveCOWBuffer) GetData() []byte {
	cb.mu.RLock()
	defer cb.mu.RUnlock()
	return cb.data
}

// GetSlice returns a slice from offset to offset+length
func (cb *ComprehensiveCOWBuffer) GetSlice(offset, length int) []byte {
	cb.mu.RLock()
	defer cb.mu.RUnlock()
	return cb.data[offset : offset+length]
}

// Release decrements the reference count and frees memory if count reaches 0
func (cb *ComprehensiveCOWBuffer) Release() {
	if atomic.AddInt32(&cb.refCount, -1) == 0 {
		cb.mu.Lock()
		cb.data = nil
		cb.mu.Unlock()
	}
}

// Size returns the size of the buffer
func (cb *ComprehensiveCOWBuffer) Size() int {
	cb.mu.RLock()
	defer cb.mu.RUnlock()
	return len(cb.data)
}

// IsShared returns whether this buffer is shared with other references
func (cb *ComprehensiveCOWBuffer) IsShared() bool {
	cb.mu.RLock()
	defer cb.mu.RUnlock()
	return cb.isShared
}

// GetCallPath returns the call path for debugging
func (cb *ComprehensiveCOWBuffer) GetCallPath() []string {
	cb.mu.RLock()
	defer cb.mu.RUnlock()
	return append([]string{}, cb.callPath...)
}

// GetOrigin returns the origin of this buffer
func (cb *ComprehensiveCOWBuffer) GetOrigin() string {
	cb.mu.RLock()
	defer cb.mu.RUnlock()
	return cb.origin
}

// COWDataFlow tracks the entire data flow from allocation to usage
type COWDataFlow struct {
	buffers map[string]*ComprehensiveCOWBuffer
	mu      sync.RWMutex
}

// NewCOWDataFlow creates a new COW data flow tracker
func NewCOWDataFlow() *COWDataFlow {
	return &COWDataFlow{
		buffers: make(map[string]*ComprehensiveCOWBuffer),
	}
}

// AllocateBuffer allocates a new buffer with COW tracking
func (df *COWDataFlow) AllocateBuffer(size int, origin string) *ComprehensiveCOWBuffer {
	df.mu.Lock()
	defer df.mu.Unlock()

	buffer := NewComprehensiveCOWBuffer(size, origin)
	df.buffers[origin] = buffer
	return buffer
}

// GetBuffer retrieves a buffer by origin
func (df *COWDataFlow) GetBuffer(origin string) *ComprehensiveCOWBuffer {
	df.mu.RLock()
	defer df.mu.RUnlock()
	return df.buffers[origin]
}

// ReleaseBuffer releases a buffer
func (df *COWDataFlow) ReleaseBuffer(origin string) {
	df.mu.Lock()
	defer df.mu.Unlock()

	if buffer, exists := df.buffers[origin]; exists {
		buffer.Release()
		delete(df.buffers, origin)
	}
}

// COWExtentHandler integrates comprehensive COW with ExtentHandler
type COWExtentHandler struct {
	*ExtentHandler
	dataFlow   *COWDataFlow
	cowBuffers map[string]*ComprehensiveCOWBuffer
}

// NewCOWExtentHandler creates a new COW extent handler
func NewCOWExtentHandler(stream *Streamer, offset int, storeMode int, size int,
	storageClass uint32, isMigration bool) *COWExtentHandler {

	eh := NewExtentHandler(stream, offset, storeMode, size, storageClass, isMigration)

	cohe := &COWExtentHandler{
		ExtentHandler: eh,
		dataFlow:      NewCOWDataFlow(),
		cowBuffers:    make(map[string]*ComprehensiveCOWBuffer),
	}

	return cohe
}

// writeWithCOW implements comprehensive copy-on-write for the entire data flow
func (cohe *COWExtentHandler) writeWithCOW(data []byte, offset, size int, direct bool) (ek *proto.ExtentKey, err error) {
	// Step 1: Create COW buffer for incoming data
	dataOrigin := "client_write_data"
	dataBuffer := cohe.dataFlow.AllocateBuffer(len(data), dataOrigin)
	copy(dataBuffer.GetData(), data)
	cohe.cowBuffers[dataOrigin] = dataBuffer

	var total, write int
	blksize := util.BlockSize
	if cohe.storeMode == proto.TinyExtentType {
		blksize = util.DefaultTinySizeLimit
	}

	for total < size {
		if cohe.packet == nil {
			cohe.packet = NewWritePacket(cohe.inode, offset+total, cohe.storeMode)
			log.LogDebugf("COWExtentHandler write packet nil and new packet: eh(%v)", cohe)
			if direct {
				cohe.packet.Opcode = proto.OpSyncWrite
			}
		}

		packsize := int(cohe.packet.Size)
		write = util.Min(size-total, blksize-packsize)

		if write > 0 {
			// Step 2: Use comprehensive COW for data transfer
			cohe.writeDataWithComprehensiveCOW(packsize, dataBuffer.GetSlice(total, write), "extent_handler_write")
			cohe.packet.Size += uint32(write)
			total += write
		}

		if int(cohe.packet.Size) >= blksize {
			cohe.flushPacketWithCOW()
		}
	}

	cohe.size += total

	ek = &proto.ExtentKey{
		FileOffset: uint64(cohe.fileOffset),
		Size:       uint32(cohe.size),
	}
	return ek, nil
}

// writeDataWithComprehensiveCOW uses comprehensive copy-on-write
func (cohe *COWExtentHandler) writeDataWithComprehensiveCOW(offset int, data []byte, origin string) {
	if len(data) == 0 {
		return
	}

	// Create packet buffer if needed
	packetOrigin := "packet_data_buffer"
	packetBuffer, exists := cohe.cowBuffers[packetOrigin]
	if !exists {
		packetBuffer = cohe.dataFlow.AllocateBuffer(len(cohe.packet.Data), packetOrigin)
		copy(packetBuffer.GetData(), cohe.packet.Data)
		cohe.cowBuffers[packetOrigin] = packetBuffer
	}

	// Use comprehensive COW for data transfer
	if len(data) > 64 || packetBuffer.IsShared() {
		packetBuffer.WriteAt(offset, data, origin)
		cohe.packet.Data = packetBuffer.GetData()
	} else {
		// Direct copy for small, unshared data
		copy(cohe.packet.Data[offset:offset+len(data)], data)
	}
}

// flushPacketWithCOW optimizes packet flushing with COW
func (cohe *COWExtentHandler) flushPacketWithCOW() {
	if cohe.packet == nil {
		return
	}

	// Use COW buffer for packet data
	packetOrigin := "packet_data_buffer"
	if packetBuffer, exists := cohe.cowBuffers[packetOrigin]; exists {
		cohe.packet.Data = packetBuffer.GetData()
	}

	cohe.pushToRequest(cohe.packet)
	cohe.packet = nil
}

// cleanupWithCOW properly releases COW resources
func (cohe *COWExtentHandler) cleanupWithCOW() error {
	// Release all COW buffers
	for origin := range cohe.cowBuffers {
		cohe.dataFlow.ReleaseBuffer(origin)
	}
	cohe.cowBuffers = nil

	return cohe.cleanup()
}

// GetCOWStats returns statistics about COW usage
func (cohe *COWExtentHandler) GetCOWStats() map[string]interface{} {
	stats := make(map[string]interface{})

	cohe.dataFlow.mu.RLock()
	defer cohe.dataFlow.mu.RUnlock()

	stats["total_buffers"] = len(cohe.dataFlow.buffers)
	stats["cow_buffers"] = len(cohe.cowBuffers)

	for origin, buffer := range cohe.cowBuffers {
		stats[origin+"_call_path"] = buffer.GetCallPath()
		stats[origin+"_is_shared"] = buffer.IsShared()
		stats[origin+"_size"] = buffer.Size()
	}

	return stats
}
