package stream

import (
	"github.com/cubefs/cubefs/proto"
	"github.com/cubefs/cubefs/util"
	"github.com/cubefs/cubefs/util/log"
)

// OptimizedExtentHandler demonstrates copy-on-write and zero-copy optimizations
type OptimizedExtentHandler struct {
	*ExtentHandler
	zcb        *ZeroCopyBuffer
	cowBuffer  *COWBuffer
	bufferPool *ExtentHandlerBuffer
}

// NewOptimizedExtentHandler creates an optimized extent handler
func NewOptimizedExtentHandler(stream *Streamer, offset int, storeMode int, size int,
	storageClass uint32, isMigration bool) *OptimizedExtentHandler {

	eh := NewExtentHandler(stream, offset, storeMode, size, storageClass, isMigration)

	ohe := &OptimizedExtentHandler{
		ExtentHandler: eh,
		zcb:           NewZeroCopyBuffer(),
	}

	// Initialize buffer based on store mode
	if storeMode == proto.TinyExtentType {
		ohe.cowBuffer = NewCOWBuffer(util.DefaultTinySizeLimit)
		ohe.bufferPool = NewExtentHandlerBuffer(util.DefaultTinySizeLimit, ohe.zcb)
	} else {
		ohe.cowBuffer = NewCOWBuffer(util.BlockSize)
		ohe.bufferPool = NewExtentHandlerBuffer(util.BlockSize, ohe.zcb)
	}

	return ohe
}

// writeOptimized demonstrates the optimized write method with copy-on-write
func (ohe *OptimizedExtentHandler) writeOptimized(data []byte, offset, size int, direct bool) (ek *proto.ExtentKey, err error) {
	var total int
	blksize := util.BlockSize
	if ohe.storeMode == proto.TinyExtentType {
		blksize = util.DefaultTinySizeLimit
	}

	for total < size {
		if ohe.packet == nil {
			ohe.packet = NewWritePacket(ohe.inode, offset+total, ohe.storeMode)
			log.LogDebugf("OptimizedExtentHandler write packet nil and new packet: eh(%v)", ohe)
			if direct {
				ohe.packet.Opcode = proto.OpSyncWrite
			}
		}

		packsize := int(ohe.packet.Size)
		write := util.Min(size-total, blksize-packsize)

		if write > 0 {
			// OPTIMIZATION 1: Use copy-on-write buffer
			ohe.writeDataOptimized(packsize, data[total:total+write])
			ohe.packet.Size += uint32(write)
			total += write
		}

		if int(ohe.packet.Size) >= blksize {
			ohe.flushPacketOptimized()
		}
	}

	ohe.size += total

	ek = &proto.ExtentKey{
		FileOffset: uint64(ohe.fileOffset),
		Size:       uint32(ohe.size),
	}
	return ek, nil
}

// writeDataOptimized uses copy-on-write semantics
func (ohe *OptimizedExtentHandler) writeDataOptimized(offset int, data []byte) {
	// OPTIMIZATION 2: Use slice views to avoid unnecessary copying
	if len(data) == 0 {
		return
	}

	// Check if we can use zero-copy for small writes
	if len(data) <= 64 && !ohe.cowBuffer.IsShared() {
		// Direct write for small, unshared buffers
		copy(ohe.packet.Data[offset:offset+len(data)], data)
		return
	}

	// Use copy-on-write for larger or shared buffers
	ohe.cowBuffer.WriteAt(offset, data)

	// Update packet data reference
	ohe.packet.Data = ohe.cowBuffer.GetData()
}

// flushPacketOptimized optimizes packet flushing
func (ohe *OptimizedExtentHandler) flushPacketOptimized() {
	if ohe.packet == nil {
		return
	}

	// OPTIMIZATION 3: Use buffer pool for packet data
	if ohe.bufferPool != nil {
		// Copy current data to buffer pool
		ohe.bufferPool.WriteData(ohe.packet.Data[:ohe.packet.Size])
		ohe.packet.Data = ohe.bufferPool.GetData()
	}

	ohe.pushToRequest(ohe.packet)
	ohe.packet = nil
}

// writeZeroCopy demonstrates zero-copy write operations
func (ohe *OptimizedExtentHandler) writeZeroCopy(data []byte, offset, size int) (ek *proto.ExtentKey, err error) {
	var total int
	blksize := util.BlockSize
	if ohe.storeMode == proto.TinyExtentType {
		blksize = util.DefaultTinySizeLimit
	}

	for total < size {
		if ohe.packet == nil {
			ohe.packet = NewWritePacket(ohe.inode, offset+total, ohe.storeMode)
			if ohe.bufferPool != nil {
				ohe.bufferPool.Reset()
			}
		}

		packsize := int(ohe.packet.Size)
		write := util.Min(size-total, blksize-packsize)

		if write > 0 {
			// OPTIMIZATION 4: Zero-copy write using buffer pool
			written := ohe.bufferPool.WriteData(data[total : total+write])
			ohe.packet.Size += uint32(written)
			total += written
		}

		if int(ohe.packet.Size) >= blksize {
			// Use buffer pool data directly
			if ohe.bufferPool != nil {
				ohe.packet.Data = ohe.bufferPool.GetData()
			}
			ohe.pushToRequest(ohe.packet)
			ohe.packet = nil
		}
	}

	ohe.size += total

	ek = &proto.ExtentKey{
		FileOffset: uint64(ohe.fileOffset),
		Size:       uint32(ohe.size),
	}
	return ek, nil
}

// cleanupOptimized properly releases resources
func (ohe *OptimizedExtentHandler) cleanupOptimized() error {
	if ohe.cowBuffer != nil {
		ohe.cowBuffer.Release()
	}

	if ohe.bufferPool != nil {
		ohe.bufferPool.Release()
	}

	return ohe.cleanup()
}
