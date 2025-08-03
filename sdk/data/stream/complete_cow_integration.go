package stream

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/cubefs/cubefs/proto"
	"github.com/cubefs/cubefs/util"
	"github.com/cubefs/cubefs/util/log"
)

// CompleteCOWIntegration provides comprehensive COW for the entire data flow
type CompleteCOWIntegration struct {
	dataFlow    *COWDataFlow
	globalStats map[string]interface{}
	mu          sync.RWMutex
}

// NewCompleteCOWIntegration creates a new complete COW integration
func NewCompleteCOWIntegration() *CompleteCOWIntegration {
	return &CompleteCOWIntegration{
		dataFlow:    NewCOWDataFlow(),
		globalStats: make(map[string]interface{}),
	}
}

// COWStreamer extends Streamer with comprehensive COW
type COWStreamer struct {
	*Streamer
	cowIntegration *CompleteCOWIntegration
}

// NewCOWStreamer creates a new COW streamer
func NewCOWStreamer(stream *Streamer) *COWStreamer {
	return &COWStreamer{
		Streamer:       stream,
		cowIntegration: NewCompleteCOWIntegration(),
	}
}

// IssueWriteRequestWithCOW implements COW for the entire write path
func (cs *COWStreamer) IssueWriteRequestWithCOW(offset int, data []byte, flags int, checkFunc func() error, storageClass uint32, isMigration bool) (write int, err error) {
	// Step 1: Create COW buffer for incoming data at the client level
	dataOrigin := "client_write_request"
	dataBuffer := cs.cowIntegration.dataFlow.AllocateBuffer(len(data), dataOrigin)
	copy(dataBuffer.GetData(), data)

	// Step 2: Pass COW buffer through the entire call chain
	write, err = cs.writeWithCOW(dataBuffer, offset, len(data), flags, checkFunc, storageClass, isMigration)

	// Step 3: Cleanup
	cs.cowIntegration.dataFlow.ReleaseBuffer(dataOrigin)

	return write, err
}

// writeWithCOW implements COW for the streamer write method
func (cs *COWStreamer) writeWithCOW(dataBuffer *ComprehensiveCOWBuffer, offset, size, flags int, checkFunc func() error, storageClass uint32, isMigration bool) (total int, err error) {
	var retryTimes int8

	if atomic.LoadInt32(&cs.status) >= StreamerError {
		return 0, errors.New(fmt.Sprintf("IssueWriteRequest: stream writer in error status, ino(%v)", cs.inode))
	}

	direct := false
	if flags&proto.FlagsSyncWrite != 0 {
		direct = true
	}

begin:
	if flags&proto.FlagsAppend != 0 {
		filesize, _ := cs.extents.Size()
		offset = filesize
	}

	log.LogDebugf("COWStreamer write enter: ino(%v) offset(%v) size(%v) flags(%v) storageClass(%v) isMigration(%v)",
		cs.inode, offset, size, flags, storageClass, isMigration)

	ctx := context.Background()
	cs.client.writeLimiter.Wait(ctx)
	cs.client.LimitManager.WriteAlloc(ctx, size)

	// Step 3: Use COW buffer for prepare write requests
	requests := cs.extents.PrepareWriteRequests(offset, size, dataBuffer.GetData())
	log.LogDebugf("COWStreamer write: ino(%v) prepared requests(%v)", cs.inode, requests)

	isChecked := false
	// Must flush before doing overwrite
	for _, req := range requests {
		if req.ExtentKey == nil {
			continue
		}
		err = cs.flush(true)
		if err != nil {
			return
		}
		// some extent key in requests with partition id 0 means it's append operation and on flight.
		// need to flush and get the right key then used to make modification
		requests = cs.extents.PrepareWriteRequests(offset, size, dataBuffer.GetData())
		log.LogDebugf("COWStreamer write: ino(%v) prepared requests after flush(%v)", cs.inode, requests)
		break
	}

	for _, req := range requests {
		var writeSize int
		if req.ExtentKey != nil {
			if cs.client.bcacheEnable {
				cacheKey := util.GenerateRepVolKey(cs.client.volumeName, cs.inode, req.ExtentKey.PartitionId, req.ExtentKey.ExtentId, uint64(req.FileOffset))
				if _, ok := cs.inflightEvictL1cache.Load(cacheKey); !ok {
					go func(cacheKey string) {
						cs.inflightEvictL1cache.Store(cacheKey, true)
						cs.client.evictBcache(cacheKey)
						cs.inflightEvictL1cache.Delete(cacheKey)
					}(cacheKey)
				}
			}
			log.LogDebugf("action[cowstreamer.write] inode [%v] latest seq [%v] extentkey seq [%v]  info [%v] before compare seq",
				cs.inode, cs.verSeq, req.ExtentKey.GetSeq(), req.ExtentKey)
			if req.ExtentKey.GetSeq() == cs.verSeq {
				writeSize, err = cs.doOverwriteWithCOW(req, direct, storageClass, dataBuffer)
				if err == proto.ErrCodeVersionOp {
					log.LogDebugf("action[cowstreamer.write] write need version update")
					if err = cs.GetExtentsForceRefresh(); err != nil {
						log.LogErrorf("action[cowstreamer.write] err %v", err)
						return
					}
					if retryTimes > 3 {
						err = proto.ErrCodeVersionOp
						log.LogWarnf("action[cowstreamer.write] err %v", err)
						return
					}
					time.Sleep(time.Millisecond * 100)
					retryTimes++
					log.LogDebugf("action[cowstreamer.write] err %v retryTimes %v", err, retryTimes)
					goto begin
				}
				log.LogDebugf("action[cowstreamer.write] err %v retryTimes %v", err, retryTimes)
			} else {
				log.LogDebugf("action[cowstreamer.write] ino %v doOverWriteByAppend extent key (%v)", cs.inode, req.ExtentKey)
				writeSize, _, err, _ = cs.doOverWriteByAppendWithCOW(req, direct, storageClass, isMigration, dataBuffer)
			}
			if cs.client.bcacheEnable {
				cacheKey := util.GenerateKey(cs.client.volumeName, cs.inode, uint64(req.FileOffset))
				go cs.client.evictBcache(cacheKey)
			}
		} else {
			if !isChecked && checkFunc != nil {
				isChecked = true
				if err = checkFunc(); err != nil {
					return
				}
			}
			writeSize, err = cs.doWriteAppendWithCOW(req, direct, storageClass, isMigration, dataBuffer)
		}
		if err != nil {
			log.LogErrorf("COWStreamer write: ino(%v) err(%v)", cs.inode, err)
			break
		}
		total += writeSize
	}

	return total, err
}

// doOverwriteWithCOW implements COW for overwrite operations
func (cs *COWStreamer) doOverwriteWithCOW(req *ExtentRequest, direct bool, storageClass uint32, dataBuffer *ComprehensiveCOWBuffer) (writeSize int, err error) {
	// Create COW extent handler with default store mode
	cohe := NewCOWExtentHandler(cs.Streamer, req.FileOffset, proto.NormalExtentType, req.Size, storageClass, false)

	// Use COW buffer for data transfer
	_, err = cohe.writeWithCOW(dataBuffer.GetData(), req.FileOffset, req.Size, direct)

	// Cleanup
	cohe.cleanupWithCOW()

	return req.Size, err
}

// doOverWriteByAppendWithCOW implements COW for append operations
func (cs *COWStreamer) doOverWriteByAppendWithCOW(req *ExtentRequest, direct bool, storageClass uint32, isMigration bool, dataBuffer *ComprehensiveCOWBuffer) (total int, extKey *proto.ExtentKey, err error, status int32) {
	// Create COW extent handler with default store mode
	cohe := NewCOWExtentHandler(cs.Streamer, req.FileOffset, proto.NormalExtentType, req.Size, storageClass, isMigration)

	// Use COW buffer for data transfer
	extKey, err = cohe.writeWithCOW(dataBuffer.GetData(), req.FileOffset, req.Size, direct)

	// Cleanup
	cohe.cleanupWithCOW()

	return req.Size, extKey, err, 0
}

// doWriteAppendWithCOW implements COW for append operations
func (cs *COWStreamer) doWriteAppendWithCOW(req *ExtentRequest, direct bool, storageClass uint32, isMigration bool, dataBuffer *ComprehensiveCOWBuffer) (writeSize int, err error) {
	// Create COW extent handler with default store mode
	cohe := NewCOWExtentHandler(cs.Streamer, req.FileOffset, proto.NormalExtentType, req.Size, storageClass, isMigration)

	// Use COW buffer for data transfer
	_, err = cohe.writeWithCOW(dataBuffer.GetData(), req.FileOffset, req.Size, direct)

	// Cleanup
	cohe.cleanupWithCOW()

	return req.Size, err
}

// GetCOWStats returns comprehensive COW statistics
func (cs *COWStreamer) GetCOWStats() map[string]interface{} {
	cs.cowIntegration.mu.RLock()
	defer cs.cowIntegration.mu.RUnlock()

	stats := make(map[string]interface{})
	stats["total_buffers"] = len(cs.cowIntegration.dataFlow.buffers)
	stats["global_stats"] = cs.cowIntegration.globalStats

	return stats
}

// COWExtentClient extends ExtentClient with comprehensive COW
type COWExtentClient struct {
	*ExtentClient
	cowIntegration *CompleteCOWIntegration
}

// NewCOWExtentClient creates a new COW extent client
func NewCOWExtentClient(ec *ExtentClient) *COWExtentClient {
	return &COWExtentClient{
		ExtentClient:   ec,
		cowIntegration: NewCompleteCOWIntegration(),
	}
}

// WriteWithCOW implements COW for the extent client write method
func (cowec *COWExtentClient) WriteWithCOW(inode uint64, offset int, data []byte, flags int, checkFunc func() error, storageClass uint32, isMigration bool) (write int, err error) {
	prefix := fmt.Sprintf("WriteWithCOW{ino(%v)offset(%v)size(%v)}", inode, offset, len(data))
	s := cowec.GetStreamer(inode)
	if s == nil {
		log.LogErrorf("Prefix(%v): stream is not opened yet", prefix)
		return 0, syscall.EBADF
	}

	if !cowec.dataWrapper.CanWriteByClass(storageClass) {
		log.LogWarnf("WriteWithCOW: target storage class is alrady full, can't write more. pref %s, class %s",
			prefix, proto.StorageClassString(storageClass))
		return 0, syscall.EDQUOT
	}

	s.once.Do(func() {
		// TODO unhandled error
		s.GetExtents(isMigration)
	})

	// Create COW streamer
	cowStreamer := NewCOWStreamer(s)
	write, err = cowStreamer.IssueWriteRequestWithCOW(offset, data, flags, checkFunc, storageClass, isMigration)
	if err != nil {
		log.LogErrorf("WriteWithCOW error: %v", err)
	}
	return
}
