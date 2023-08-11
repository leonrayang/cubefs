package util

import (
	"fmt"
	"runtime"
	"sync"
	"sync/atomic"
	"time"
)

var used int64

// Alloc returns size bytes memory from Go heap.
func Alloc(size int) []byte {
	zeros := powerOf2(size)
	b := *pools[zeros].Get().(*[]byte)
	if cap(b) < size {
		panic(fmt.Sprintf("%d < %d", cap(b), size))
	}
	atomic.AddInt64(&used, int64(cap(b)))
	return b[:size]
}

// Free returns memory to Go heap.
func Free(b []byte) {
	// buf could be zero length
	atomic.AddInt64(&used, -int64(cap(b)))
	pools[powerOf2(cap(b))].Put(&b)
}

// AllocMemory returns the allocated memory
func AllocMemory() int64 {
	return atomic.LoadInt64(&used)
}

var pools []*sync.Pool

func powerOf2(s int) int {
	var bits int
	var p int = 1
	for p < s {
		bits++
		p *= 2
	}
	return bits
}

func init() {
	pools = make([]*sync.Pool, 30) // 1 - 1G
	for i := 0; i < 30; i++ {
		func(bits int) {
			pools[i] = &sync.Pool{
				New: func() interface{} {
					b := make([]byte, 1<<bits)
					return &b
				},
			}
		}(i)
	}
	go func() {
		for {
			time.Sleep(time.Minute * 10)
			runtime.GC()
		}
	}()
}
