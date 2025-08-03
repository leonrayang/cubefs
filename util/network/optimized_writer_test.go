package network

import (
	"bytes"
	"net"
	"testing"
	"time"
)

// Mock connection for testing
type mockConn struct {
	*bytes.Buffer
}

func (mc *mockConn) Close() error                       { return nil }
func (mc *mockConn) LocalAddr() net.Addr                { return nil }
func (mc *mockConn) RemoteAddr() net.Addr               { return nil }
func (mc *mockConn) SetDeadline(t time.Time) error      { return nil }
func (mc *mockConn) SetReadDeadline(t time.Time) error  { return nil }
func (mc *mockConn) SetWriteDeadline(t time.Time) error { return nil }

// Benchmark comparing current vs optimized approach
func BenchmarkPacketWrite(b *testing.B) {
	// Test data
	header := make([]byte, 64)
	args := make([]byte, 128)
	data := make([]byte, 1024)

	// Benchmark current approach (multiple writes)
	b.Run("Current-MultipleWrites", func(b *testing.B) {
		conn := &mockConn{&bytes.Buffer{}}
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			conn.Write(header)
			conn.Write(args)
			conn.Write(data)
		}
	})

	// Benchmark optimized approach (batch write)
	b.Run("Optimized-BatchWrite", func(b *testing.B) {
		conn := &mockConn{&bytes.Buffer{}}
		writer := NewOptimizedPacketWriter(conn, 64*1024)
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			writer.WritePacket(header, args, data)
		}
	})

	// Benchmark with buffering
	b.Run("Optimized-Buffered", func(b *testing.B) {
		conn := &mockConn{&bytes.Buffer{}}
		buffered := NewBufferedConn(conn, 64*1024)
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			buffered.Write(header)
			buffered.Write(args)
			buffered.Write(data)
			buffered.Flush()
		}
	})
}

// Benchmark buffer pool performance
func BenchmarkBufferPool(b *testing.B) {
	pool := NewBufferPool(64 * 1024)

	b.Run("Pool-GetPut", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			buf := pool.Get()
			pool.Put(buf)
		}
	})

	b.Run("Pool-Allocation", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			buf := make([]byte, 64*1024)
			_ = buf
		}
	})
}

// Benchmark write batcher
func BenchmarkWriteBatcher(b *testing.B) {
	conn := &mockConn{&bytes.Buffer{}}
	batcher := NewWriteBatcher(conn, 64*1024)

	smallData := make([]byte, 64)
	mediumData := make([]byte, 1024)
	largeData := make([]byte, 8192)

	b.Run("Batcher-SmallWrites", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			batcher.Write(smallData)
		}
		batcher.Flush()
	})

	b.Run("Batcher-MediumWrites", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			batcher.Write(mediumData)
		}
		batcher.Flush()
	})

	b.Run("Batcher-LargeWrites", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			batcher.Write(largeData)
		}
		batcher.Flush()
	})
}

// Benchmark connection pooling
func BenchmarkConnectionPool(b *testing.B) {
	pool := NewConnectionPool(10, 5*time.Second)

	b.Run("Pool-GetPut", func(b *testing.B) {
		// This is a mock test since we can't easily create real connections in benchmark
		for i := 0; i < b.N; i++ {
			// Simulate connection operations
			_ = pool
		}
	})
}

// Test optimized packet writer functionality
func TestOptimizedPacketWriter(t *testing.T) {
	conn := &mockConn{&bytes.Buffer{}}
	writer := NewOptimizedPacketWriter(conn, 64*1024)

	header := []byte("header")
	args := []byte("args")
	data := []byte("data")

	err := writer.WritePacket(header, args, data)
	if err != nil {
		t.Fatalf("WritePacket failed: %v", err)
	}

	// Verify data was written
	expected := append(header, append(args, data...)...)
	if !bytes.Equal(conn.Bytes(), expected) {
		t.Errorf("Expected %v, got %v", expected, conn.Bytes())
	}
}

// Test buffered connection
func TestBufferedConn(t *testing.T) {
	conn := &mockConn{&bytes.Buffer{}}
	buffered := NewBufferedConn(conn, 1024)

	data := []byte("test data")
	_, err := buffered.Write(data)
	if err != nil {
		t.Fatalf("Write failed: %v", err)
	}

	// Data should be buffered, not yet written
	if conn.Len() > 0 {
		t.Error("Data should be buffered, not written yet")
	}

	err = buffered.Flush()
	if err != nil {
		t.Fatalf("Flush failed: %v", err)
	}

	// Data should now be written
	if !bytes.Equal(conn.Bytes(), data) {
		t.Errorf("Expected %v, got %v", data, conn.Bytes())
	}
}

// Test buffer pool
func TestBufferPool(t *testing.T) {
	pool := NewBufferPool(1024)

	buf1 := pool.Get()
	if len(buf1) != 1024 {
		t.Errorf("Expected buffer size 1024, got %d", len(buf1))
	}

	// Modify buffer
	copy(buf1, []byte("test"))

	pool.Put(buf1)

	buf2 := pool.Get()
	if len(buf2) != 1024 {
		t.Errorf("Expected buffer size 1024, got %d", len(buf2))
	}

	// Buffer should be reused (same underlying array)
	if &buf1[0] != &buf2[0] {
		t.Error("Buffer should be reused from pool")
	}
}

// Test write batcher
func TestWriteBatcher(t *testing.T) {
	conn := &mockConn{&bytes.Buffer{}}
	batcher := NewWriteBatcher(conn, 1024)

	data1 := []byte("data1")
	data2 := []byte("data2")

	err := batcher.Write(data1)
	if err != nil {
		t.Fatalf("Write failed: %v", err)
	}

	err = batcher.Write(data2)
	if err != nil {
		t.Fatalf("Write failed: %v", err)
	}

	// Data should be batched, not yet written
	if conn.Len() > 0 {
		t.Error("Data should be batched, not written yet")
	}

	err = batcher.Flush()
	if err != nil {
		t.Fatalf("Flush failed: %v", err)
	}

	expected := append(data1, data2...)
	if !bytes.Equal(conn.Bytes(), expected) {
		t.Errorf("Expected %v, got %v", expected, conn.Bytes())
	}
}

// Test metrics
func TestWriteMetrics(t *testing.T) {
	metrics := NewWriteMetrics()

	metrics.RecordWrite(100, 10*time.Millisecond)
	metrics.RecordWrite(200, 20*time.Millisecond)

	writes, bytes, latency := metrics.GetStats()

	if writes != 2 {
		t.Errorf("Expected 2 writes, got %d", writes)
	}

	if bytes != 300 {
		t.Errorf("Expected 300 bytes, got %d", bytes)
	}

	if latency != 20*time.Millisecond {
		t.Errorf("Expected 20ms latency, got %v", latency)
	}
}
