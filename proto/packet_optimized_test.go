package proto

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

func TestWriteToConnOptimized(t *testing.T) {
	// Initialize buffer pool
	InitBufferPool(1024 * 1024) // 1MB limit

	// Create a test packet
	p := NewPacket()
	p.Opcode = OpWrite
	p.Size = 1024
	p.ArgLen = 128
	p.Arg = make([]byte, 128)
	p.Data = make([]byte, 1024)

	// Fill with test data
	for i := range p.Arg {
		p.Arg[i] = byte(i % 256)
	}
	for i := range p.Data {
		p.Data[i] = byte(i % 256)
	}

	// Test current implementation
	conn1 := &mockConn{&bytes.Buffer{}}
	err := p.WriteToConn(conn1)
	if err != nil {
		t.Fatalf("Current WriteToConn failed: %v", err)
	}

	// Test optimized implementation
	conn2 := &mockConn{&bytes.Buffer{}}
	err = p.WriteToConnOptimized(conn2)
	if err != nil {
		t.Fatalf("Optimized WriteToConn failed: %v", err)
	}

	// Compare results - they should be identical
	if !bytes.Equal(conn1.Bytes(), conn2.Bytes()) {
		t.Errorf("Optimized implementation produced different output")
		t.Logf("Current length: %d", len(conn1.Bytes()))
		t.Logf("Optimized length: %d", len(conn2.Bytes()))
	}
}

func TestWriteToConnOptimizedWithMetrics(t *testing.T) {
	// Initialize buffer pool
	InitBufferPool(1024 * 1024)

	// Create a test packet
	p := NewPacket()
	p.Opcode = OpWrite
	p.Size = 512
	p.ArgLen = 64
	p.Arg = make([]byte, 64)
	p.Data = make([]byte, 512)

	conn := &mockConn{&bytes.Buffer{}}
	err := p.WriteToConnOptimizedWithMetrics(conn)
	if err != nil {
		t.Fatalf("WriteToConnOptimizedWithMetrics failed: %v", err)
	}

	// Verify data was written
	if conn.Len() == 0 {
		t.Error("No data was written")
	}
}

func TestWriteToConnOptimizedBatch(t *testing.T) {
	// Initialize buffer pool
	InitBufferPool(1024 * 1024)

	// Create multiple test packets
	packets := make([]*Packet, 3)
	for i := range packets {
		p := NewPacket()
		p.Opcode = OpWrite
		p.Size = 256
		p.ArgLen = 32
		p.Arg = make([]byte, 32)
		p.Data = make([]byte, 256)

		// Fill with test data
		for j := range p.Arg {
			p.Arg[j] = byte(i*100 + j)
		}
		for j := range p.Data {
			p.Data[j] = byte(i*100 + j)
		}

		packets[i] = p
	}

	conn := &mockConn{&bytes.Buffer{}}
	err := WriteToConnOptimizedBatch(conn, packets)
	if err != nil {
		t.Fatalf("WriteToConnOptimizedBatch failed: %v", err)
	}

	// Verify data was written
	if conn.Len() == 0 {
		t.Error("No data was written")
	}
}

func BenchmarkWriteToConnComparison(b *testing.B) {
	// Initialize buffer pool
	InitBufferPool(1024 * 1024)

	// Create a test packet
	p := NewPacket()
	p.Opcode = OpWrite
	p.Size = 1024
	p.ArgLen = 128
	p.Arg = make([]byte, 128)
	p.Data = make([]byte, 1024)

	// Benchmark current implementation
	b.Run("Current", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			conn := &mockConn{&bytes.Buffer{}}
			p.WriteToConn(conn)
		}
	})

	// Benchmark optimized implementation
	b.Run("Optimized", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			conn := &mockConn{&bytes.Buffer{}}
			p.WriteToConnOptimized(conn)
		}
	})

	// Benchmark optimized with metrics
	b.Run("OptimizedWithMetrics", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			conn := &mockConn{&bytes.Buffer{}}
			p.WriteToConnOptimizedWithMetrics(conn)
		}
	})
}

func TestWriteToConnOptimizedWithVersionList(t *testing.T) {
	// Initialize buffer pool
	InitBufferPool(1024 * 1024)

	// Create a test packet with version list
	p := NewPacket()
	p.Opcode = OpWrite
	p.Size = 512
	p.ArgLen = 64
	p.Arg = make([]byte, 64)
	p.Data = make([]byte, 512)
	p.ExtentType |= VersionListFlag // Enable version list

	conn := &mockConn{&bytes.Buffer{}}
	err := p.WriteToConnOptimized(conn)
	if err != nil {
		t.Fatalf("WriteToConnOptimized with version list failed: %v", err)
	}

	// Verify data was written
	if conn.Len() == 0 {
		t.Error("No data was written")
	}
}
