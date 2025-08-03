package proto

import (
	"bytes"
	"net"
	"testing"
	"time"

	"github.com/cubefs/cubefs/util/network"
)

// Realistic network connection simulation
type realisticConn struct {
	*bytes.Buffer
	writeDelay time.Duration // Simulate network latency
}

func (rc *realisticConn) Write(b []byte) (int, error) {
	// Simulate network write delay
	time.Sleep(rc.writeDelay)
	return rc.Buffer.Write(b)
}

func (rc *realisticConn) Close() error                       { return nil }
func (rc *realisticConn) LocalAddr() net.Addr                { return nil }
func (rc *realisticConn) RemoteAddr() net.Addr               { return nil }
func (rc *realisticConn) SetDeadline(t time.Time) error      { return nil }
func (rc *realisticConn) SetReadDeadline(t time.Time) error  { return nil }
func (rc *realisticConn) SetWriteDeadline(t time.Time) error { return nil }

func BenchmarkRealisticNetworkWrite(b *testing.B) {
	// Initialize buffer pool
	InitBufferPool(1024 * 1024)

	// Test different packet sizes
	sizes := []struct {
		name     string
		argSize  int
		dataSize int
	}{
		{"Small", 64, 512},
		{"Medium", 256, 4096},
		{"Large", 1024, 32768},
	}

	for _, size := range sizes {
		b.Run(size.name, func(b *testing.B) {
			// Create test packet
			p := NewPacket()
			p.Opcode = OpWrite
			p.Size = uint32(size.dataSize)
			p.ArgLen = uint32(size.argSize)
			p.Arg = make([]byte, size.argSize)
			p.Data = make([]byte, size.dataSize)

			// Fill with test data
			for i := range p.Arg {
				p.Arg[i] = byte(i % 256)
			}
			for i := range p.Data {
				p.Data[i] = byte(i % 256)
			}

			// Test with different network latencies
			latencies := []time.Duration{
				0,                      // No latency
				10 * time.Microsecond,  // Low latency
				100 * time.Microsecond, // Medium latency
				1 * time.Millisecond,   // High latency
			}

			for _, latency := range latencies {
				b.Run(latency.String(), func(b *testing.B) {
					// Benchmark current implementation
					b.Run("Current", func(b *testing.B) {
						for i := 0; i < b.N; i++ {
							conn := &realisticConn{&bytes.Buffer{}, latency}
							p.WriteToConn(conn)
						}
					})

					// Benchmark optimized implementation
					b.Run("Optimized", func(b *testing.B) {
						for i := 0; i < b.N; i++ {
							conn := &realisticConn{&bytes.Buffer{}, latency}
							p.WriteToConnOptimized(conn)
						}
					})
				})
			}
		})
	}
}

func BenchmarkBatchWriteOperations(b *testing.B) {
	// Initialize buffer pool
	InitBufferPool(1024 * 1024)

	// Create multiple packets for batch testing
	packets := make([]*Packet, 10)
	for i := range packets {
		p := NewPacket()
		p.Opcode = OpWrite
		p.Size = 1024
		p.ArgLen = 128
		p.Arg = make([]byte, 128)
		p.Data = make([]byte, 1024)

		// Fill with test data
		for j := range p.Arg {
			p.Arg[j] = byte(i*100 + j)
		}
		for j := range p.Data {
			p.Data[j] = byte(i*100 + j)
		}

		packets[i] = p
	}

	// Test with different network latencies
	latencies := []time.Duration{
		0,
		10 * time.Microsecond,
		100 * time.Microsecond,
	}

	for _, latency := range latencies {
		b.Run(latency.String(), func(b *testing.B) {
			// Benchmark individual writes
			b.Run("Individual", func(b *testing.B) {
				for i := 0; i < b.N; i++ {
					conn := &realisticConn{&bytes.Buffer{}, latency}
					for _, p := range packets {
						p.WriteToConn(conn)
					}
				}
			})

			// Benchmark batch writes
			b.Run("Batch", func(b *testing.B) {
				for i := 0; i < b.N; i++ {
					conn := &realisticConn{&bytes.Buffer{}, latency}
					WriteToConnOptimizedBatch(conn, packets)
				}
			})
		})
	}
}

func BenchmarkHighFrequencyWrites(b *testing.B) {
	// Initialize buffer pool
	InitBufferPool(1024 * 1024)

	// Create a small packet for high-frequency testing
	p := NewPacket()
	p.Opcode = OpWrite
	p.Size = 256
	p.ArgLen = 32
	p.Arg = make([]byte, 32)
	p.Data = make([]byte, 256)

	// Fill with test data
	for i := range p.Arg {
		p.Arg[i] = byte(i % 256)
	}
	for i := range p.Data {
		p.Data[i] = byte(i % 256)
	}

	// Test high-frequency scenarios
	b.Run("HighFrequency", func(b *testing.B) {
		// Current implementation
		b.Run("Current", func(b *testing.B) {
			for i := 0; i < b.N; i++ {
				conn := &realisticConn{&bytes.Buffer{}, 10 * time.Microsecond}
				p.WriteToConn(conn)
			}
		})

		// Optimized implementation
		b.Run("Optimized", func(b *testing.B) {
			for i := 0; i < b.N; i++ {
				conn := &realisticConn{&bytes.Buffer{}, 10 * time.Microsecond}
				p.WriteToConnOptimized(conn)
			}
		})

		// Optimized with batching
		b.Run("OptimizedWithBatching", func(b *testing.B) {
			conn := &realisticConn{&bytes.Buffer{}, 10 * time.Microsecond}
			batcher := network.NewWriteBatcher(conn, 64*1024)
			defer batcher.Flush()

			for i := 0; i < b.N; i++ {
				p.WriteToConnOptimizedWithBatching(conn, batcher)
			}
		})
	})
}
