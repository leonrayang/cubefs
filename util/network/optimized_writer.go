package network

import (
	"bufio"
	"net"
	"sync"
	"time"
)

// OptimizedPacketWriter provides optimized network writing for packets
type OptimizedPacketWriter struct {
	conn     net.Conn
	buffered *BufferedConn
	pool     *BufferPool
}

// NewOptimizedPacketWriter creates a new optimized packet writer
func NewOptimizedPacketWriter(conn net.Conn, bufferSize int) *OptimizedPacketWriter {
	return &OptimizedPacketWriter{
		conn:     conn,
		buffered: NewBufferedConn(conn, bufferSize),
		pool:     NewBufferPool(64 * 1024), // 64KB default
	}
}

// WritePacket writes a packet using optimized batching
func (opw *OptimizedPacketWriter) WritePacket(header, args, data []byte) error {
	// Get buffer from pool
	buf := opw.pool.Get()
	defer opw.pool.Put(buf)

	// Prepare buffers for batch write
	buffers := net.Buffers{
		header,
		args,
	}

	if len(data) > 0 {
		buffers = append(buffers, data)
	}

	// Use net.Buffers for efficient batch writing
	_, err := buffers.WriteTo(opw.buffered)
	if err != nil {
		return err
	}

	// Flush to ensure data is sent
	return opw.buffered.Flush()
}

// Flush flushes the buffered connection
func (opw *OptimizedPacketWriter) Flush() error {
	return opw.buffered.Flush()
}

// BufferedConn wraps a net.Conn with buffering for better performance
type BufferedConn struct {
	net.Conn
	buf *bufio.Writer
}

// NewBufferedConn creates a new buffered connection
func NewBufferedConn(conn net.Conn, bufferSize int) *BufferedConn {
	return &BufferedConn{
		Conn: conn,
		buf:  bufio.NewWriterSize(conn, bufferSize),
	}
}

// Write implements io.Writer with buffering
func (bc *BufferedConn) Write(b []byte) (int, error) {
	return bc.buf.Write(b)
}

// Flush flushes the buffer to the underlying connection
func (bc *BufferedConn) Flush() error {
	return bc.buf.Flush()
}

// BufferPool provides efficient buffer reuse
type BufferPool struct {
	pool sync.Pool
}

// NewBufferPool creates a new buffer pool
func NewBufferPool(defaultSize int) *BufferPool {
	return &BufferPool{
		pool: sync.Pool{
			New: func() interface{} {
				return make([]byte, defaultSize)
			},
		},
	}
}

// Get retrieves a buffer from the pool
func (bp *BufferPool) Get() []byte {
	return bp.pool.Get().([]byte)
}

// Put returns a buffer to the pool
func (bp *BufferPool) Put(buf []byte) {
	bp.pool.Put(buf)
}

// ConnectionPool manages connection reuse
type ConnectionPool struct {
	mu       sync.Mutex
	conns    map[string][]net.Conn
	maxConns int
	timeout  time.Duration
}

// NewConnectionPool creates a new connection pool
func NewConnectionPool(maxConns int, timeout time.Duration) *ConnectionPool {
	return &ConnectionPool{
		conns:    make(map[string][]net.Conn),
		maxConns: maxConns,
		timeout:  timeout,
	}
}

// Get retrieves a connection from the pool or creates a new one
func (cp *ConnectionPool) Get(addr string) (net.Conn, error) {
	cp.mu.Lock()
	defer cp.mu.Unlock()

	if conns, exists := cp.conns[addr]; exists && len(conns) > 0 {
		conn := conns[len(conns)-1]
		cp.conns[addr] = conns[:len(conns)-1]
		return conn, nil
	}

	return net.DialTimeout("tcp", addr, cp.timeout)
}

// Put returns a connection to the pool
func (cp *ConnectionPool) Put(addr string, conn net.Conn) {
	cp.mu.Lock()
	defer cp.mu.Unlock()

	if len(cp.conns[addr]) < cp.maxConns {
		cp.conns[addr] = append(cp.conns[addr], conn)
	} else {
		conn.Close()
	}
}

// WriteBatcher batches multiple small writes into larger operations
type WriteBatcher struct {
	conn    net.Conn
	buffer  []byte
	offset  int
	maxSize int
	mu      sync.Mutex
}

// NewWriteBatcher creates a new write batcher
func NewWriteBatcher(conn net.Conn, maxSize int) *WriteBatcher {
	return &WriteBatcher{
		conn:    conn,
		buffer:  make([]byte, maxSize),
		maxSize: maxSize,
	}
}

// Write adds data to the batch
func (wb *WriteBatcher) Write(data []byte) error {
	wb.mu.Lock()
	defer wb.mu.Unlock()

	if wb.offset+len(data) > wb.maxSize {
		// Flush current buffer
		if err := wb.flush(); err != nil {
			return err
		}
	}

	copy(wb.buffer[wb.offset:], data)
	wb.offset += len(data)
	return nil
}

// Flush writes the current batch to the connection
func (wb *WriteBatcher) Flush() error {
	wb.mu.Lock()
	defer wb.mu.Unlock()
	return wb.flush()
}

// flush is the internal flush method (assumes lock is held)
func (wb *WriteBatcher) flush() error {
	if wb.offset > 0 {
		_, err := wb.conn.Write(wb.buffer[:wb.offset])
		wb.offset = 0
		return err
	}
	return nil
}

// OptimizeTCPConn applies TCP optimizations for better performance
func OptimizeTCPConn(conn net.Conn) error {
	if tcpConn, ok := conn.(*net.TCPConn); ok {
		// Disable Nagle's algorithm for low-latency writes
		if err := tcpConn.SetNoDelay(true); err != nil {
			return err
		}

		// Set write buffer size
		if err := tcpConn.SetWriteBuffer(64 * 1024); err != nil {
			return err
		}

		// Set read buffer size
		if err := tcpConn.SetReadBuffer(64 * 1024); err != nil {
			return err
		}

		// Set keep-alive
		if err := tcpConn.SetKeepAlive(true); err != nil {
			return err
		}

		if err := tcpConn.SetKeepAlivePeriod(30 * time.Second); err != nil {
			return err
		}
	}
	return nil
}

// PacketWriter interface for optimized packet writing
type PacketWriter interface {
	WritePacket(header, args, data []byte) error
	Flush() error
}

// OptimizedPacketWriterWithMetrics adds metrics to packet writing
type OptimizedPacketWriterWithMetrics struct {
	writer  PacketWriter
	metrics *WriteMetrics
}

// WriteMetrics tracks performance metrics
type WriteMetrics struct {
	TotalWrites     int64
	TotalBytes      int64
	WriteLatency    time.Duration
	BatchEfficiency float64
	mu              sync.RWMutex
}

// NewWriteMetrics creates new write metrics
func NewWriteMetrics() *WriteMetrics {
	return &WriteMetrics{}
}

// RecordWrite records a write operation
func (wm *WriteMetrics) RecordWrite(bytes int, latency time.Duration) {
	wm.mu.Lock()
	defer wm.mu.Unlock()

	wm.TotalWrites++
	wm.TotalBytes += int64(bytes)
	wm.WriteLatency = latency
}

// GetStats returns current statistics
func (wm *WriteMetrics) GetStats() (int64, int64, time.Duration) {
	wm.mu.RLock()
	defer wm.mu.RUnlock()

	return wm.TotalWrites, wm.TotalBytes, wm.WriteLatency
}

// NewOptimizedPacketWriterWithMetrics creates a packet writer with metrics
func NewOptimizedPacketWriterWithMetrics(conn net.Conn, bufferSize int) *OptimizedPacketWriterWithMetrics {
	return &OptimizedPacketWriterWithMetrics{
		writer:  NewOptimizedPacketWriter(conn, bufferSize),
		metrics: NewWriteMetrics(),
	}
}

// WritePacket writes a packet with metrics tracking
func (opwm *OptimizedPacketWriterWithMetrics) WritePacket(header, args, data []byte) error {
	start := time.Now()
	defer func() {
		opwm.metrics.RecordWrite(len(header)+len(args)+len(data), time.Since(start))
	}()

	return opwm.writer.WritePacket(header, args, data)
}

// Flush flushes the writer
func (opwm *OptimizedPacketWriterWithMetrics) Flush() error {
	return opwm.writer.Flush()
}

// GetMetrics returns the current metrics
func (opwm *OptimizedPacketWriterWithMetrics) GetMetrics() *WriteMetrics {
	return opwm.metrics
}
