package proto

import (
	"net"
	"time"

	"github.com/cubefs/cubefs/util/log"
	"github.com/cubefs/cubefs/util/network"
)

// WriteToConnOptimized writes through the given connection using optimized network writer
func (p *Packet) WriteToConnOptimized(c net.Conn) (err error) {
	// Create optimized packet writer
	writer := network.NewOptimizedPacketWriter(c, 64*1024)
	defer writer.Flush()

	// Set write deadline
	c.SetWriteDeadline(time.Now().Add(WriteDeadlineTime * time.Second))

	// Prepare header
	headSize := p.CalcPacketHeaderSize()
	header, err := Buffers.Get(headSize)
	if err != nil {
		header = make([]byte, headSize)
	}
	defer Buffers.Put(header)
	p.MarshalHeader(header)

	// Prepare args
	args := p.Arg[:int(p.ArgLen)]

	// Prepare data (if present)
	var data []byte
	if p.Data != nil && p.Size != 0 {
		data = p.Data[:p.Size]
	}

	// Write packet using optimized writer
	err = writer.WritePacket(header, args, data)
	if err != nil {
		return err
	}

	// Handle version list if present
	if p.IsVersionList() {
		versionData, err1 := p.MarshalVersionSlice()
		if err1 != nil {
			log.LogErrorf("MarshalVersionSlice: marshal version info failed, err %s", err1.Error())
			return err1
		}

		// Write version data separately since it's optional
		_, err = c.Write(versionData)
		if err != nil {
			return err
		}
	}

	return nil
}

// WriteToConnOptimizedWithMetrics writes through the given connection with performance metrics
func (p *Packet) WriteToConnOptimizedWithMetrics(c net.Conn) (err error) {
	// Create optimized packet writer with metrics
	writer := network.NewOptimizedPacketWriterWithMetrics(c, 64*1024)
	defer writer.Flush()

	// Set write deadline
	c.SetWriteDeadline(time.Now().Add(WriteDeadlineTime * time.Second))

	// Prepare header
	headSize := p.CalcPacketHeaderSize()
	header, err := Buffers.Get(headSize)
	if err != nil {
		header = make([]byte, headSize)
	}
	defer Buffers.Put(header)
	p.MarshalHeader(header)

	// Prepare args
	args := p.Arg[:int(p.ArgLen)]

	// Prepare data (if present)
	var data []byte
	if p.Data != nil && p.Size != 0 {
		data = p.Data[:p.Size]
	}

	// Write packet using optimized writer with metrics
	err = writer.WritePacket(header, args, data)
	if err != nil {
		return err
	}

	// Handle version list if present
	if p.IsVersionList() {
		versionData, err1 := p.MarshalVersionSlice()
		if err1 != nil {
			log.LogErrorf("MarshalVersionSlice: marshal version info failed, err %s", err1.Error())
			return err1
		}

		// Write version data separately since it's optional
		_, err = c.Write(versionData)
		if err != nil {
			return err
		}
	}

	return nil
}

// WriteToConnOptimizedBatch writes multiple packets efficiently
func WriteToConnOptimizedBatch(c net.Conn, packets []*Packet) error {
	// Create optimized packet writer
	writer := network.NewOptimizedPacketWriter(c, 64*1024)
	defer writer.Flush()

	// Set write deadline
	c.SetWriteDeadline(time.Now().Add(WriteDeadlineTime * time.Second))

	for _, p := range packets {
		// Prepare header
		headSize := p.CalcPacketHeaderSize()
		header, err := Buffers.Get(headSize)
		if err != nil {
			header = make([]byte, headSize)
		}
		defer Buffers.Put(header)
		p.MarshalHeader(header)

		// Prepare args
		args := p.Arg[:int(p.ArgLen)]

		// Prepare data (if present)
		var data []byte
		if p.Data != nil && p.Size != 0 {
			data = p.Data[:p.Size]
		}

		// Write packet using optimized writer
		err = writer.WritePacket(header, args, data)
		if err != nil {
			return err
		}

		// Handle version list if present
		if p.IsVersionList() {
			versionData, err1 := p.MarshalVersionSlice()
			if err1 != nil {
				log.LogErrorf("MarshalVersionSlice: marshal version info failed, err %s", err1.Error())
				return err1
			}

			// Write version data separately since it's optional
			_, err = c.Write(versionData)
			if err != nil {
				return err
			}
		}
	}

	return nil
}

// WriteToConnOptimizedWithConnectionPool writes using connection pooling for high-volume scenarios
func (p *Packet) WriteToConnOptimizedWithConnectionPool(pool *network.ConnectionPool, addr string) error {
	// Get connection from pool
	conn, err := pool.Get(addr)
	if err != nil {
		return err
	}
	defer pool.Put(addr, conn)

	// Optimize TCP settings
	if err := network.OptimizeTCPConn(conn); err != nil {
		log.LogWarnf("Failed to optimize TCP connection: %v", err)
	}

	// Use optimized writer
	return p.WriteToConnOptimized(conn)
}

// WriteToConnOptimizedWithBatching writes with write batching for high-frequency operations
func (p *Packet) WriteToConnOptimizedWithBatching(c net.Conn, batcher *network.WriteBatcher) error {
	// Prepare header
	headSize := p.CalcPacketHeaderSize()
	header, err := Buffers.Get(headSize)
	if err != nil {
		header = make([]byte, headSize)
	}
	defer Buffers.Put(header)
	p.MarshalHeader(header)

	// Add header to batch
	if err := batcher.Write(header); err != nil {
		return err
	}

	// Add args to batch
	args := p.Arg[:int(p.ArgLen)]
	if err := batcher.Write(args); err != nil {
		return err
	}

	// Add data to batch (if present)
	if p.Data != nil && p.Size != 0 {
		data := p.Data[:p.Size]
		if err := batcher.Write(data); err != nil {
			return err
		}
	}

	// Handle version list if present
	if p.IsVersionList() {
		versionData, err1 := p.MarshalVersionSlice()
		if err1 != nil {
			log.LogErrorf("MarshalVersionSlice: marshal version info failed, err %s", err1.Error())
			return err1
		}

		// Add version data to batch
		if err := batcher.Write(versionData); err != nil {
			return err
		}
	}

	return nil
}
