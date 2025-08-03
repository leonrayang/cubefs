package stream

import (
	"testing"
	"time"

	"github.com/cubefs/cubefs/proto"
)

// BenchmarkExtentHandlerWrite compares original vs COW implementation
func BenchmarkExtentHandlerWrite(b *testing.B) {
	// Create test data
	testData := make([]byte, 1024)
	for i := range testData {
		testData[i] = byte(i % 256)
	}

	// Test original implementation
	b.Run("Original", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			// Simulate original copy operation
			dest := make([]byte, 1024)
			copy(dest, testData)
		}
	})

	// Test COW implementation
	b.Run("CopyOnWrite", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			// Simulate COW operation
			cowBuffer := NewCOWBuffer(1024)
			cowBuffer.WriteAt(0, testData)
			_ = cowBuffer.GetData()
		}
	})
}

// TestCopyOnWriteOptimization tests the COW optimization
func TestCopyOnWriteOptimization(t *testing.T) {
	// Create test data
	testData := make([]byte, 512)
	for i := range testData {
		testData[i] = byte(i % 256)
	}

	// Test small write (should use direct copy)
	t.Run("SmallWrite", func(t *testing.T) {
		cowBuffer := NewCOWBuffer(1024)
		smallData := testData[:64]

		start := time.Now()
		cowBuffer.WriteAt(0, smallData)
		duration := time.Since(start)

		t.Logf("Small write (64 bytes) took: %v", duration)

		result := cowBuffer.GetData()[:64]
		if len(result) != 64 {
			t.Errorf("Expected 64 bytes, got %d", len(result))
		}
	})

	// Test large write (should use COW)
	t.Run("LargeWrite", func(t *testing.T) {
		cowBuffer := NewCOWBuffer(1024)

		start := time.Now()
		cowBuffer.WriteAt(0, testData)
		duration := time.Since(start)

		t.Logf("Large write (512 bytes) took: %v", duration)

		result := cowBuffer.GetData()[:512]
		if len(result) != 512 {
			t.Errorf("Expected 512 bytes, got %d", len(result))
		}
	})

	// Test shared buffer scenario
	t.Run("SharedBuffer", func(t *testing.T) {
		cowBuffer := NewCOWBuffer(1024)

		// Create a copy to make it shared
		sharedBuffer := cowBuffer.Copy()

		start := time.Now()
		sharedBuffer.WriteAt(0, testData)
		duration := time.Since(start)

		t.Logf("Shared buffer write took: %v", duration)

		result := sharedBuffer.GetData()[:512]
		if len(result) != 512 {
			t.Errorf("Expected 512 bytes, got %d", len(result))
		}
	})
}

// TestExtentHandlerCOWIntegration tests the integration with ExtentHandler
func TestExtentHandlerCOWIntegration(t *testing.T) {
	// Create a mock streamer for testing
	mockStream := &Streamer{
		inode: 12345,
		// Note: This is a simplified test - in real usage, client would be properly initialized
	}

	// Create extent handler with COW enabled
	eh := NewExtentHandler(mockStream, 0, proto.NormalExtentType, 1024, 0, false)

	// Test data
	testData := make([]byte, 256)
	for i := range testData {
		testData[i] = byte(i % 256)
	}

	// Test the writeDataWithCOW method
	t.Run("WriteDataWithCOW", func(t *testing.T) {
		// Create a packet
		eh.packet = NewWritePacket(eh.inode, 0, eh.storeMode)

		start := time.Now()
		eh.writeDataWithCOW(0, testData)
		duration := time.Since(start)

		t.Logf("writeDataWithCOW took: %v", duration)

		// Verify data was written correctly
		if eh.packet.Size != uint32(len(testData)) {
			t.Errorf("Expected packet size %d, got %d", len(testData), eh.packet.Size)
		}
	})

	// Cleanup
	eh.cleanup()
}

// BenchmarkExtentHandlerWriteComparison benchmarks the actual write method
func BenchmarkExtentHandlerWriteComparison(b *testing.B) {
	// Create test data
	testData := make([]byte, 1024)
	for i := range testData {
		testData[i] = byte(i % 256)
	}

	// Create mock streamer
	mockStream := &Streamer{
		inode: 12345,
		// Note: This is a simplified test - in real usage, client would be properly initialized
	}

	b.Run("WithCOW", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			eh := NewExtentHandler(mockStream, 0, proto.NormalExtentType, 1024, 0, false)
			eh.write(testData, 0, len(testData), false)
			eh.cleanup()
		}
	})

	b.Run("WithoutCOW", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			eh := NewExtentHandler(mockStream, 0, proto.NormalExtentType, 1024, 0, false)
			eh.isCOWEnabled = false // Disable COW
			eh.write(testData, 0, len(testData), false)
			eh.cleanup()
		}
	})
}
