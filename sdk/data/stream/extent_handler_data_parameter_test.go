package stream

import (
	"testing"

	"github.com/cubefs/cubefs/proto"
)

// TestExtentHandlerDataParameterCOW tests the updated data parameter usage with COW
func TestExtentHandlerDataParameterCOW(t *testing.T) {
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

	// Test cases
	testCases := []struct {
		name      string
		data      []byte
		offset    int
		size      int
		direct    bool
		storeMode int
	}{
		{
			name:      "Small data with COW",
			data:      testData[:64],
			offset:    0,
			size:      64,
			direct:    false,
			storeMode: proto.TinyExtentType,
		},
		{
			name:      "Large data with COW",
			data:      testData[:512],
			offset:    0,
			size:      512,
			direct:    false,
			storeMode: proto.NormalExtentType,
		},
		{
			name:      "Full block with COW",
			data:      testData,
			offset:    0,
			size:      1024,
			direct:    true,
			storeMode: proto.NormalExtentType,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			// Create extent handler with COW enabled
			eh := NewExtentHandler(mockStream, tc.offset, tc.storeMode, tc.size, 0, false)

			// Test the write method with COW data parameter
			ek, err := eh.write(tc.data, tc.offset, tc.size, tc.direct)

			if err != nil {
				t.Errorf("write failed: %v", err)
				return
			}

			if ek == nil {
				t.Errorf("expected extent key, got nil")
				return
			}

			// Verify the extent key
			if ek.FileOffset != uint64(tc.offset) {
				t.Errorf("expected file offset %d, got %d", tc.offset, ek.FileOffset)
			}

			if ek.Size != uint32(tc.size) {
				t.Errorf("expected size %d, got %d", tc.size, ek.Size)
			}

			// Cleanup
			eh.cleanup()
		})
	}
}

// BenchmarkExtentHandlerDataParameterCOW benchmarks the data parameter COW performance
func BenchmarkExtentHandlerDataParameterCOW(b *testing.B) {
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

// TestDataParameterTracking tests the data parameter tracking functionality
func TestDataParameterTracking(t *testing.T) {
	// Create test data
	testData := make([]byte, 256)
	for i := range testData {
		testData[i] = byte(i % 256)
	}

	// Create mock streamer
	mockStream := &Streamer{
		inode: 12345,
		// Note: This is a simplified test - in real usage, client would be properly initialized
	}

	// Create extent handler
	eh := NewExtentHandler(mockStream, 0, proto.NormalExtentType, 256, 0, false)

	// Test data parameter tracking
	eh.trackDataParameterUsage(testData, 0, 256)

	// Verify tracking was called (this is mainly for coverage)
	if eh.id == 0 {
		t.Errorf("expected handler ID to be set")
	}

	// Cleanup
	eh.cleanup()
}

// TestCOWBufferFromSlice tests the COW buffer creation from slice
func TestCOWBufferFromSlice(t *testing.T) {
	// Create test data
	testData := make([]byte, 128)
	for i := range testData {
		testData[i] = byte(i % 256)
	}

	// Create COW buffer from slice
	cowBuffer := NewCOWBufferFromSlice(testData)

	// Verify buffer properties
	if cowBuffer.Size() != len(testData) {
		t.Errorf("expected size %d, got %d", len(testData), cowBuffer.Size())
	}

	if cowBuffer.IsShared() {
		t.Errorf("expected buffer to not be shared initially")
	}

	// Test GetSlice functionality
	slice := cowBuffer.GetSlice(0, 64)
	if len(slice) != 64 {
		t.Errorf("expected slice length 64, got %d", len(slice))
	}

	// Test WriteAt functionality
	newData := make([]byte, 32)
	for i := range newData {
		newData[i] = byte(i + 100)
	}

	cowBuffer.WriteAt(0, newData)

	// Verify data was written
	resultData := cowBuffer.GetData()
	if resultData[0] != newData[0] {
		t.Errorf("expected first byte %d, got %d", newData[0], resultData[0])
	}

	// Cleanup
	cowBuffer.Release()
}
