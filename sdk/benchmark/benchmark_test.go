// Copyright 2018 The CubeFS Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License.

package main

import (
	"encoding/json"
	"os"
	"testing"
	"time"
)

func TestBenchmarkConfig(t *testing.T) {
	config := &BenchmarkConfig{
		VolumeName:   "test-volume",
		Owner:        "test-owner",
		Masters:      "192.168.1.100:17010",
		SubDir:       "/test",
		NumFiles:     100,
		FileSize:     1024,
		BlockSize:    4096,
		NumThreads:   4,
		Duration:     30,
		TestType:     "create",
		OutputFile:   "test-results.json",
		Verbose:      false,
		RandomData:   true,
		VerifyData:   false,
		CleanupFiles: true,
		FilePrefix:   "test",
		StorageClass: 0,
	}

	// Test JSON marshaling
	data, err := json.Marshal(config)
	if err != nil {
		t.Fatalf("Failed to marshal config: %v", err)
	}

	// Test JSON unmarshaling
	var newConfig BenchmarkConfig
	err = json.Unmarshal(data, &newConfig)
	if err != nil {
		t.Fatalf("Failed to unmarshal config: %v", err)
	}

	// Verify fields
	if newConfig.VolumeName != config.VolumeName {
		t.Errorf("VolumeName mismatch: got %s, want %s", newConfig.VolumeName, config.VolumeName)
	}
	if newConfig.NumFiles != config.NumFiles {
		t.Errorf("NumFiles mismatch: got %d, want %d", newConfig.NumFiles, config.NumFiles)
	}
}

func TestBenchmarkResult(t *testing.T) {
	result := &BenchmarkResult{
		TestType:     "create",
		StartTime:    time.Now(),
		EndTime:      time.Now().Add(time.Second * 10),
		Duration:     time.Second * 10,
		TotalFiles:   100,
		TotalBytes:   1024000,
		TotalOps:     100,
		Throughput:   100.5,
		IOPS:         10.0,
		ErrorCount:   0,
		SuccessCount: 100,
		Latency: LatencyStats{
			Min:    time.Millisecond,
			Max:    time.Millisecond * 100,
			Mean:   time.Millisecond * 10,
			Median: time.Millisecond * 8,
			P95:    time.Millisecond * 50,
			P99:    time.Millisecond * 80,
		},
		ThreadResults: map[int]*ThreadResult{
			0: {
				ThreadID:     0,
				FilesCreated: 50,
				BytesWritten: 512000,
				BytesRead:    0,
				OpsCompleted: 50,
				Errors:       0,
				Latencies:    []time.Duration{time.Millisecond, time.Millisecond * 2},
			},
		},
	}

	// Test JSON marshaling
	data, err := json.Marshal(result)
	if err != nil {
		t.Fatalf("Failed to marshal result: %v", err)
	}

	// Test JSON unmarshaling
	var newResult BenchmarkResult
	err = json.Unmarshal(data, &newResult)
	if err != nil {
		t.Fatalf("Failed to unmarshal result: %v", err)
	}

	// Verify fields
	if newResult.TestType != result.TestType {
		t.Errorf("TestType mismatch: got %s, want %s", newResult.TestType, result.TestType)
	}
	if newResult.TotalFiles != result.TotalFiles {
		t.Errorf("TotalFiles mismatch: got %d, want %d", newResult.TotalFiles, result.TotalFiles)
	}
}

func TestLatencyStats(t *testing.T) {
	latencies := []time.Duration{
		time.Millisecond,
		time.Millisecond * 2,
		time.Millisecond * 3,
		time.Millisecond * 4,
		time.Millisecond * 5,
	}

	stats := calculateLatencyStats(latencies)

	if stats.Min != time.Millisecond {
		t.Errorf("Min latency mismatch: got %v, want %v", stats.Min, time.Millisecond)
	}
	if stats.Max != time.Millisecond*5 {
		t.Errorf("Max latency mismatch: got %v, want %v", stats.Max, time.Millisecond*5)
	}
	if stats.Mean != time.Millisecond*3 {
		t.Errorf("Mean latency mismatch: got %v, want %v", stats.Mean, time.Millisecond*3)
	}
	if stats.Median != time.Millisecond*3 {
		t.Errorf("Median latency mismatch: got %v, want %v", stats.Median, time.Millisecond*3)
	}
}

func TestFormatBytes(t *testing.T) {
	tests := []struct {
		bytes    int64
		expected string
	}{
		{1024, "1.0 KB"},
		{1024 * 1024, "1.0 MB"},
		{1024 * 1024 * 1024, "1.0 GB"},
		{512, "512 B"},
		{1536, "1.5 KB"},
	}

	for _, test := range tests {
		result := formatBytes(test.bytes)
		if result != test.expected {
			t.Errorf("formatBytes(%d) = %s, want %s", test.bytes, result, test.expected)
		}
	}
}

func TestGenerateRandomData(t *testing.T) {
	size := 1024
	data := generateRandomData(size)

	if len(data) != size {
		t.Errorf("Generated data size mismatch: got %d, want %d", len(data), size)
	}

	// Check that data is not all zeros (basic randomness check)
	allZero := true
	for _, b := range data {
		if b != 0 {
			allZero = false
			break
		}
	}
	if allZero {
		t.Error("Generated data appears to be all zeros")
	}
}

func TestLoadConfig(t *testing.T) {
	// Create a temporary config file
	config := &BenchmarkConfig{
		VolumeName: "test-volume",
		Owner:      "test-owner",
		Masters:    "192.168.1.100:17010",
		NumFiles:   100,
		FileSize:   1024,
		BlockSize:  4096,
		NumThreads: 4,
		Duration:   30,
		TestType:   "create",
	}

	// Write config to temporary file
	data, err := json.MarshalIndent(config, "", "  ")
	if err != nil {
		t.Fatalf("Failed to marshal config: %v", err)
	}

	tmpFile, err := os.CreateTemp("", "benchmark-config-*.json")
	if err != nil {
		t.Fatalf("Failed to create temp file: %v", err)
	}
	defer os.Remove(tmpFile.Name())

	_, err = tmpFile.Write(data)
	if err != nil {
		t.Fatalf("Failed to write config file: %v", err)
	}
	tmpFile.Close()

	// Load config from file
	loadedConfig, err := LoadConfig(tmpFile.Name())
	if err != nil {
		t.Fatalf("Failed to load config: %v", err)
	}

	// Verify loaded config
	if loadedConfig.VolumeName != config.VolumeName {
		t.Errorf("VolumeName mismatch: got %s, want %s", loadedConfig.VolumeName, config.VolumeName)
	}
	if loadedConfig.NumFiles != config.NumFiles {
		t.Errorf("NumFiles mismatch: got %d, want %d", loadedConfig.NumFiles, config.NumFiles)
	}
}

func TestSaveConfig(t *testing.T) {
	config := &BenchmarkConfig{
		VolumeName: "test-volume",
		Owner:      "test-owner",
		Masters:    "192.168.1.100:17010",
		NumFiles:   100,
		FileSize:   1024,
		BlockSize:  4096,
		NumThreads: 4,
		Duration:   30,
		TestType:   "create",
	}

	// Save config to temporary file
	tmpFile, err := os.CreateTemp("", "benchmark-config-*.json")
	if err != nil {
		t.Fatalf("Failed to create temp file: %v", err)
	}
	defer os.Remove(tmpFile.Name())
	tmpFile.Close()

	err = config.SaveConfig(tmpFile.Name())
	if err != nil {
		t.Fatalf("Failed to save config: %v", err)
	}

	// Load config back and verify
	loadedConfig, err := LoadConfig(tmpFile.Name())
	if err != nil {
		t.Fatalf("Failed to load saved config: %v", err)
	}

	if loadedConfig.VolumeName != config.VolumeName {
		t.Errorf("Saved VolumeName mismatch: got %s, want %s", loadedConfig.VolumeName, config.VolumeName)
	}
}

func TestBenchmarkConfigValidation(t *testing.T) {
	tests := []struct {
		name    string
		config  BenchmarkConfig
		isValid bool
	}{
		{
			name: "valid config",
			config: BenchmarkConfig{
				VolumeName: "test-volume",
				Masters:    "192.168.1.100:17010",
				Owner:      "test-owner",
				BlockSize:  4096,
			},
			isValid: true,
		},
		{
			name: "missing volume name",
			config: BenchmarkConfig{
				Masters:   "192.168.1.100:17010",
				Owner:     "test-owner",
				BlockSize: 4096,
			},
			isValid: false,
		},
		{
			name: "missing masters",
			config: BenchmarkConfig{
				VolumeName: "test-volume",
				Owner:      "test-owner",
				BlockSize:  4096,
			},
			isValid: false,
		},
		{
			name: "invalid block size too small",
			config: BenchmarkConfig{
				VolumeName: "test-volume",
				Masters:    "192.168.1.100:17010",
				Owner:      "test-owner",
				BlockSize:  256, // Too small
			},
			isValid: false,
		},
		{
			name: "invalid block size too large",
			config: BenchmarkConfig{
				VolumeName: "test-volume",
				Masters:    "192.168.1.100:17010",
				Owner:      "test-owner",
				BlockSize:  2 * 1024 * 1024, // Too large
			},
			isValid: false,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			// Simple validation logic
			isValid := test.config.VolumeName != "" &&
				test.config.Masters != "" &&
				test.config.BlockSize >= MinBlockSize &&
				test.config.BlockSize <= MaxBlockSize

			if isValid != test.isValid {
				t.Errorf("Validation result mismatch: got %v, want %v", isValid, test.isValid)
			}
		})
	}
}

func TestThreadResult(t *testing.T) {
	result := &ThreadResult{
		ThreadID:     1,
		FilesCreated: 100,
		BytesWritten: 1024000,
		BytesRead:    512000,
		OpsCompleted: 150,
		Errors:       2,
		Latencies:    []time.Duration{time.Millisecond, time.Millisecond * 2},
	}

	// Test JSON marshaling
	data, err := json.Marshal(result)
	if err != nil {
		t.Fatalf("Failed to marshal thread result: %v", err)
	}

	// Test JSON unmarshaling
	var newResult ThreadResult
	err = json.Unmarshal(data, &newResult)
	if err != nil {
		t.Fatalf("Failed to unmarshal thread result: %v", err)
	}

	// Verify fields
	if newResult.ThreadID != result.ThreadID {
		t.Errorf("ThreadID mismatch: got %d, want %d", newResult.ThreadID, result.ThreadID)
	}
	if newResult.FilesCreated != result.FilesCreated {
		t.Errorf("FilesCreated mismatch: got %d, want %d", newResult.FilesCreated, result.FilesCreated)
	}
	if newResult.BytesWritten != result.BytesWritten {
		t.Errorf("BytesWritten mismatch: got %d, want %d", newResult.BytesWritten, result.BytesWritten)
	}
}

func BenchmarkCalculateLatencyStats(b *testing.B) {
	latencies := make([]time.Duration, 1000)
	for i := range latencies {
		latencies[i] = time.Duration(i) * time.Microsecond
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		calculateLatencyStats(latencies)
	}
}

func BenchmarkFormatBytes(b *testing.B) {
	bytes := int64(1024 * 1024 * 1024) // 1GB

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		formatBytes(bytes)
	}
}

func BenchmarkGenerateRandomData(b *testing.B) {
	size := 4096

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		generateRandomData(size)
	}
}
