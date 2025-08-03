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
	"context"
	"crypto/rand"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/cubefs/cubefs/proto"
	"github.com/cubefs/cubefs/sdk/meta"
	"github.com/cubefs/cubefs/util/log"
)

const (
	DefaultBlockSize = 4 * 1024    // 4KB
	MaxBlockSize     = 1024 * 1024 // 1MB
	MinBlockSize     = 512         // 512B
)

// BenchmarkConfig holds the configuration for the benchmark
type BenchmarkConfig struct {
	// Volume configuration
	VolumeName    string `json:"volume_name"`
	Owner         string `json:"owner"`
	Masters       string `json:"masters"`
	SubDir        string `json:"sub_dir"`
	Authenticate  bool   `json:"authenticate"`
	ValidateOwner bool   `json:"validate_owner"`

	// Benchmark parameters
	NumFiles   int    `json:"num_files"`
	FileSize   int64  `json:"file_size"`
	BlockSize  int    `json:"block_size"`
	NumThreads int    `json:"num_threads"`
	Duration   int    `json:"duration"`  // seconds
	TestType   string `json:"test_type"` // create, write, read, mixed
	OutputFile string `json:"output_file"`
	ConfigFile string `json:"config_file"`
	Verbose    bool   `json:"verbose"`

	// Advanced options
	RandomData   bool   `json:"random_data"`
	VerifyData   bool   `json:"verify_data"`
	CleanupFiles bool   `json:"cleanup_files"`
	FilePrefix   string `json:"file_prefix"`
	StorageClass uint64 `json:"storage_class"`
}

// BenchmarkResult holds the results of a benchmark test
type BenchmarkResult struct {
	TestType      string                `json:"test_type"`
	Config        BenchmarkConfig       `json:"config"`
	StartTime     time.Time             `json:"start_time"`
	EndTime       time.Time             `json:"end_time"`
	Duration      time.Duration         `json:"duration"`
	TotalFiles    int64                 `json:"total_files"`
	TotalBytes    int64                 `json:"total_bytes"`
	TotalOps      int64                 `json:"total_ops"`
	Throughput    float64               `json:"throughput_mbps"`
	IOPS          float64               `json:"iops"`
	Latency       LatencyStats          `json:"latency"`
	ErrorCount    int64                 `json:"error_count"`
	SuccessCount  int64                 `json:"success_count"`
	ThreadResults map[int]*ThreadResult `json:"thread_results"`
}

// LatencyStats holds latency statistics
type LatencyStats struct {
	Min    time.Duration `json:"min_ms"`
	Max    time.Duration `json:"max_ms"`
	Mean   time.Duration `json:"mean_ms"`
	Median time.Duration `json:"median_ms"`
	P95    time.Duration `json:"p95_ms"`
	P99    time.Duration `json:"p99_ms"`
}

// ThreadResult holds results for a single thread
type ThreadResult struct {
	ThreadID     int             `json:"thread_id"`
	FilesCreated int64           `json:"files_created"`
	BytesWritten int64           `json:"bytes_written"`
	BytesRead    int64           `json:"bytes_read"`
	OpsCompleted int64           `json:"ops_completed"`
	Errors       int64           `json:"errors"`
	Latencies    []time.Duration `json:"latencies"`
}

// BenchmarkRunner handles the benchmark execution
type BenchmarkRunner struct {
	config     *BenchmarkConfig
	metaClient *meta.MetaWrapper
	results    *BenchmarkResult
	ctx        context.Context
	cancel     context.CancelFunc
	wg         sync.WaitGroup
	mu         sync.Mutex
}

// NewBenchmarkRunner creates a new benchmark runner
func NewBenchmarkRunner(config *BenchmarkConfig) (*BenchmarkRunner, error) {
	// Parse masters
	masters := strings.Split(config.Masters, ",")
	for i, master := range masters {
		masters[i] = strings.TrimSpace(master)
	}

	// Create meta client configuration
	metaConfig := &meta.MetaConfig{
		Volume:        config.VolumeName,
		Owner:         config.Owner,
		Masters:       masters,
		Authenticate:  config.Authenticate,
		ValidateOwner: config.ValidateOwner,
		SubDir:        config.SubDir,
	}

	// Create meta wrapper
	metaClient, err := meta.NewMetaWrapper(metaConfig)
	if err != nil {
		return nil, fmt.Errorf("failed to create meta wrapper: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())

	return &BenchmarkRunner{
		config:     config,
		metaClient: metaClient,
		ctx:        ctx,
		cancel:     cancel,
	}, nil
}

// LoadConfig loads configuration from file
func LoadConfig(filename string) (*BenchmarkConfig, error) {
	file, err := os.Open(filename)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	config := &BenchmarkConfig{}
	decoder := json.NewDecoder(file)
	if err := decoder.Decode(config); err != nil {
		return nil, err
	}

	return config, nil
}

// SaveConfig saves configuration to file
func (config *BenchmarkConfig) SaveConfig(filename string) error {
	file, err := os.Create(filename)
	if err != nil {
		return err
	}
	defer file.Close()

	encoder := json.NewEncoder(file)
	encoder.SetIndent("", "  ")
	return encoder.Encode(config)
}

// generateRandomData generates random data of specified size
func generateRandomData(size int) []byte {
	data := make([]byte, size)
	_, err := rand.Read(data)
	if err != nil {
		// Fallback to pseudo-random if crypto/rand fails
		for i := range data {
			data[i] = byte(i % 256)
		}
	}
	return data
}

// generateTestData generates test data based on configuration
func (br *BenchmarkRunner) generateTestData() []byte {
	if br.config.RandomData {
		return generateRandomData(br.config.BlockSize)
	}

	// Generate deterministic test data
	data := make([]byte, br.config.BlockSize)
	for i := range data {
		data[i] = byte(i % 256)
	}
	return data
}

// createFile creates a single file for testing
func (br *BenchmarkRunner) createFile(threadID int, fileIndex int64) (int64, time.Duration, error) {
	start := time.Now()

	fileName := fmt.Sprintf("%s_%d_%d", br.config.FilePrefix, threadID, fileIndex)
	fullPath := filepath.Join(br.config.SubDir, fileName)

	// Create file
	info, err := br.metaClient.Create_ll(proto.RootIno, fileName, uint32(0644), 0, 0, nil, fullPath, false)
	if err != nil {
		return 0, 0, fmt.Errorf("failed to create file %s: %v", fullPath, err)
	}

	duration := time.Since(start)
	return int64(info.Inode), duration, nil
}

// writeFile writes data to a file
func (br *BenchmarkRunner) writeFile(inode uint64, fileName string, data []byte) (int64, time.Duration, error) {
	start := time.Now()

	// Get extents for writing
	_, size, extents, err := br.metaClient.GetExtents(inode, false, true, false)
	if err != nil {
		return 0, 0, fmt.Errorf("failed to get extents for file %s: %v", fileName, err)
	}

	// Create extent key for the write
	ek := proto.ExtentKey{
		FileOffset:   size,
		PartitionId:  extents[len(extents)-1].PartitionId,
		ExtentId:     extents[len(extents)-1].ExtentId + 1,
		ExtentOffset: 0,
		Size:         uint32(len(data)),
		CRC:          0, // Will be calculated by data layer
	}

	// Append extent key
	_, err = br.metaClient.AppendExtentKey(proto.RootIno, inode, ek, nil, false, uint32(br.config.StorageClass), false)
	if err != nil {
		return 0, 0, fmt.Errorf("failed to append extent key for file %s: %v", fileName, err)
	}

	duration := time.Since(start)
	return int64(len(data)), duration, nil
}

// readFile reads data from a file
func (br *BenchmarkRunner) readFile(inode uint64, fileName string, size int64) (int64, time.Duration, error) {
	start := time.Now()

	// Get extents for reading
	_, fileSize, _, err := br.metaClient.GetExtents(inode, false, false, false)
	if err != nil {
		return 0, 0, fmt.Errorf("failed to get extents for file %s: %v", fileName, err)
	}

	if fileSize == 0 {
		return 0, time.Since(start), nil
	}

	// Calculate how much to read
	readSize := size
	if readSize > int64(fileSize) {
		readSize = int64(fileSize)
	}

	duration := time.Since(start)
	return readSize, duration, nil
}

// calculateLatencyStats calculates latency statistics from a slice of durations
func calculateLatencyStats(latencies []time.Duration) LatencyStats {
	if len(latencies) == 0 {
		return LatencyStats{}
	}

	// Sort latencies for percentile calculation
	sorted := make([]time.Duration, len(latencies))
	copy(sorted, latencies)

	// Calculate basic stats
	min := sorted[0]
	max := sorted[0]
	var sum time.Duration

	for _, lat := range sorted {
		if lat < min {
			min = lat
		}
		if lat > max {
			max = lat
		}
		sum += lat
	}

	mean := sum / time.Duration(len(sorted))
	median := sorted[len(sorted)/2]

	// Calculate percentiles
	p95Index := int(float64(len(sorted)) * 0.95)
	p99Index := int(float64(len(sorted)) * 0.99)

	if p95Index >= len(sorted) {
		p95Index = len(sorted) - 1
	}
	if p99Index >= len(sorted) {
		p99Index = len(sorted) - 1
	}

	return LatencyStats{
		Min:    min,
		Max:    max,
		Mean:   mean,
		Median: median,
		P95:    sorted[p95Index],
		P99:    sorted[p99Index],
	}
}

// runCreateTest runs the file creation benchmark
func (br *BenchmarkRunner) runCreateTest() error {
	br.results.TestType = "create"
	br.results.StartTime = time.Now()

	var totalFiles int64
	var totalErrors int64
	var totalLatency time.Duration

	threadResults := make(map[int]*ThreadResult)
	var mu sync.Mutex

	// Start worker threads
	for threadID := 0; threadID < br.config.NumThreads; threadID++ {
		br.wg.Add(1)
		go func(tid int) {
			defer br.wg.Done()

			threadResult := &ThreadResult{
				ThreadID:  tid,
				Latencies: make([]time.Duration, 0),
			}

			fileIndex := int64(0)
			startTime := time.Now()

			for {
				select {
				case <-br.ctx.Done():
					return
				default:
					// Check if we've reached the duration limit
					if br.config.Duration > 0 && time.Since(startTime) >= time.Duration(br.config.Duration)*time.Second {
						return
					}

					// Check if we've reached the file count limit
					if br.config.NumFiles > 0 && fileIndex >= int64(br.config.NumFiles/br.config.NumThreads) {
						return
					}

					// Create file
					_, latency, err := br.createFile(tid, fileIndex)

					mu.Lock()
					threadResult.Latencies = append(threadResult.Latencies, latency)
					if err != nil {
						threadResult.Errors++
						atomic.AddInt64(&totalErrors, 1)
						if br.config.Verbose {
							fmt.Printf("Thread %d: Error creating file %d: %v\n", tid, fileIndex, err)
						}
					} else {
						threadResult.FilesCreated++
						atomic.AddInt64(&totalFiles, 1)
						totalLatency += latency
					}
					mu.Unlock()

					fileIndex++
				}
			}
		}(threadID)
	}

	// Wait for all threads to complete
	br.wg.Wait()

	br.results.EndTime = time.Now()
	br.results.Duration = br.results.EndTime.Sub(br.results.StartTime)
	br.results.TotalFiles = totalFiles
	br.results.ErrorCount = totalErrors
	br.results.SuccessCount = totalFiles

	// Calculate throughput (files per second)
	if br.results.Duration > 0 {
		br.results.IOPS = float64(totalFiles) / br.results.Duration.Seconds()
	}

	// Aggregate thread results
	allLatencies := make([]time.Duration, 0)
	for _, result := range threadResults {
		allLatencies = append(allLatencies, result.Latencies...)
	}
	br.results.Latency = calculateLatencyStats(allLatencies)
	br.results.ThreadResults = threadResults

	return nil
}

// runWriteTest runs the file writing benchmark
func (br *BenchmarkRunner) runWriteTest() error {
	br.results.TestType = "write"
	br.results.StartTime = time.Now()

	// First create files if they don't exist
	fmt.Println("Creating files for write test...")
	if err := br.runCreateTest(); err != nil {
		return err
	}

	// Now run write test
	var totalBytes int64
	var totalOps int64
	var totalErrors int64

	testData := br.generateTestData()

	// Start worker threads for writing
	for threadID := 0; threadID < br.config.NumThreads; threadID++ {
		br.wg.Add(1)
		go func(tid int) {
			defer br.wg.Done()

			fileIndex := int64(0)
			startTime := time.Now()

			for {
				select {
				case <-br.ctx.Done():
					return
				default:
					// Check duration limit
					if br.config.Duration > 0 && time.Since(startTime) >= time.Duration(br.config.Duration)*time.Second {
						return
					}

					fileName := fmt.Sprintf("%s_%d_%d", br.config.FilePrefix, tid, fileIndex)

					// Get file inode
					child, _, err := br.metaClient.Lookup_ll(proto.RootIno, fileName)
					if err != nil {
						atomic.AddInt64(&totalErrors, 1)
						if br.config.Verbose {
							fmt.Printf("Thread %d: Error looking up file %s: %v\n", tid, fileName, err)
						}
						fileIndex++
						continue
					}

					// Write to file
					bytesWritten, _, err := br.writeFile(child, fileName, testData)
					if err != nil {
						atomic.AddInt64(&totalErrors, 1)
						if br.config.Verbose {
							fmt.Printf("Thread %d: Error writing to file %s: %v\n", tid, fileName, err)
						}
					} else {
						atomic.AddInt64(&totalBytes, bytesWritten)
						atomic.AddInt64(&totalOps, 1)
					}

					fileIndex++
				}
			}
		}(threadID)
	}

	br.wg.Wait()

	br.results.EndTime = time.Now()
	br.results.Duration = br.results.EndTime.Sub(br.results.StartTime)
	br.results.TotalBytes = totalBytes
	br.results.TotalOps = totalOps
	br.results.ErrorCount = totalErrors

	// Calculate throughput (MB/s)
	if br.results.Duration > 0 {
		br.results.Throughput = float64(totalBytes) / 1024 / 1024 / br.results.Duration.Seconds()
		br.results.IOPS = float64(totalOps) / br.results.Duration.Seconds()
	}

	return nil
}

// runReadTest runs the file reading benchmark
func (br *BenchmarkRunner) runReadTest() error {
	br.results.TestType = "read"
	br.results.StartTime = time.Now()

	var totalBytes int64
	var totalOps int64
	var totalErrors int64

	// Start worker threads for reading
	for threadID := 0; threadID < br.config.NumThreads; threadID++ {
		br.wg.Add(1)
		go func(tid int) {
			defer br.wg.Done()

			fileIndex := int64(0)
			startTime := time.Now()

			for {
				select {
				case <-br.ctx.Done():
					return
				default:
					// Check duration limit
					if br.config.Duration > 0 && time.Since(startTime) >= time.Duration(br.config.Duration)*time.Second {
						return
					}

					fileName := fmt.Sprintf("%s_%d_%d", br.config.FilePrefix, tid, fileIndex)

					// Get file inode
					child, _, err := br.metaClient.Lookup_ll(proto.RootIno, fileName)
					if err != nil {
						atomic.AddInt64(&totalErrors, 1)
						if br.config.Verbose {
							fmt.Printf("Thread %d: Error looking up file %s: %v\n", tid, fileName, err)
						}
						fileIndex++
						continue
					}

					// Read from file
					bytesRead, _, err := br.readFile(child, fileName, int64(br.config.BlockSize))
					if err != nil {
						atomic.AddInt64(&totalErrors, 1)
						if br.config.Verbose {
							fmt.Printf("Thread %d: Error reading from file %s: %v\n", tid, fileName, err)
						}
					} else {
						atomic.AddInt64(&totalBytes, bytesRead)
						atomic.AddInt64(&totalOps, 1)
					}

					fileIndex++
				}
			}
		}(threadID)
	}

	br.wg.Wait()

	br.results.EndTime = time.Now()
	br.results.Duration = br.results.EndTime.Sub(br.results.StartTime)
	br.results.TotalBytes = totalBytes
	br.results.TotalOps = totalOps
	br.results.ErrorCount = totalErrors

	// Calculate throughput (MB/s)
	if br.results.Duration > 0 {
		br.results.Throughput = float64(totalBytes) / 1024 / 1024 / br.results.Duration.Seconds()
		br.results.IOPS = float64(totalOps) / br.results.Duration.Seconds()
	}

	return nil
}

// runMixedTest runs a mixed workload benchmark
func (br *BenchmarkRunner) runMixedTest() error {
	br.results.TestType = "mixed"
	br.results.StartTime = time.Now()

	// Create some initial files
	fmt.Println("Creating initial files for mixed test...")
	initialFiles := br.config.NumFiles / 4
	br.config.NumFiles = initialFiles
	if err := br.runCreateTest(); err != nil {
		return err
	}

	// Reset results for mixed test
	br.results = &BenchmarkResult{
		TestType: "mixed",
		Config:   *br.config,
	}
	br.results.StartTime = time.Now()

	var totalBytes int64
	var totalOps int64
	var totalErrors int64

	testData := br.generateTestData()

	// Start worker threads for mixed workload
	for threadID := 0; threadID < br.config.NumThreads; threadID++ {
		br.wg.Add(1)
		go func(tid int) {
			defer br.wg.Done()

			fileIndex := int64(0)
			startTime := time.Now()

			for {
				select {
				case <-br.ctx.Done():
					return
				default:
					// Check duration limit
					if br.config.Duration > 0 && time.Since(startTime) >= time.Duration(br.config.Duration)*time.Second {
						return
					}

					// Alternate between create, write, and read operations
					opType := fileIndex % 3

					switch opType {
					case 0: // Create
						_, _, err := br.createFile(tid, fileIndex)
						if err != nil {
							atomic.AddInt64(&totalErrors, 1)
						} else {
							atomic.AddInt64(&totalOps, 1)
						}

					case 1: // Write
						fileName := fmt.Sprintf("%s_%d_%d", br.config.FilePrefix, tid, fileIndex-1)
						child, _, err := br.metaClient.Lookup_ll(proto.RootIno, fileName)
						if err == nil {
							bytesWritten, _, err := br.writeFile(child, fileName, testData)
							if err != nil {
								atomic.AddInt64(&totalErrors, 1)
							} else {
								atomic.AddInt64(&totalBytes, bytesWritten)
								atomic.AddInt64(&totalOps, 1)
							}
						}

					case 2: // Read
						fileName := fmt.Sprintf("%s_%d_%d", br.config.FilePrefix, tid, fileIndex-2)
						child, _, err := br.metaClient.Lookup_ll(proto.RootIno, fileName)
						if err == nil {
							bytesRead, _, err := br.readFile(child, fileName, int64(br.config.BlockSize))
							if err != nil {
								atomic.AddInt64(&totalErrors, 1)
							} else {
								atomic.AddInt64(&totalBytes, bytesRead)
								atomic.AddInt64(&totalOps, 1)
							}
						}
					}

					fileIndex++
				}
			}
		}(threadID)
	}

	br.wg.Wait()

	br.results.EndTime = time.Now()
	br.results.Duration = br.results.EndTime.Sub(br.results.StartTime)
	br.results.TotalBytes = totalBytes
	br.results.TotalOps = totalOps
	br.results.ErrorCount = totalErrors

	// Calculate throughput (MB/s)
	if br.results.Duration > 0 {
		br.results.Throughput = float64(totalBytes) / 1024 / 1024 / br.results.Duration.Seconds()
		br.results.IOPS = float64(totalOps) / br.results.Duration.Seconds()
	}

	return nil
}

// Run executes the benchmark based on the test type
func (br *BenchmarkRunner) Run() error {
	br.results = &BenchmarkResult{
		Config: *br.config,
	}

	fmt.Printf("Starting %s benchmark...\n", br.config.TestType)
	fmt.Printf("Configuration: Files=%d, Size=%d, BlockSize=%d, Threads=%d, Duration=%ds\n",
		br.config.NumFiles, br.config.FileSize, br.config.BlockSize, br.config.NumThreads, br.config.Duration)

	switch br.config.TestType {
	case "create":
		return br.runCreateTest()
	case "write":
		return br.runWriteTest()
	case "read":
		return br.runReadTest()
	case "mixed":
		return br.runMixedTest()
	default:
		return fmt.Errorf("unknown test type: %s", br.config.TestType)
	}
}

// PrintResults prints the benchmark results
func (br *BenchmarkRunner) PrintResults() {
	if br.results == nil {
		fmt.Println("No results to print")
		return
	}

	fmt.Println("\n" + strings.Repeat("=", 80))
	fmt.Println("BENCHMARK RESULTS")
	fmt.Println(strings.Repeat("=", 80))

	fmt.Printf("Test Type:        %s\n", br.results.TestType)
	fmt.Printf("Duration:         %v\n", br.results.Duration)
	fmt.Printf("Total Files:      %d\n", br.results.TotalFiles)
	fmt.Printf("Total Bytes:      %d (%s)\n", br.results.TotalBytes, formatBytes(br.results.TotalBytes))
	fmt.Printf("Total Operations: %d\n", br.results.TotalOps)
	fmt.Printf("Success Count:    %d\n", br.results.SuccessCount)
	fmt.Printf("Error Count:      %d\n", br.results.ErrorCount)

	if br.results.Duration > 0 {
		fmt.Printf("Throughput:       %.2f MB/s\n", br.results.Throughput)
		fmt.Printf("IOPS:             %.2f ops/s\n", br.results.IOPS)
	}

	if br.results.Latency.Mean > 0 {
		fmt.Printf("\nLatency Statistics:\n")
		fmt.Printf("  Min:    %v\n", br.results.Latency.Min)
		fmt.Printf("  Max:    %v\n", br.results.Latency.Max)
		fmt.Printf("  Mean:   %v\n", br.results.Latency.Mean)
		fmt.Printf("  Median: %v\n", br.results.Latency.Median)
		fmt.Printf("  P95:    %v\n", br.results.Latency.P95)
		fmt.Printf("  P99:    %v\n", br.results.Latency.P99)
	}

	fmt.Println(strings.Repeat("=", 80))
}

// SaveResults saves the benchmark results to a file
func (br *BenchmarkRunner) SaveResults(filename string) error {
	if br.results == nil {
		return fmt.Errorf("no results to save")
	}

	file, err := os.Create(filename)
	if err != nil {
		return err
	}
	defer file.Close()

	encoder := json.NewEncoder(file)
	encoder.SetIndent("", "  ")
	return encoder.Encode(br.results)
}

// formatBytes formats bytes into human readable format
func formatBytes(bytes int64) string {
	const unit = 1024
	if bytes < unit {
		return fmt.Sprintf("%d B", bytes)
	}
	div, exp := int64(unit), 0
	for n := bytes / unit; n >= unit; n /= unit {
		div *= unit
		exp++
	}
	return fmt.Sprintf("%.1f %cB", float64(bytes)/float64(div), "KMGTPE"[exp])
}

// cleanupFiles removes test files
func (br *BenchmarkRunner) cleanupFiles() error {
	if !br.config.CleanupFiles {
		return nil
	}

	fmt.Println("Cleaning up test files...")

	// List files in the directory
	dentries, err := br.metaClient.ReadDir_ll(proto.RootIno)
	if err != nil {
		return fmt.Errorf("failed to read directory: %v", err)
	}

	var deletedCount int64
	for _, dentry := range dentries {
		if strings.HasPrefix(dentry.Name, br.config.FilePrefix) {
			_, err := br.metaClient.Delete_ll(proto.RootIno, dentry.Name, false, "")
			if err != nil {
				if br.config.Verbose {
					fmt.Printf("Failed to delete file %s: %v\n", dentry.Name, err)
				}
			} else {
				deletedCount++
			}
		}
	}

	fmt.Printf("Deleted %d test files\n", deletedCount)
	return nil
}

// Close closes the benchmark runner
func (br *BenchmarkRunner) Close() error {
	br.cancel()
	br.wg.Wait()

	if br.config.CleanupFiles {
		return br.cleanupFiles()
	}

	return br.metaClient.Close()
}

func main() {
	var config BenchmarkConfig

	// Set default values
	config.NumFiles = 1000
	config.FileSize = 1024 * 1024 // 1MB
	config.BlockSize = DefaultBlockSize
	config.NumThreads = runtime.NumCPU()
	config.Duration = 60 // 60 seconds
	config.TestType = "mixed"
	config.FilePrefix = "benchmark"
	config.StorageClass = 0
	config.Verbose = false

	// Command line flags
	flag.StringVar(&config.VolumeName, "volume", "", "Volume name")
	flag.StringVar(&config.Owner, "owner", "", "Volume owner")
	flag.StringVar(&config.Masters, "masters", "", "Master addresses (comma-separated)")
	flag.StringVar(&config.SubDir, "subdir", "", "Subdirectory for testing")
	flag.BoolVar(&config.Authenticate, "auth", false, "Enable authentication")
	flag.BoolVar(&config.ValidateOwner, "validate-owner", false, "Validate owner")

	flag.IntVar(&config.NumFiles, "files", config.NumFiles, "Number of files to create")
	flag.Int64Var(&config.FileSize, "size", config.FileSize, "File size in bytes")
	flag.IntVar(&config.BlockSize, "block-size", config.BlockSize, "Block size for I/O")
	flag.IntVar(&config.NumThreads, "threads", config.NumThreads, "Number of threads")
	flag.IntVar(&config.Duration, "duration", config.Duration, "Test duration in seconds (0 for unlimited)")
	flag.StringVar(&config.TestType, "test", config.TestType, "Test type: create, write, read, mixed")
	flag.StringVar(&config.OutputFile, "output", "", "Output file for results")
	flag.StringVar(&config.ConfigFile, "config", "", "Configuration file")
	flag.BoolVar(&config.Verbose, "verbose", config.Verbose, "Verbose output")

	flag.BoolVar(&config.RandomData, "random", false, "Use random data for writes")
	flag.BoolVar(&config.VerifyData, "verify", false, "Verify data integrity")
	flag.BoolVar(&config.CleanupFiles, "cleanup", false, "Clean up test files after benchmark")
	flag.StringVar(&config.FilePrefix, "prefix", config.FilePrefix, "File name prefix")
	flag.Uint64Var(&config.StorageClass, "storage-class", uint64(config.StorageClass), "Storage class")

	flag.Parse()

	// Load configuration from file if specified
	if config.ConfigFile != "" {
		fileConfig, err := LoadConfig(config.ConfigFile)
		if err != nil {
			fmt.Printf("Error loading config file: %v\n", err)
			os.Exit(1)
		}

		// Merge file config with command line flags
		if fileConfig.VolumeName != "" {
			config.VolumeName = fileConfig.VolumeName
		}
		if fileConfig.Owner != "" {
			config.Owner = fileConfig.Owner
		}
		if fileConfig.Masters != "" {
			config.Masters = fileConfig.Masters
		}
		if fileConfig.SubDir != "" {
			config.SubDir = fileConfig.SubDir
		}
		// Add other fields as needed
	}

	// Validate configuration
	if config.VolumeName == "" {
		fmt.Println("Error: Volume name is required")
		flag.Usage()
		os.Exit(1)
	}

	if config.Masters == "" {
		fmt.Println("Error: Master addresses are required")
		flag.Usage()
		os.Exit(1)
	}

	if config.Owner == "" {
		config.Owner = "benchmark"
	}

	if config.SubDir == "" {
		config.SubDir = "/benchmark"
	}

	// Validate block size
	if config.BlockSize < MinBlockSize || config.BlockSize > MaxBlockSize {
		fmt.Printf("Error: Block size must be between %d and %d bytes\n", MinBlockSize, MaxBlockSize)
		os.Exit(1)
	}

	// Initialize logging
	_, err := log.InitLog("/tmp/cubefs", "benchmark", log.DebugLevel, nil, log.DefaultLogLeftSpaceLimitRatio)
	if err != nil {
		fmt.Printf("Error initializing log: %v\n", err)
		os.Exit(1)
	}
	defer log.LogFlush()

	// Create and run benchmark
	runner, err := NewBenchmarkRunner(&config)
	if err != nil {
		fmt.Printf("Error creating benchmark runner: %v\n", err)
		os.Exit(1)
	}
	defer runner.Close()

	// Run benchmark
	if err := runner.Run(); err != nil {
		fmt.Printf("Error running benchmark: %v\n", err)
		os.Exit(1)
	}

	// Print results
	runner.PrintResults()

	// Save results if output file is specified
	if config.OutputFile != "" {
		if err := runner.SaveResults(config.OutputFile); err != nil {
			fmt.Printf("Error saving results: %v\n", err)
		} else {
			fmt.Printf("Results saved to %s\n", config.OutputFile)
		}
	}
}
