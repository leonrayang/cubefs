# CubeFS SDK Benchmark Tool

A comprehensive benchmark tool for CubeFS that tests the real performance of the system without using FUSE. This tool directly uses the CubeFS SDK to create, write, and read multiple files, providing accurate performance metrics.

## Features

- **Direct SDK Access**: Uses CubeFS SDK directly without FUSE overhead
- **Multiple Test Types**: Supports create, write, read, and mixed workload tests
- **Configurable Parameters**: Flexible configuration for file count, size, block size, threads, etc.
- **Real-time Metrics**: Provides throughput, IOPS, and latency statistics
- **JSON Output**: Saves detailed results in JSON format for analysis
- **Multi-threaded**: Supports concurrent operations across multiple threads
- **Data Verification**: Optional data integrity verification
- **Cleanup**: Automatic cleanup of test files

## Installation

The benchmark tool is part of the CubeFS SDK. To build it:

```bash
cd sdk/benchmark
go build -o cubefs-benchmark benchmark.go
```

## Usage

### Command Line Options

```bash
./cubefs-benchmark [options]
```

#### Required Options
- `-volume`: Volume name to test
- `-masters`: Master addresses (comma-separated)

#### Optional Options
- `-owner`: Volume owner (default: "benchmark")
- `-subdir`: Subdirectory for testing (default: "/benchmark")
- `-auth`: Enable authentication (default: false)
- `-validate-owner`: Validate owner (default: false)

#### Benchmark Parameters
- `-files`: Number of files to create (default: 1000)
- `-size`: File size in bytes (default: 1MB)
- `-block-size`: Block size for I/O (default: 4KB)
- `-threads`: Number of threads (default: CPU count)
- `-duration`: Test duration in seconds, 0 for unlimited (default: 60)
- `-test`: Test type: create, write, read, mixed (default: mixed)

#### Output Options
- `-output`: Output file for results
- `-config`: Configuration file
- `-verbose`: Verbose output

#### Advanced Options
- `-random`: Use random data for writes
- `-verify`: Verify data integrity
- `-cleanup`: Clean up test files after benchmark
- `-prefix`: File name prefix (default: "benchmark")
- `-storage-class`: Storage class (default: 0)

### Configuration File

You can use a JSON configuration file instead of command line options:

```bash
./cubefs-benchmark -config config.json
```

Example configuration file (`config-example.json`):

```json
{
  "volume_name": "test-volume",
  "owner": "benchmark",
  "masters": "192.168.1.100:17010,192.168.1.101:17010,192.168.1.102:17010",
  "sub_dir": "/benchmark",
  "authenticate": false,
  "validate_owner": false,
  "num_files": 1000,
  "file_size": 1048576,
  "block_size": 4096,
  "num_threads": 8,
  "duration": 60,
  "test_type": "mixed",
  "output_file": "benchmark-results.json",
  "verbose": false,
  "random_data": true,
  "verify_data": false,
  "cleanup_files": true,
  "file_prefix": "benchmark",
  "storage_class": 0
}
```

## Test Types

### 1. Create Test
Tests file creation performance:
```bash
./cubefs-benchmark -volume test-vol -masters "192.168.1.100:17010" -test create -files 10000 -threads 16
```

### 2. Write Test
Tests file writing performance:
```bash
./cubefs-benchmark -volume test-vol -masters "192.168.1.100:17010" -test write -files 1000 -size 1048576 -threads 8
```

### 3. Read Test
Tests file reading performance:
```bash
./cubefs-benchmark -volume test-vol -masters "192.168.1.100:17010" -test read -files 1000 -threads 8
```

### 4. Mixed Test
Tests mixed workload (create, write, read):
```bash
./cubefs-benchmark -volume test-vol -masters "192.168.1.100:17010" -test mixed -files 1000 -threads 8 -duration 120
```

## Examples

### Basic Performance Test
```bash
./cubefs-benchmark \
  -volume my-volume \
  -masters "192.168.1.100:17010,192.168.1.101:17010" \
  -files 1000 \
  -size 1048576 \
  -threads 8 \
  -duration 60 \
  -test mixed \
  -output results.json
```

### High-Throughput Test
```bash
./cubefs-benchmark \
  -volume my-volume \
  -masters "192.168.1.100:17010" \
  -files 10000 \
  -size 10485760 \
  -block-size 65536 \
  -threads 32 \
  -duration 300 \
  -test write \
  -random \
  -output high-throughput.json
```

### Small File Test
```bash
./cubefs-benchmark \
  -volume my-volume \
  -masters "192.168.1.100:17010" \
  -files 100000 \
  -size 4096 \
  -block-size 512 \
  -threads 16 \
  -test create \
  -output small-files.json
```

## Output Format

The benchmark tool outputs detailed results including:

- **Test Configuration**: All parameters used for the test
- **Performance Metrics**: Throughput (MB/s), IOPS, latency statistics
- **Statistics**: Min, max, mean, median, P95, P99 latencies
- **Error Information**: Success and error counts
- **Thread Results**: Per-thread performance breakdown

Example output:
```
================================================================================
BENCHMARK RESULTS
================================================================================
Test Type:        mixed
Duration:         60.123s
Total Files:      1500
Total Bytes:      1572864000 (1.5 GB)
Total Operations: 4500
Success Count:    4500
Error Count:      0
Throughput:       25.67 MB/s
IOPS:             74.85 ops/s

Latency Statistics:
  Min:    1.234ms
  Max:    45.678ms
  Mean:   13.456ms
  Median: 12.345ms
  P95:    25.678ms
  P99:    35.789ms
================================================================================
```

## JSON Results Format

When using the `-output` option, results are saved in JSON format:

```json
{
  "test_type": "mixed",
  "config": {
    "volume_name": "test-volume",
    "num_files": 1000,
    "file_size": 1048576,
    "block_size": 4096,
    "num_threads": 8,
    "duration": 60,
    "test_type": "mixed"
  },
  "start_time": "2024-01-15T10:30:00Z",
  "end_time": "2024-01-15T10:31:00Z",
  "duration": "60.123s",
  "total_files": 1500,
  "total_bytes": 1572864000,
  "total_ops": 4500,
  "throughput_mbps": 25.67,
  "iops": 74.85,
  "latency": {
    "min_ms": "1.234ms",
    "max_ms": "45.678ms",
    "mean_ms": "13.456ms",
    "median_ms": "12.345ms",
    "p95_ms": "25.678ms",
    "p99_ms": "35.789ms"
  },
  "error_count": 0,
  "success_count": 4500,
  "thread_results": {
    "0": {
      "thread_id": 0,
      "files_created": 187,
      "bytes_written": 196083712,
      "bytes_read": 196083712,
      "ops_completed": 562,
      "errors": 0,
      "latencies": [...]
    }
  }
}
```

## Performance Tuning Tips

### 1. Block Size Optimization
- For large files: Use larger block sizes (64KB-1MB)
- For small files: Use smaller block sizes (512B-4KB)
- Test different block sizes to find optimal performance

### 2. Thread Count
- Start with CPU count
- Increase for I/O bound workloads
- Monitor system resources to avoid overload

### 3. File Count vs Size
- Many small files: Test metadata performance
- Few large files: Test data transfer performance
- Mixed workload: Test overall system performance

### 4. Duration
- Short tests (30-60s): Quick performance validation
- Long tests (300s+): Stress testing and stability validation

## Troubleshooting

### Common Issues

1. **Connection Errors**
   - Verify master addresses are correct
   - Check network connectivity
   - Ensure volume exists and is accessible

2. **Permission Errors**
   - Verify volume owner
   - Check authentication settings
   - Ensure proper volume permissions

3. **Performance Issues**
   - Check system resources (CPU, memory, network)
   - Verify storage backend performance
   - Monitor CubeFS cluster health

4. **Memory Issues**
   - Reduce thread count
   - Decrease file count
   - Use smaller block sizes

### Debug Mode

Enable verbose output for detailed debugging:
```bash
./cubefs-benchmark -verbose -volume test-vol -masters "192.168.1.100:17010" -test create
```

## Integration with Monitoring

The benchmark tool can be integrated with monitoring systems:

1. **Prometheus Metrics**: Parse JSON output for metrics
2. **Grafana Dashboards**: Create dashboards from benchmark results
3. **CI/CD Pipelines**: Use for performance regression testing
4. **Alerting**: Set thresholds for performance degradation

## Contributing

To contribute to the benchmark tool:

1. Follow the existing code style
2. Add tests for new features
3. Update documentation
4. Submit pull requests with detailed descriptions

## License

This tool is part of CubeFS and follows the same Apache 2.0 license. 