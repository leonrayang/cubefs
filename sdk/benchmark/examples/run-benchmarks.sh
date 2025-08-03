#!/bin/bash

# CubeFS SDK Benchmark Examples
# This script demonstrates how to run different types of benchmarks

set -e

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Configuration
VOLUME_NAME=${VOLUME_NAME:-"test-volume"}
MASTERS=${MASTERS:-"127.0.0.1:17010"}
OWNER=${OWNER:-"benchmark"}
OUTPUT_DIR=${OUTPUT_DIR:-"./results"}
BENCHMARK_BIN=${BENCHMARK_BIN:-"../cubefs-benchmark"}

# Create output directory
mkdir -p "$OUTPUT_DIR"

echo -e "${BLUE}CubeFS SDK Benchmark Examples${NC}"
echo -e "${BLUE}============================${NC}"
echo ""
echo -e "Volume: ${GREEN}$VOLUME_NAME${NC}"
echo -e "Masters: ${GREEN}$MASTERS${NC}"
echo -e "Owner: ${GREEN}$OWNER${NC}"
echo -e "Output Directory: ${GREEN}$OUTPUT_DIR${NC}"
echo ""

# Function to run benchmark
run_benchmark() {
    local test_name="$1"
    local test_type="$2"
    local files="$3"
    local size="$4"
    local threads="$5"
    local duration="$6"
    local extra_args="$7"
    
    local output_file="$OUTPUT_DIR/${test_name}-$(date +%Y%m%d-%H%M%S).json"
    
    echo -e "${YELLOW}Running $test_name benchmark...${NC}"
    echo -e "  Type: $test_type"
    echo -e "  Files: $files"
    echo -e "  Size: $size bytes"
    echo -e "  Threads: $threads"
    echo -e "  Duration: $duration seconds"
    echo -e "  Output: $output_file"
    echo ""
    
    $BENCHMARK_BIN \
        -volume "$VOLUME_NAME" \
        -masters "$MASTERS" \
        -owner "$OWNER" \
        -test "$test_type" \
        -files "$files" \
        -size "$size" \
        -threads "$threads" \
        -duration "$duration" \
        -output "$output_file" \
        $extra_args
    
    if [ $? -eq 0 ]; then
        echo -e "${GREEN}✓ $test_name completed successfully${NC}"
        echo -e "  Results saved to: $output_file"
    else
        echo -e "${RED}✗ $test_name failed${NC}"
    fi
    echo ""
}

# Function to check if benchmark binary exists
check_binary() {
    if [ ! -f "$BENCHMARK_BIN" ]; then
        echo -e "${RED}Error: Benchmark binary not found at $BENCHMARK_BIN${NC}"
        echo -e "${YELLOW}Please build the benchmark tool first:${NC}"
        echo -e "  cd .. && make build"
        exit 1
    fi
    
    if [ ! -x "$BENCHMARK_BIN" ]; then
        echo -e "${RED}Error: Benchmark binary is not executable${NC}"
        chmod +x "$BENCHMARK_BIN"
    fi
}

# Function to show help
show_help() {
    echo -e "${BLUE}Usage: $0 [OPTIONS] [TEST_NAME]${NC}"
    echo ""
    echo -e "${YELLOW}Options:${NC}"
    echo -e "  -h, --help              Show this help message"
    echo -e "  -v, --volume NAME       Volume name (default: $VOLUME_NAME)"
    echo -e "  -m, --masters ADDRS     Master addresses (default: $MASTERS)"
    echo -e "  -o, --owner OWNER       Volume owner (default: $OWNER)"
    echo -e "  -d, --output-dir DIR    Output directory (default: $OUTPUT_DIR)"
    echo -e "  -b, --binary PATH       Benchmark binary path (default: $BENCHMARK_BIN)"
    echo ""
    echo -e "${YELLOW}Available Tests:${NC}"
    echo -e "  create-small            Create many small files"
    echo -e "  create-large            Create few large files"
    echo -e "  write-sequential        Sequential write test"
    echo -e "  write-random            Random write test"
    echo -e "  read-sequential         Sequential read test"
    echo -e "  read-random             Random read test"
    echo -e "  mixed-workload          Mixed workload test"
    echo -e "  stress-test             Stress test with high load"
    echo -e "  all                     Run all tests"
    echo ""
    echo -e "${YELLOW}Examples:${NC}"
    echo -e "  $0 create-small"
    echo -e "  $0 -v my-volume -m \"192.168.1.100:17010\" mixed-workload"
    echo -e "  $0 -o my-user all"
}

# Parse command line arguments
while [[ $# -gt 0 ]]; do
    case $1 in
        -h|--help)
            show_help
            exit 0
            ;;
        -v|--volume)
            VOLUME_NAME="$2"
            shift 2
            ;;
        -m|--masters)
            MASTERS="$2"
            shift 2
            ;;
        -o|--owner)
            OWNER="$2"
            shift 2
            ;;
        -d|--output-dir)
            OUTPUT_DIR="$2"
            shift 2
            ;;
        -b|--binary)
            BENCHMARK_BIN="$2"
            shift 2
            ;;
        *)
            TEST_NAME="$1"
            shift
            ;;
    esac
done

# Check binary
check_binary

# If no test specified, show help
if [ -z "$TEST_NAME" ]; then
    show_help
    exit 1
fi

echo -e "${BLUE}Starting benchmark tests...${NC}"
echo ""

# Run specified test or all tests
case "$TEST_NAME" in
    "create-small")
        run_benchmark "create-small" "create" 10000 4096 16 60
        ;;
    "create-large")
        run_benchmark "create-large" "create" 100 10485760 8 60
        ;;
    "write-sequential")
        run_benchmark "write-sequential" "write" 1000 1048576 8 60
        ;;
    "write-random")
        run_benchmark "write-random" "write" 1000 1048576 8 60 "-random"
        ;;
    "read-sequential")
        run_benchmark "read-sequential" "read" 1000 1048576 8 60
        ;;
    "read-random")
        run_benchmark "read-random" "read" 1000 1048576 8 60
        ;;
    "mixed-workload")
        run_benchmark "mixed-workload" "mixed" 1000 1048576 8 120
        ;;
    "stress-test")
        run_benchmark "stress-test" "mixed" 5000 1048576 32 300 "-random"
        ;;
    "all")
        echo -e "${BLUE}Running all benchmark tests...${NC}"
        echo ""
        
        run_benchmark "create-small" "create" 10000 4096 16 60
        run_benchmark "create-large" "create" 100 10485760 8 60
        run_benchmark "write-sequential" "write" 1000 1048576 8 60
        run_benchmark "write-random" "write" 1000 1048576 8 60 "-random"
        run_benchmark "read-sequential" "read" 1000 1048576 8 60
        run_benchmark "read-random" "read" 1000 1048576 8 60
        run_benchmark "mixed-workload" "mixed" 1000 1048576 8 120
        run_benchmark "stress-test" "mixed" 5000 1048576 32 300 "-random"
        
        echo -e "${GREEN}All benchmark tests completed!${NC}"
        echo -e "Results saved in: $OUTPUT_DIR"
        ;;
    *)
        echo -e "${RED}Error: Unknown test '$TEST_NAME'${NC}"
        echo ""
        show_help
        exit 1
        ;;
esac

echo -e "${GREEN}Benchmark examples completed!${NC}"
echo -e "Check the results in: $OUTPUT_DIR" 