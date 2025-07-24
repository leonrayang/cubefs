#!/bin/bash

# Test script for Cubefs Client Demo

set -e

MOUNT_POINT="/tmp/cubefs_demo"
DATA_DIR="/tmp/cubefs_demo_data"
DEMO_BINARY="./cubefs_demo_standalone"

echo "=== Cubefs Client Demo Test ==="

# Clean up any existing mount
if mountpoint -q "$MOUNT_POINT"; then
    echo "Unmounting existing filesystem..."
    sudo umount "$MOUNT_POINT" 2>/dev/null || true
fi

# Create mount point
echo "Creating mount point..."
sudo mkdir -p "$MOUNT_POINT"

# Start the demo in background
echo "Starting FUSE demo..."
$DEMO_BINARY -mount "$MOUNT_POINT" -data "$DATA_DIR" -debug &
DEMO_PID=$!

# Wait for mount to be ready
echo "Waiting for mount to be ready..."
sleep 3

# Check if mount is successful
if ! mountpoint -q "$MOUNT_POINT"; then
    echo "ERROR: Failed to mount filesystem"
    kill $DEMO_PID 2>/dev/null || true
    exit 1
fi

echo "Filesystem mounted successfully at $MOUNT_POINT"

# Test basic operations
echo ""
echo "=== Testing Basic Operations ==="

# Test file creation
echo "Creating test file..."
echo "Hello World from Cubefs Demo!" > "$MOUNT_POINT/test.txt"

# Test file reading
echo "Reading test file..."
cat "$MOUNT_POINT/test.txt"

# Test directory listing
echo ""
echo "Directory listing:"
ls -la "$MOUNT_POINT/"

# Test directory creation
echo ""
echo "Creating directory..."
mkdir "$MOUNT_POINT/testdir"

# Test file in subdirectory
echo "Creating file in subdirectory..."
echo "Test content" > "$MOUNT_POINT/testdir/subfile.txt"

# Test directory listing
echo ""
echo "Subdirectory listing:"
ls -la "$MOUNT_POINT/testdir/"

# Test large file operations
echo ""
echo "=== Testing Large File Operations ==="

# Create a larger file
echo "Creating 1MB test file..."
dd if=/dev/zero of="$MOUNT_POINT/large_file.bin" bs=1M count=1 2>/dev/null

# Check file size
echo "Large file size:"
ls -lh "$MOUNT_POINT/large_file.bin"

# Test read performance
echo ""
echo "Testing read performance..."
time dd if="$MOUNT_POINT/large_file.bin" of=/dev/null bs=1M 2>/dev/null

# Test write performance
echo ""
echo "Testing write performance..."
time dd if=/dev/zero of="$MOUNT_POINT/write_test.bin" bs=1M count=10 2>/dev/null

# Performance comparison
echo ""
echo "=== Performance Comparison ==="
echo "Testing local filesystem performance for comparison..."

# Test local filesystem
time dd if=/dev/zero of=/tmp/local_test.bin bs=1M count=10 2>/dev/null
time dd if=/tmp/local_test.bin of=/dev/null bs=1M 2>/dev/null

# Cleanup local test file
rm -f /tmp/local_test.bin

echo ""
echo "=== Final Directory State ==="
ls -la "$MOUNT_POINT/"

# Unmount
echo ""
echo "Unmounting filesystem..."
sudo umount "$MOUNT_POINT"

# Kill demo process
kill $DEMO_PID 2>/dev/null || true

echo ""
echo "=== Test Complete ==="
echo "The demo successfully processed FUSE messages and stored data in memory."
echo "You can compare the performance with a real Cubefs deployment to measure FUSE overhead." 