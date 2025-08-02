#!/bin/bash

# Pprof-enabled FUSE Demo Test Script

echo "Starting FUSE Demo with Pprof profiling..."

# Kill any existing processes
sudo pkill -f bazil_fuse_demo 2>/dev/null || true
sudo umount -f /tmp/cubefs_demo 2>/dev/null || true

# Start the demo with pprof
echo "Starting demo on port 6060..."
sudo /tmp/bazil_fuse_demo_pprof -mount /tmp/cubefs_demo -data /tmp/cubefs_demo_data -debug -pprof :6060 > /tmp/fuse_demo_pprof.log 2>&1 &
DEMO_PID=$!

echo "Demo started with PID: $DEMO_PID"
echo "Pprof server available at: http://localhost:6060/debug/pprof/"
echo ""
echo "Available profiling endpoints:"
echo "  - CPU profile: http://localhost:6060/debug/pprof/profile"
echo "  - Memory profile: http://localhost:6060/debug/pprof/heap"
echo "  - Goroutine profile: http://localhost:6060/debug/pprof/goroutine"
echo "  - All profiles: http://localhost:6060/debug/pprof/"
echo ""
echo "To collect profiles:"
echo "  curl -o cpu.prof http://localhost:6060/debug/pprof/profile"
echo "  curl -o heap.prof http://localhost:6060/debug/pprof/heap"
echo "  curl -o goroutine.prof http://localhost:6060/debug/pprof/goroutine"
echo ""
echo "To analyze profiles:"
echo "  go tool pprof cpu.prof"
echo "  go tool pprof heap.prof"
echo "  go tool pprof goroutine.prof"
echo ""
echo "Press Enter to test the mount point..."
read

# Test the mount
echo "Testing mount point..."
sudo ls /tmp/cubefs_demo

echo ""
echo "Press Enter to stop the demo..."
read

# Cleanup
echo "Stopping demo..."
sudo pkill -f bazil_fuse_demo_pprof
sudo umount -f /tmp/cubefs_demo 2>/dev/null || true

echo "Demo stopped. Check /tmp/fuse_demo_pprof.log for logs." 