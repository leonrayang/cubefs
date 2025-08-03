#!/bin/bash

# CubeFS SDK Benchmark Tool Build Script

set -e

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# Script directory
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"

echo -e "${GREEN}Building CubeFS SDK Benchmark Tool...${NC}"

# Check if Go is installed
if ! command -v go &> /dev/null; then
    echo -e "${RED}Error: Go is not installed. Please install Go 1.16 or later.${NC}"
    exit 1
fi

# Check Go version
GO_VERSION=$(go version | awk '{print $3}' | sed 's/go//')
REQUIRED_VERSION="1.16"

if [ "$(printf '%s\n' "$REQUIRED_VERSION" "$GO_VERSION" | sort -V | head -n1)" != "$REQUIRED_VERSION" ]; then
    echo -e "${RED}Error: Go version $GO_VERSION is too old. Required: $REQUIRED_VERSION or later.${NC}"
    exit 1
fi

echo -e "${GREEN}Go version: $GO_VERSION${NC}"

# Set build variables
BINARY_NAME="cubefs-benchmark"
BUILD_DIR="$SCRIPT_DIR"
OUTPUT_DIR="$SCRIPT_DIR"

# Create output directory if it doesn't exist
mkdir -p "$OUTPUT_DIR"

# Build the benchmark tool
echo -e "${YELLOW}Building benchmark tool...${NC}"
cd "$BUILD_DIR"

# Set build flags
LDFLAGS="-s -w"
BUILD_FLAGS="-ldflags '$LDFLAGS'"

# Build for current platform
echo "Building for $(go env GOOS)/$(go env GOARCH)..."
go build $BUILD_FLAGS -o "$OUTPUT_DIR/$BINARY_NAME" benchmark.go

if [ $? -eq 0 ]; then
    echo -e "${GREEN}✓ Successfully built $BINARY_NAME${NC}"
    echo -e "${GREEN}Binary location: $OUTPUT_DIR/$BINARY_NAME${NC}"
    
    # Make executable
    chmod +x "$OUTPUT_DIR/$BINARY_NAME"
    
    # Show binary info
    echo -e "${YELLOW}Binary information:${NC}"
    ls -lh "$OUTPUT_DIR/$BINARY_NAME"
    
    echo -e "${GREEN}Build completed successfully!${NC}"
    echo -e "${YELLOW}To run the benchmark tool:${NC}"
    echo -e "  cd $OUTPUT_DIR"
    echo -e "  ./$BINARY_NAME -help"
    echo -e ""
    echo -e "${YELLOW}Example usage:${NC}"
    echo -e "  ./$BINARY_NAME -volume test-vol -masters \"192.168.1.100:17010\" -test mixed"
else
    echo -e "${RED}✗ Build failed${NC}"
    exit 1
fi

# Optional: Build for multiple platforms
if [ "$1" = "--cross-compile" ]; then
    echo -e "${YELLOW}Cross-compiling for multiple platforms...${NC}"
    
    PLATFORMS=(
        "linux/amd64"
        "linux/arm64"
        "darwin/amd64"
        "darwin/arm64"
        "windows/amd64"
    )
    
    for platform in "${PLATFORMS[@]}"; do
        IFS='/' read -r GOOS GOARCH <<< "$platform"
        OUTPUT="$OUTPUT_DIR/${BINARY_NAME}-${GOOS}-${GOARCH}"
        
        if [ "$GOOS" = "windows" ]; then
            OUTPUT="$OUTPUT.exe"
        fi
        
        echo "Building for $platform..."
        env GOOS=$GOOS GOARCH=$GOARCH go build $BUILD_FLAGS -o "$OUTPUT" benchmark.go
        
        if [ $? -eq 0 ]; then
            echo -e "${GREEN}✓ Built $OUTPUT${NC}"
        else
            echo -e "${RED}✗ Failed to build for $platform${NC}"
        fi
    done
    
    echo -e "${GREEN}Cross-compilation completed!${NC}"
fi

echo -e "${GREEN}Build script completed.${NC}" 