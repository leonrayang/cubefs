# CubeFS go-fuse: Two Binary Implementation

## Overview

The `client/fs_gofuse` directory now provides **two distinct binaries** for different use cases:

### 1. 🎯 **Demo Binary**: `cubefs_gofuse_demo`
**Purpose**: Standalone demo for testing go-fuse performance and functionality

**Features**:
- ✅ **No backend required** - runs completely standalone
- ✅ **In-memory filesystem** - creates virtual files for testing
- ✅ **FUSE kernel parameter testing** - test performance impact of different settings
- ✅ **Simple to use** - perfect for go-fuse development and benchmarking

**Usage**:
```bash
# Build the demo
go build -tags demo -o cubefs_gofuse_demo demo_main.go

# Run with default parameters
./cubefs_gofuse_demo

# Run with custom FUSE parameters
./cubefs_gofuse_demo -mount /tmp/demo \
    -fuseMaxBackground 64 \
    -fuseMaxPagesPerReq 512 \
    -debug

# Test the demo filesystem
cat /tmp/demo/fuse_info.txt
ls -la /tmp/demo/
```

### 2. 🏢 **Production Client**: `cubefs_client_gofuse`
**Purpose**: Real CubeFS client using `sdk/gofuse_adapter` for backend connectivity

**Features**:
- ✅ **Full CubeFS integration** - connects to master, metanode, datanode
- ✅ **Complete filesystem operations** - all FUSE operations supported
- ✅ **Backend connectivity** - uses `gofuse_adapter` for CubeFS protocol
- ✅ **Production ready** - includes caching, logging, monitoring
- ✅ **New FUSE kernel parameters** - `fuseDefaultMaxBackground` and `fuseMaxPagesPerReq`

**Usage**:
```bash
# Build the production client (already built)
go build -o cubefs_client_gofuse .

# Run with configuration file
./cubefs_client_gofuse -c config.json

# Run with command line parameters
./cubefs_client_gofuse \
    -mountPoint /mnt/cubefs \
    -volName my_volume \
    -masterAddr "master1:17010,master2:17010" \
    -fuseDefaultMaxBackground 64 \
    -fuseMaxPagesPerReq 512
```

## 🔧 New FUSE Kernel Parameters

Both binaries support the new FUSE kernel parameters:

| Parameter | Default | Description |
|-----------|---------|-------------|
| `fuseDefaultMaxBackground` | 32 | Maximum number of background FUSE requests |
| `fuseMaxPagesPerReq` | 256 | Maximum pages per FUSE request |

### Performance Impact

These parameters allow fine-tuning of:
- **Concurrency**: Higher `maxBackground` allows more parallel operations
- **Memory usage**: Higher `maxPagesPerReq` uses more memory but can improve throughput
- **I/O patterns**: Different workloads benefit from different settings

## 📊 Testing Strategy

### 1. **Development & Performance Testing**
Use `cubefs_gofuse_demo` for:
- Testing go-fuse library integration
- Benchmarking FUSE parameter impact
- Development without backend setup
- Quick functional verification

### 2. **Production Integration Testing** 
Use `cubefs_client_gofuse` for:
- Full CubeFS cluster testing
- Real workload performance evaluation
- Production deployment validation
- Backend connectivity verification

## 🏗️ Build Architecture

```
client/fs_gofuse/
├── main.go                    # Production client (gofuse_adapter)
├── demo_main.go              # Demo client (build tag: demo)
├── fuse_nodes.go             # FUSE node implementations
├── cache.go                  # Cache management
├── interface.go              # Interfaces
├── fuse_config_example.json  # Example configuration
└── TWO_BINARIES_SUMMARY.md   # This document
```

**Build commands**:
```bash
# Production client
go build -o cubefs_client_gofuse .

# Demo client
go build -tags demo -o cubefs_gofuse_demo demo_main.go
```

## 🎯 Use Cases

### Demo Binary Perfect For:
- 🧪 **Go-fuse library testing**
- 📈 **Performance benchmarking**
- 🔧 **FUSE parameter tuning**
- 🚀 **Quick development iteration**
- 📚 **Learning FUSE concepts**

### Production Client Perfect For:
- 🏢 **Production deployments**
- 🔗 **CubeFS cluster integration**
- 💾 **Real data operations**
- 📊 **Performance monitoring**
- 🔐 **Authentication & security**

## ✅ Success Validation

Both binaries have been successfully:
- ✅ **Built without errors**
- ✅ **Include new FUSE parameters** in help output
- ✅ **Demonstrate parameter configuration**
- ✅ **Ready for respective use cases**

The implementation provides a complete solution for both go-fuse development/testing and production CubeFS deployment! 