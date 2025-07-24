# Bazil/FUSE Demo

This is a simple FUSE filesystem demo using the CubeFS depends/bazil.org/fuse library.

## Purpose

This demo serves as a test tool for bazil/fuse performance and functionality. It implements a basic in-memory filesystem that can be mounted and used for testing.

## Build

The demo is integrated into the main CubeFS build system:

```bash
# Build just the demo
make bazil_fuse_demo

# Build all components including the demo
make build
```

The binary will be created at `build/bin/bazil-fuse-demo`.

## Usage

```bash
# Basic usage
./build/bin/bazil-fuse-demo

# With custom mount point and data directory
./build/bin/bazil-fuse-demo -mount /tmp/my_demo -data /tmp/my_data

# With debug logging
./build/bin/bazil-fuse-demo -debug
```

## Features

- **In-memory filesystem**: All data is stored in memory
- **Basic file operations**: Create, read, write, delete files and directories
- **FUSE protocol**: Uses the CubeFS depends/bazil.org/fuse library
- **Simple interface**: Easy to understand and modify

## Integration

This demo is part of the CubeFS project and uses:
- CubeFS depends/bazil.org/fuse library
- Main project build system
- No external dependencies

## Testing

The demo can be used to test:
- FUSE protocol compatibility
- Filesystem performance
- Basic filesystem operations
- Mount/unmount functionality 