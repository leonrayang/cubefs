# Pprof Profiling for FUSE Demo

This version of the FUSE demo includes Go's pprof profiling capabilities to help debug performance issues and analyze the hanging problem.

## Features Added

1. **HTTP Pprof Server**: Runs on port 6060 by default
2. **Profiling Hooks**: Added to ReadDir function for debugging
3. **Easy Profile Collection**: Simple curl commands to collect profiles

## Usage

### Start the Demo with Pprof

```bash
# Build the pprof-enabled version
go build -o /tmp/bazil_fuse_demo_pprof main.go

# Start with pprof server
sudo /tmp/bazil_fuse_demo_pprof -mount /tmp/cubefs_demo -data /tmp/cubefs_demo_data -debug -pprof :6060
```

### Available Profiling Endpoints

- **CPU Profile**: `http://localhost:6060/debug/pprof/profile`
- **Memory Profile**: `http://localhost:6060/debug/pprof/heap`
- **Goroutine Profile**: `http://localhost:6060/debug/pprof/goroutine`
- **All Profiles**: `http://localhost:6060/debug/pprof/`

### Collect Profiles

```bash
# CPU profile (30 seconds)
curl -o cpu.prof http://localhost:6060/debug/pprof/profile

# Memory profile
curl -o heap.prof http://localhost:6060/debug/pprof/heap

# Goroutine profile
curl -o goroutine.prof http://localhost:6060/debug/pprof/goroutine
```

### Analyze Profiles

```bash
# Interactive CPU analysis
go tool pprof cpu.prof

# Interactive memory analysis
go tool pprof heap.prof

# Interactive goroutine analysis
go tool pprof goroutine.prof

# Web interface (if you have graphviz installed)
go tool pprof -http=:8080 cpu.prof
```

## Debugging the Hanging Issue

### Step 1: Start the Demo
```bash
sudo /tmp/bazil_fuse_demo_pprof -mount /tmp/cubefs_demo -data /tmp/cubefs_demo_data -debug -pprof :6060 > /tmp/fuse_demo_pprof.log 2>&1 &
```

### Step 2: Collect Profiles During Hanging
```bash
# In another terminal, when ls hangs:
curl -o cpu_hanging.prof http://localhost:6060/debug/pprof/profile
curl -o goroutine_hanging.prof http://localhost:6060/debug/pprof/goroutine
```

### Step 3: Analyze the Profiles
```bash
# Check what's consuming CPU
go tool pprof cpu_hanging.prof

# Check goroutine stack traces
go tool pprof goroutine_hanging.prof
```

## Common Commands in pprof

Once in the pprof interactive mode:

```bash
# Show top functions by CPU usage
(pprof) top

# Show top functions by memory usage
(pprof) top -cum

# Show call graph
(pprof) web

# Show specific function
(pprof) list ReadDir

# Show goroutines
(pprof) traces
```

## Example Workflow

1. **Start the demo**: `./pprof_test.sh`
2. **Trigger the hanging**: `sudo ls /tmp/cubefs_demo`
3. **Collect profiles**: Use the curl commands above
4. **Analyze**: Use `go tool pprof` to examine the profiles
5. **Identify the issue**: Look for functions consuming excessive CPU or blocked goroutines

## Troubleshooting

- **Port already in use**: Change the port with `-pprof :6061`
- **Permission denied**: Make sure to run with sudo
- **No profiles available**: Check that the demo is running and the pprof server started

## Integration with Existing Tools

The pprof server integrates well with:
- **go tool pprof**: Command-line analysis
- **pprof web interface**: Visual analysis (requires graphviz)
- **Continuous profiling**: Tools like Prometheus + Grafana 