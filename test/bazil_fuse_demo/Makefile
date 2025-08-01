# Makefile for Cubefs Client Demo

.PHONY: build clean run

# Build the client demo
build:
	go build -o cubefs_demo main.go

# Clean build artifacts
clean:
	rm -f cubefs_demo

# Run the demo (build first)
run: build
	./cubefs_demo -mount /tmp/cubefs_demo -data /tmp/cubefs_demo_data -debug

# Install dependencies
deps:
	go mod init cubefs_demo
	go mod tidy

# Test the demo
test: build
	# Create mount point
	sudo mkdir -p /tmp/cubefs_demo
	# Run demo in background
	./cubefs_demo -mount /tmp/cubefs_demo -data /tmp/cubefs_demo_data &
	# Wait a moment for mount
	sleep 2
	# Test basic operations
	echo "Testing file operations..."
	echo "Hello World" > /tmp/cubefs_demo/test.txt
	cat /tmp/cubefs_demo/test.txt
	ls -la /tmp/cubefs_demo/
	# Cleanup
	sudo umount /tmp/cubefs_demo
	rm -f cubefs_demo 