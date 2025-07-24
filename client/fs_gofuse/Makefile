# Makefile for Cubefs Go-FUSE Client

.PHONY: build clean test deps

BINARY_NAME = cubefs_gofuse
BUILD_DIR = build

build: deps
	@echo "Building Cubefs Go-FUSE client..."
	@mkdir -p $(BUILD_DIR)
	go build -o $(BUILD_DIR)/$(BINARY_NAME) main.go

clean:
	@echo "Cleaning build artifacts..."
	rm -rf $(BUILD_DIR)
	go clean

test:
	@echo "Running tests..."
	go test -v ./...

deps:
	@echo "Downloading dependencies..."
	go mod tidy
	go mod download

install: build
	@echo "Installing Cubefs Go-FUSE client..."
	cp $(BUILD_DIR)/$(BINARY_NAME) /usr/local/bin/

run: build
	@echo "Running Cubefs Go-FUSE client..."
	./$(BUILD_DIR)/$(BINARY_NAME) -debug

run-mount: build
	@echo "Running Cubefs Go-FUSE client with custom mount point..."
	./$(BUILD_DIR)/$(BINARY_NAME) -mount /tmp/cubefs_gofuse -volume cubefs -masters 127.0.0.1:17010 -debug

help:
	@echo "Available targets:"
	@echo "  build      - Build the client binary"
	@echo "  clean      - Clean build artifacts"
	@echo "  test       - Run tests"
	@echo "  deps       - Download dependencies"
	@echo "  install    - Install binary to /usr/local/bin"
	@echo "  run        - Build and run with default settings"
	@echo "  run-mount  - Build and run with custom mount point"
	@echo "  help       - Show this help message" 