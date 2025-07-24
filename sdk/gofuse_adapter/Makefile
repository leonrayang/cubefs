# Makefile for Cubefs Go-FUSE SDK

.PHONY: build clean test deps

BUILD_DIR = build

build: deps
	@echo "Building Cubefs Go-FUSE SDK..."
	@mkdir -p $(BUILD_DIR)
	go build -o $(BUILD_DIR)/sdk_gofuse.a .

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
	@echo "Installing Cubefs Go-FUSE SDK..."
	cp $(BUILD_DIR)/sdk_gofuse.a /usr/local/lib/

help:
	@echo "Available targets:"
	@echo "  build      - Build the SDK library"
	@echo "  clean      - Clean build artifacts"
	@echo "  test       - Run tests"
	@echo "  deps       - Download dependencies"
	@echo "  install    - Install library to /usr/local/lib"
	@echo "  help       - Show this help message" 