# CubeFS SDK Benchmark Tool Makefile

.PHONY: all build clean test help install

# Variables
BINARY_NAME = cubefs-benchmark
BUILD_DIR = .
SOURCE_FILES = benchmark.go
VERSION = $(shell git describe --tags --always --dirty 2>/dev/null || echo "unknown")
BUILD_TIME = $(shell date -u '+%Y-%m-%d_%H:%M:%S')
LDFLAGS = -ldflags "-X main.Version=$(VERSION) -X main.BuildTime=$(BUILD_TIME) -s -w"

# Default target
all: build

# Build the benchmark tool
build:
	@echo "Building $(BINARY_NAME)..."
	@go build $(LDFLAGS) -o $(BINARY_NAME) $(SOURCE_FILES)
	@chmod +x $(BINARY_NAME)
	@echo "Build completed: $(BINARY_NAME)"

# Build with debug information
build-debug:
	@echo "Building $(BINARY_NAME) with debug information..."
	@go build -gcflags="all=-N -l" -o $(BINARY_NAME) $(SOURCE_FILES)
	@chmod +x $(BINARY_NAME)
	@echo "Debug build completed: $(BINARY_NAME)"

# Build for multiple platforms
build-all: build-linux-amd64 build-linux-arm64 build-darwin-amd64 build-darwin-arm64 build-windows-amd64

build-linux-amd64:
	@echo "Building for linux/amd64..."
	@GOOS=linux GOARCH=amd64 go build $(LDFLAGS) -o $(BINARY_NAME)-linux-amd64 $(SOURCE_FILES)

build-linux-arm64:
	@echo "Building for linux/arm64..."
	@GOOS=linux GOARCH=arm64 go build $(LDFLAGS) -o $(BINARY_NAME)-linux-arm64 $(SOURCE_FILES)

build-darwin-amd64:
	@echo "Building for darwin/amd64..."
	@GOOS=darwin GOARCH=amd64 go build $(LDFLAGS) -o $(BINARY_NAME)-darwin-amd64 $(SOURCE_FILES)

build-darwin-arm64:
	@echo "Building for darwin/arm64..."
	@GOOS=darwin GOARCH=arm64 go build $(LDFLAGS) -o $(BINARY_NAME)-darwin-arm64 $(SOURCE_FILES)

build-windows-amd64:
	@echo "Building for windows/amd64..."
	@GOOS=windows GOARCH=amd64 go build $(LDFLAGS) -o $(BINARY_NAME)-windows-amd64.exe $(SOURCE_FILES)

# Run tests
test:
	@echo "Running tests..."
	@go test -v ./...

# Run tests with coverage
test-coverage:
	@echo "Running tests with coverage..."
	@go test -v -coverprofile=coverage.out ./...
	@go tool cover -html=coverage.out -o coverage.html
	@echo "Coverage report generated: coverage.html"

# Clean build artifacts
clean:
	@echo "Cleaning build artifacts..."
	@rm -f $(BINARY_NAME)
	@rm -f $(BINARY_NAME)-*
	@rm -f coverage.out coverage.html
	@echo "Clean completed"

# Install to system (requires sudo)
install: build
	@echo "Installing $(BINARY_NAME) to /usr/local/bin..."
	@sudo cp $(BINARY_NAME) /usr/local/bin/
	@echo "Installation completed"

# Uninstall from system
uninstall:
	@echo "Uninstalling $(BINARY_NAME) from /usr/local/bin..."
	@sudo rm -f /usr/local/bin/$(BINARY_NAME)
	@echo "Uninstallation completed"

# Run the benchmark tool with example parameters
run-example: build
	@echo "Running example benchmark..."
	@./$(BINARY_NAME) -help

# Run a quick test
run-quick: build
	@echo "Running quick benchmark test..."
	@./$(BINARY_NAME) -volume test-vol -masters "127.0.0.1:17010" -test create -files 10 -threads 2 -duration 5

# Format code
fmt:
	@echo "Formatting code..."
	@go fmt $(SOURCE_FILES)

# Run linter
lint:
	@echo "Running linter..."
	@if command -v golangci-lint >/dev/null 2>&1; then \
		golangci-lint run; \
	else \
		echo "golangci-lint not found, skipping linting"; \
	fi

# Check for security vulnerabilities
security:
	@echo "Checking for security vulnerabilities..."
	@if command -v gosec >/dev/null 2>&1; then \
		gosec ./...; \
	else \
		echo "gosec not found, skipping security check"; \
	fi

# Generate documentation
docs:
	@echo "Generating documentation..."
	@if command -v godoc >/dev/null 2>&1; then \
		godoc -http=:6060 & echo "Documentation server started at http://localhost:6060"; \
	else \
		echo "godoc not found, skipping documentation generation"; \
	fi

# Show help
help:
	@echo "CubeFS SDK Benchmark Tool Makefile"
	@echo ""
	@echo "Available targets:"
	@echo "  build          - Build the benchmark tool"
	@echo "  build-debug    - Build with debug information"
	@echo "  build-all      - Build for all platforms"
	@echo "  test           - Run tests"
	@echo "  test-coverage  - Run tests with coverage report"
	@echo "  clean          - Clean build artifacts"
	@echo "  install        - Install to system"
	@echo "  uninstall      - Uninstall from system"
	@echo "  run-example    - Run with help"
	@echo "  run-quick      - Run quick test"
	@echo "  fmt            - Format code"
	@echo "  lint           - Run linter"
	@echo "  security       - Check for security vulnerabilities"
	@echo "  docs           - Generate documentation"
	@echo "  help           - Show this help"
	@echo ""
	@echo "Examples:"
	@echo "  make build"
	@echo "  make test"
	@echo "  make clean"
	@echo "  make run-quick"

# Show version information
version:
	@echo "Version: $(VERSION)"
	@echo "Build Time: $(BUILD_TIME)"
	@echo "Go Version: $(shell go version)"
	@echo "Platform: $(shell go env GOOS)/$(shell go env GOARCH)"

# Dependencies
deps:
	@echo "Downloading dependencies..."
	@go mod download
	@go mod tidy

# Update dependencies
deps-update:
	@echo "Updating dependencies..."
	@go get -u ./...
	@go mod tidy

# Show dependency tree
deps-tree:
	@echo "Dependency tree:"
	@go mod graph

# Create release package
release: clean build-all
	@echo "Creating release package..."
	@mkdir -p release
	@cp $(BINARY_NAME)-* release/
	@cp README.md config-example.json release/
	@tar -czf cubefs-benchmark-$(VERSION).tar.gz release/
	@rm -rf release
	@echo "Release package created: cubefs-benchmark-$(VERSION).tar.gz" 