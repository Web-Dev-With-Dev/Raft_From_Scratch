.PHONY: all build run-cluster stop-cluster test clean help

CONFIG ?= cluster.json
BIN_DIR := bin
BINARY := $(BIN_DIR)/node

# Detect Windows environment
ifeq ($(OS),Windows_NT)
    BINARY := $(BIN_DIR)/node.exe
endif

all: build

help:
	@echo "Available targets:"
	@echo "  make build         - Build the Raft node binary"
	@echo "  make run-cluster   - Launch a 3-node local cluster with one command"
	@echo "  make stop-cluster  - Terminate any running cluster nodes"
	@echo "  make test          - Run all unit and integration tests"
	@echo "  make clean         - Remove compiled binaries and build artifacts"

build:
	@echo "==> Building Raft node binary..."
	@go build -o $(BINARY) ./cmd/node
	@echo "==> Built: $(BINARY)"

run-cluster: build
	@echo "==> Launching 3-node Raft cluster using $(CONFIG)..."
	@bash ./scripts/cluster.sh start

stop-cluster:
	@echo "==> Stopping Raft cluster..."
	@bash ./scripts/cluster.sh stop

test:
	@echo "==> Running all tests..."
	@go test -v ./...

clean:
	@echo "==> Cleaning build artifacts..."
	@go clean
	@rm -rf $(BIN_DIR)
