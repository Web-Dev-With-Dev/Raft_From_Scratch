#!/usr/bin/env bash
# ==============================================================================
# Raft 3-Node Cluster Launcher
# Launches nodes 1, 2, and 3 concurrently with unified structured logs.
# Pressing Ctrl+C shuts down all 3 nodes gracefully.
# ==============================================================================

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$SCRIPT_DIR"

CONFIG_FILE="${CONFIG_FILE:-cluster.json}"
# Detect Go binary if not already in PATH (e.g. WSL or Git Bash on Windows)
if ! command -v go >/dev/null 2>&1; then
    for go_candidate in \
        "/mnt/c/Program Files/Go/bin" \
        "/c/Program Files/Go/bin" \
        "/usr/local/go/bin" \
        "$HOME/go/bin"; do
        if [ -d "$go_candidate" ]; then
            export PATH="$PATH:$go_candidate"
            break
        fi
    done
fi

GO_CMD="go"
if ! command -v go >/dev/null 2>&1 && command -v go.exe >/dev/null 2>&1; then
    GO_CMD="go.exe"
fi

BIN_DIR="bin"
NODE_BIN="$BIN_DIR/node"
if command -v "$BIN_DIR/node.exe" >/dev/null 2>&1 || [ -f "$BIN_DIR/node.exe" ] || command -v go.exe >/dev/null 2>&1; then
    NODE_BIN="$BIN_DIR/node.exe"
fi

build() {
    echo "==> Building Raft node binary..."
    mkdir -p "$BIN_DIR"
    "$GO_CMD" build -o "$NODE_BIN" ./cmd/node
    echo "==> Build complete: $NODE_BIN"
}

start() {
    build

    if [ ! -f "$CONFIG_FILE" ]; then
        echo "Error: Configuration file '$CONFIG_FILE' not found!"
        exit 1
    fi

    echo "==> Launching 3-node Raft cluster (Nodes 1, 2, 3) using $CONFIG_FILE..."
    echo "==> Press Ctrl+C at any time to stop all nodes."
    echo "----------------------------------------------------------------------"

    PIDS=()

    cleanup() {
        echo ""
        echo "==> Received shutdown signal. Terminating cluster nodes..."
        for pid in "${PIDS[@]}"; do
            if kill -0 "$pid" 2>/dev/null; then
                kill -TERM "$pid" 2>/dev/null || true
            fi
        done

        # Wait briefly for graceful shutdown
        sleep 1

        for pid in "${PIDS[@]}"; do
            if kill -0 "$pid" 2>/dev/null; then
                kill -9 "$pid" 2>/dev/null || true
            fi
        done

        echo "==> All cluster nodes stopped."
        exit 0
    }

    trap cleanup SIGINT SIGTERM EXIT

    # Launch node 1
    "$NODE_BIN" -id 1 -config "$CONFIG_FILE" &
    PIDS+=($!)

    # Launch node 2
    "$NODE_BIN" -id 2 -config "$CONFIG_FILE" &
    PIDS+=($!)

    # Launch node 3
    "$NODE_BIN" -id 3 -config "$CONFIG_FILE" &
    PIDS+=($!)

    # Wait for all node processes
    wait
}

stop() {
    echo "==> Stopping any running node processes..."
    if command -v pkill >/dev/null 2>&1; then
        pkill -f "node.*-config" 2>/dev/null || true
    fi
    if command -v taskkill >/dev/null 2>&1; then
        taskkill //F //IM node.exe 2>/dev/null || true
    fi
    echo "==> Cluster stopped."
}

case "${1:-start}" in
    start)
        start
        ;;
    stop)
        stop
        ;;
    build)
        build
        ;;
    *)
        echo "Usage: $0 {start|stop|build}"
        exit 1
        ;;
esac
