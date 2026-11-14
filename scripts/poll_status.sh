#!/usr/bin/env bash
# ==============================================================================
# Polls /status on all Raft cluster nodes and prints an ASCII table every second.
# Usage:
#   ./scripts/poll_status.sh
#   ./scripts/poll_status.sh "http://127.0.0.1:9001/status http://127.0.0.1:9002/status"
# ==============================================================================

set -euo pipefail

ENDPOINTS=(${1:-"http://127.0.0.1:9001/status http://127.0.0.1:9002/status http://127.0.0.1:9003/status"})
INTERVAL=${2:-1}
COUNT=${3:-0}

echo "Polling ${#ENDPOINTS[@]} node(s) every ${INTERVAL}s (Press Ctrl+C to stop)..."

iteration=0

trap 'echo -e "\nStopped status polling."; exit 0' SIGINT SIGTERM

while true; do
    iteration=$((iteration + 1))
    timestamp=$(date "+%Y-%m-%d %H:%M:%S")

    echo ""
    echo "--- [Iteration #$iteration | $timestamp] Raft Cluster Status ---"
    printf "+---------+-----------+------+----------+------------+----------------------------------+\n"
    printf "| Node ID |   State   | Term | VotedFor | Status     | Endpoint                         |\n"
    printf "+---------+-----------+------+----------+------------+----------------------------------+\n"

    for ep in "${ENDPOINTS[@]}"; do
        response=$(curl -s -m 1 "$ep" 2>/dev/null || true)
        if [ -n "$response" ] && echo "$response" | grep -q '"term"'; then
            id=$(echo "$response" | grep -o '"id": *[0-9]*' | head -n1 | awk '{print $2}')
            term=$(echo "$response" | grep -o '"term": *[0-9]*' | head -n1 | awk '{print $2}')
            state=$(echo "$response" | grep -o '"state": *"[^"]*"' | head -n1 | cut -d'"' -f4)
            voted_for=$(echo "$response" | grep -o '"votedFor": *[-0-9]*' | head -n1 | awk '{print $2}')

            voted_str="$voted_for"
            if [ "$voted_for" = "-1" ]; then
                voted_str="none (-1)"
            elif [ "$voted_for" = "$id" ]; then
                voted_str="self ($id)"
            fi

            printf "| Node %-2s | %-9s | %-4s | %-8s | %-10s | %-32s |\n" \
                "$id" "$state" "$term" "$voted_str" "ONLINE" "$ep"
        else
            printf "| %-7s | %-9s | %-4s | %-8s | %-10s | %-32s |\n" \
                "-" "-" "-" "-" "OFFLINE" "$ep"
        fi
    done

    printf "+---------+-----------+------+----------+------------+----------------------------------+\n"

    if [ "$COUNT" -gt 0 ] && [ "$iteration" -ge "$COUNT" ]; then
        break
    fi

    sleep "$INTERVAL"
done

