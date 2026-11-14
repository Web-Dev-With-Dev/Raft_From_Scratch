package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"sort"
	"strings"
	"sync"
	"syscall"
	"time"
)

// NodeStatus mirrors the /status response structure.
type NodeStatus struct {
	ID        int64  `json:"id"`
	Term      uint64 `json:"term"`
	State     string `json:"state"`
	VotedFor  int64  `json:"votedFor"`
	LogLength int    `json:"logLength"`
}

type NodeResult struct {
	Endpoint string
	Status   *NodeStatus
	Err      error
}

func main() {
	endpointsFlag := flag.String("endpoints",
		"http://127.0.0.1:9001/status,http://127.0.0.1:9002/status,http://127.0.0.1:9003/status",
		"Comma-separated list of node /status HTTP URLs",
	)
	intervalFlag := flag.Duration("interval", 1*time.Second, "Polling interval")
	countFlag := flag.Int("count", 0, "Number of poll iterations (0 for infinite loop)")
	clearScreenFlag := flag.Bool("clear", false, "Clear terminal screen before each update")
	flag.Parse()

	endpoints := strings.Split(*endpointsFlag, ",")
	for i := range endpoints {
		endpoints[i] = strings.TrimSpace(endpoints[i])
	}

	client := &http.Client{
		Timeout: 600 * time.Millisecond,
	}

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, os.Interrupt, syscall.SIGTERM)

	ticker := time.NewTicker(*intervalFlag)
	defer ticker.Stop()

	fmt.Printf("Polling %d node(s) every %v (Press Ctrl+C to stop)\n", len(endpoints), *intervalFlag)
	iteration := 0

	pollAndPrint := func() {
		iteration++
		results := pollNodes(client, endpoints)
		printTable(iteration, results, *clearScreenFlag)
	}

	// First immediate poll
	pollAndPrint()

	for {
		if *countFlag > 0 && iteration >= *countFlag {
			return
		}

		select {
		case <-sigCh:
			fmt.Println("\nStopped status polling.")
			return
		case <-ticker.C:
			pollAndPrint()
		}
	}
}

func pollNodes(client *http.Client, endpoints []string) []NodeResult {
	results := make([]NodeResult, len(endpoints))
	var wg sync.WaitGroup

	for i, ep := range endpoints {
		wg.Add(1)
		go func(idx int, url string) {
			defer wg.Done()
			res := NodeResult{Endpoint: url}

			resp, err := client.Get(url)
			if err != nil {
				res.Err = err
				results[idx] = res
				return
			}
			defer resp.Body.Close()

			if resp.StatusCode != http.StatusOK {
				res.Err = fmt.Errorf("HTTP %d", resp.StatusCode)
				results[idx] = res
				return
			}

			var status NodeStatus
			if err := json.NewDecoder(resp.Body).Decode(&status); err != nil {
				res.Err = fmt.Errorf("invalid json: %w", err)
				results[idx] = res
				return
			}

			res.Status = &status
			results[idx] = res
		}(i, ep)
	}

	wg.Wait()

	// Sort results primarily by Node ID if available, otherwise by endpoint
	sort.Slice(results, func(i, j int) bool {
		if results[i].Status != nil && results[j].Status != nil {
			return results[i].Status.ID < results[j].Status.ID
		}
		if results[i].Status != nil {
			return true
		}
		if results[j].Status != nil {
			return false
		}
		return results[i].Endpoint < results[j].Endpoint
	})

	return results
}

func printTable(iteration int, results []NodeResult, clearScreen bool) {
	if clearScreen {
		// ANSI escape sequence to clear screen and move cursor to top-left
		fmt.Print("\033[H\033[2J")
	}

	now := time.Now().Format("2006-01-02 15:04:05")
	fmt.Printf("\n--- [Iteration #%d | %s] Raft Cluster Status ---\n", iteration, now)
	fmt.Printf("+---------+-----------+------+----------+------------+----------------------------------+\n")
	fmt.Printf("| Node ID |   State   | Term | VotedFor | Status     | Endpoint                         |\n")
	fmt.Printf("+---------+-----------+------+----------+------------+----------------------------------+\n")

	for _, res := range results {
		if res.Err != nil || res.Status == nil {
			fmt.Printf("| %-7s | %-9s | %-4s | %-8s | %-10s | %-32s |\n",
				"-", "-", "-", "-", "OFFLINE", truncate(res.Endpoint, 32),
			)
			continue
		}

		s := res.Status
		votedForStr := fmt.Sprintf("%d", s.VotedFor)
		if s.VotedFor == -1 {
			votedForStr = "none (-1)"
		} else if s.VotedFor == s.ID {
			votedForStr = fmt.Sprintf("self (%d)", s.ID)
		}

		nodeIDStr := fmt.Sprintf("Node %d", s.ID)
		fmt.Printf("| %-7s | %-9s | %-4d | %-8s | %-10s | %-32s |\n",
			nodeIDStr, s.State, s.Term, votedForStr, "ONLINE", truncate(res.Endpoint, 32),
		)
	}

	fmt.Printf("+---------+-----------+------+----------+------------+----------------------------------+\n")
}

func truncate(s string, maxLen int) string {
	if len(s) > maxLen {
		return s[:maxLen-3] + "..."
	}
	return s
}

