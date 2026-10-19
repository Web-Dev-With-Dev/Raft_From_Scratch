package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"syscall"
	"time"

	"raft/config"
	"raft/raft"
	"raft/rpc"
	"raft/server"
)

func main() {
	nodeIDFlag := flag.Int("id", 1, "Node ID for this Raft instance (e.g. 1, 2, 3)")
	configPathFlag := flag.String("config", "cluster.json", "Path to cluster configuration JSON file")
	flag.Parse()

	id := raft.NodeID(*nodeIDFlag)

	// Load cluster configuration
	cfg, err := config.LoadClusterConfig(*configPathFlag)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error loading cluster config from %q: %v\n", *configPathFlag, err)
		os.Exit(1)
	}

	// Retrieve this node's address
	nodeCfg, found := cfg.GetNode(id)
	if !found {
		fmt.Fprintf(os.Stderr, "Error: node ID %d not found in cluster configuration %q\n", id, *configPathFlag)
		os.Exit(1)
	}

	// Retrieve remote peers (excluding self)
	peers := cfg.PeersFor(id)

	// Initialize the server
	srv := server.NewServer(id, peers, nil, nil)

	// Start listening and serving gRPC
	if err := srv.Start(nodeCfg.Address); err != nil {
		srv.GetLogger().Error("failed to start node server", "error", err)
		os.Exit(1)
	}

	// Concurrently dial peers with retry/backoff
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer cancel()

		policy := rpc.DefaultRetryPolicy()
		policy.MaxRetries = 10
		policy.InitialBackoff = 100 * time.Millisecond
		policy.MaxBackoff = 1 * time.Second

		if err := srv.ConnectPeersWithRetry(ctx, policy); err != nil {
			srv.GetLogger().Warn("peer connection phase ended with incomplete connections", "error", err)
		} else {
			srv.GetLogger().Info("all cluster peers connected successfully")
		}
	}()

	// Wait for OS shutdown signal (Ctrl+C / SIGTERM)
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, os.Interrupt, syscall.SIGTERM)
	sig := <-sigCh

	srv.GetLogger().Info("received termination signal, shutting down", "signal", sig.String())
	if err := srv.Close(); err != nil {
		srv.GetLogger().Error("error closing server", "error", err)
	}
	srv.GetLogger().Info("node shutdown complete")
}
