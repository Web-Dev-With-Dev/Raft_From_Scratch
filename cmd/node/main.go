package main

import (
	"context"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"raft/config"
	"raft/logger"
	"raft/raft"
	"raft/rpc"
	"raft/server"
)

type cliFlags struct {
	nodeID     int
	configPath string
	statusAddr string
	logLevel   string
}

func parseCLIFlags() cliFlags {
	nodeIDFlag := flag.Int("id", 1, "Node ID for this Raft instance (e.g. 1, 2, 3)")
	configPathFlag := flag.String("config", "cluster.json", "Path to cluster configuration JSON file")
	statusAddrFlag := flag.String("status-addr", "", "Optional HTTP address for debug /status endpoint (e.g. 127.0.0.1:9001)")
	logLevelFlag := flag.String("log-level", "info", "Log level: debug, info, warn, error")
	flag.Parse()
	return cliFlags{
		nodeID:     *nodeIDFlag,
		configPath: *configPathFlag,
		statusAddr: *statusAddrFlag,
		logLevel:   *logLevelFlag,
	}
}

func main() {
	flags := parseCLIFlags()
	id := raft.NodeID(flags.nodeID)
	nodeLogger := logger.NewNodeLogger(id, os.Stderr, &logger.Options{
		Level: parseLogLevel(flags.logLevel),
	})

	cfg, err := config.LoadClusterConfig(flags.configPath)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error loading cluster config from %q: %v\n", flags.configPath, err)
		os.Exit(1)
	}

	nodeCfg, found := cfg.GetNode(id)
	if !found {
		fmt.Fprintf(os.Stderr, "Error: node ID %d not found in cluster configuration %q\n", id, flags.configPath)
		os.Exit(1)
	}

	peers := cfg.PeersFor(id)
	srv := server.NewServer(id, peers, nil, nil)
	srv.SetLogger(nodeLogger)

	if err := srv.Start(nodeCfg.Address); err != nil {
		srv.GetLogger().Error("failed to start node server", "error", err)
		os.Exit(1)
	}

	startDebugHTTPServer(srv, id, flags.statusAddr)

	dialCtx, dialCancel := context.WithCancel(context.Background())
	defer dialCancel()

	go connectAndVerifyPeers(dialCtx, srv, id, peers)
	waitForShutdown(srv, dialCancel)
}

func parseLogLevel(levelStr string) slog.Level {
	switch strings.ToLower(levelStr) {
	case "debug":
		return slog.LevelDebug
	case "warn", "warning":
		return slog.LevelWarn
	case "error":
		return slog.LevelError
	default:
		return slog.LevelInfo
	}
}

func startDebugHTTPServer(srv *server.Server, id raft.NodeID, addrFlag string) {
	statusAddr := addrFlag
	if statusAddr == "" {
		statusAddr = fmt.Sprintf("127.0.0.1:%d", 9000+int(id))
	}
	if statusAddr == "disabled" || statusAddr == "none" {
		return
	}
	if err := srv.StartHTTPServer(statusAddr); err != nil {
		srv.GetLogger().Warn("failed to start debug HTTP status server", "address", statusAddr, "error", err)
	} else {
		srv.GetLogger().Info("started debug HTTP status server", "address", statusAddr, "endpoint", fmt.Sprintf("http://%s/status", statusAddr))
	}
}

func connectAndVerifyPeers(ctx context.Context, srv *server.Server, id raft.NodeID, peers []server.Peer) {
	connCtx, cancel := context.WithTimeout(ctx, 15*time.Second)
	defer cancel()

	policy := rpc.DefaultRetryPolicy()
	policy.MaxRetries = 10
	policy.InitialBackoff = 100 * time.Millisecond
	policy.MaxBackoff = 1 * time.Second

	if err := srv.ConnectPeersWithRetry(connCtx, policy); err != nil {
		srv.GetLogger().Warn("peer connection phase ended with incomplete connections", "error", err)
		return
	}
	srv.GetLogger().Info("all cluster peers connected successfully")

	time.Sleep(150 * time.Millisecond)
	verifyPeerReachability(ctx, srv, id, peers)
}

func verifyPeerReachability(ctx context.Context, srv *server.Server, id raft.NodeID, peers []server.Peer) {
	srv.GetLogger().Info("starting peer RPC reachability verification", "peer_count", len(peers))
	for _, peer := range peers {
		select {
		case <-ctx.Done():
			return
		default:
		}

		rpcCtx, rpcCancel := context.WithTimeout(ctx, srv.RPCTimeout())
		reply, err := srv.SendRequestVote(rpcCtx, peer, &raft.RequestVoteArgs{
			Term:         0,
			CandidateID:  id,
			LastLogIndex: 0,
			LastLogTerm:  0,
		})
		rpcCancel()

		if err != nil {
			srv.GetLogger().Warn("peer reachability RPC failed", "peer_id", int(peer.ID), "address", peer.Address, "error", err)
		} else {
			srv.GetLogger().Info("peer reachability verified via RPC",
				"peer_id", int(peer.ID),
				"address", peer.Address,
				"reply_term", reply.Term,
				"vote_granted", reply.VoteGranted,
			)
		}
	}
}

func waitForShutdown(srv *server.Server, cancel context.CancelFunc) {
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, os.Interrupt, syscall.SIGTERM)
	sig := <-sigCh

	cancel()
	srv.GetLogger().Info("received termination signal, shutting down", "signal", sig.String())
	if err := srv.Close(); err != nil {
		srv.GetLogger().Error("error closing server", "error", err)
	}
	srv.GetLogger().Info("node shutdown complete")
}
