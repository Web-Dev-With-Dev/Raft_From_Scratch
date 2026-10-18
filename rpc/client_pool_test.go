package rpc

import (
	"context"
	"net"
	"testing"
	"time"

	raftpb "raft/proto"

	"google.golang.org/grpc"
	"google.golang.org/grpc/connectivity"
)

type mockRaftServiceServer struct {
	raftpb.UnimplementedRaftServiceServer
}

func (m *mockRaftServiceServer) RequestVote(ctx context.Context, req *raftpb.RequestVoteArgs) (*raftpb.RequestVoteReply, error) {
	return &raftpb.RequestVoteReply{
		Term:        req.Term,
		VoteGranted: true,
	}, nil
}

func (m *mockRaftServiceServer) AppendEntries(ctx context.Context, req *raftpb.AppendEntriesArgs) (*raftpb.AppendEntriesReply, error) {
	return &raftpb.AppendEntriesReply{
		Term:    req.Term,
		Success: true,
	}, nil
}

func TestClientPool_RegisterAndGet(t *testing.T) {
	pool := NewClientPool()

	// Initially empty
	if _, ok := pool.Get(1); ok {
		t.Fatalf("expected node 1 to not exist in empty pool")
	}

	// Mock server
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("failed to listen: %v", err)
	}
	defer lis.Close()

	grpcServer := grpc.NewServer()
	raftpb.RegisterRaftServiceServer(grpcServer, &mockRaftServiceServer{})
	go func() {
		_ = grpcServer.Serve(lis)
	}()
	defer grpcServer.Stop()

	client, err := pool.Connect(1, lis.Addr().String())
	if err != nil {
		t.Fatalf("failed to connect: %v", err)
	}

	if client == nil {
		t.Fatalf("expected non-nil client")
	}

	// Verify Get
	retrieved, ok := pool.Get(1)
	if !ok || retrieved == nil {
		t.Fatalf("expected to retrieve client for node 1")
	}

	// Verify RPC call works
	resp, err := retrieved.RequestVote(context.Background(), &raftpb.RequestVoteArgs{
		Term:        1,
		CandidateId: 2,
	})
	if err != nil {
		t.Fatalf("RequestVote failed: %v", err)
	}
	if !resp.VoteGranted {
		t.Fatalf("expected vote to be granted")
	}

	// Verify Peers
	peers := pool.Peers()
	if len(peers) != 1 || peers[0] != 1 {
		t.Fatalf("unexpected peers: %v", peers)
	}

	// Close peer
	if err := pool.ClosePeer(1); err != nil {
		t.Fatalf("failed to close peer: %v", err)
	}

	if _, ok := pool.Get(1); ok {
		t.Fatalf("expected node 1 to be removed after ClosePeer")
	}
}

func TestClientPool_Close(t *testing.T) {
	pool := NewClientPool()

	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("failed to listen: %v", err)
	}
	defer lis.Close()

	grpcServer := grpc.NewServer()
	raftpb.RegisterRaftServiceServer(grpcServer, &mockRaftServiceServer{})
	go func() {
		_ = grpcServer.Serve(lis)
	}()
	defer grpcServer.Stop()

	_, err = pool.Connect(1, lis.Addr().String())
	if err != nil {
		t.Fatalf("failed to connect: %v", err)
	}

	if err := pool.Close(); err != nil {
		t.Fatalf("failed to close pool: %v", err)
	}

	if len(pool.Peers()) != 0 {
		t.Fatalf("expected 0 peers after Close")
	}
}

func TestRetryPolicy_BackoffCalculation(t *testing.T) {
	policy := RetryPolicy{
		MaxRetries:     4,
		InitialBackoff: 20 * time.Millisecond,
		MaxBackoff:     100 * time.Millisecond,
		Multiplier:     2.0,
		DialTimeout:    100 * time.Millisecond,
		ProbeTCP:       true,
	}

	if policy.BackoffForAttempt(0) != 20*time.Millisecond {
		t.Fatalf("expected 20ms for attempt 0, got %v", policy.BackoffForAttempt(0))
	}
	if policy.BackoffForAttempt(1) != 40*time.Millisecond {
		t.Fatalf("expected 40ms for attempt 1, got %v", policy.BackoffForAttempt(1))
	}
	if policy.BackoffForAttempt(2) != 80*time.Millisecond {
		t.Fatalf("expected 80ms for attempt 2, got %v", policy.BackoffForAttempt(2))
	}
	// Capped at MaxBackoff (100ms)
	if policy.BackoffForAttempt(3) != 100*time.Millisecond {
		t.Fatalf("expected 100ms capped for attempt 3, got %v", policy.BackoffForAttempt(3))
	}
}

func TestClientPool_PersistentPeerConnAndState(t *testing.T) {
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("failed to listen: %v", err)
	}
	defer lis.Close()

	grpcServer := grpc.NewServer()
	raftpb.RegisterRaftServiceServer(grpcServer, &mockRaftServiceServer{})
	go func() {
		_ = grpcServer.Serve(lis)
	}()
	defer grpcServer.Stop()

	pool := NewClientPool()
	defer pool.Close()

	_, err = pool.Connect(1, lis.Addr().String())
	if err != nil {
		t.Fatalf("failed to connect: %v", err)
	}

	// Verify PeerConn metadata
	pc, ok := pool.GetPeerConn(1)
	if !ok || pc == nil {
		t.Fatalf("expected PeerConn for peer 1")
	}
	if pc.Address != lis.Addr().String() {
		t.Fatalf("unexpected address in PeerConn: %s", pc.Address)
	}

	// Verify address lookup
	addr, ok := pool.GetAddress(1)
	if !ok || addr != lis.Addr().String() {
		t.Fatalf("expected address %s, got %s (ok=%v)", lis.Addr().String(), addr, ok)
	}

	// Verify connectivity state
	state, ok := pool.GetState(1)
	if !ok {
		t.Fatalf("expected GetState to succeed")
	}
	if state == connectivity.Shutdown {
		t.Fatalf("expected active connection, got Shutdown")
	}

	if !pool.IsConnected(1) {
		t.Fatalf("expected IsConnected to be true")
	}

	// Reconnect
	reconnectedClient, err := pool.Reconnect(1)
	if err != nil {
		t.Fatalf("Reconnect failed: %v", err)
	}
	if reconnectedClient == nil {
		t.Fatalf("expected non-nil reconnected client")
	}
}

func TestClientPool_ConnectWithRetry_SuccessOnRetry(t *testing.T) {
	tempLis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("failed to get free port: %v", err)
	}
	targetAddr := tempLis.Addr().String()
	tempLis.Close()

	pool := NewClientPool()
	defer pool.Close()

	policy := RetryPolicy{
		MaxRetries:     5,
		InitialBackoff: 25 * time.Millisecond,
		MaxBackoff:     200 * time.Millisecond,
		Multiplier:     2.0,
		DialTimeout:    100 * time.Millisecond,
		ProbeTCP:       true,
	}

	go func() {
		time.Sleep(50 * time.Millisecond)
		lis, err := net.Listen("tcp", targetAddr)
		if err != nil {
			return
		}
		defer lis.Close()

		grpcServer := grpc.NewServer()
		raftpb.RegisterRaftServiceServer(grpcServer, &mockRaftServiceServer{})
		_ = grpcServer.Serve(lis)
	}()

	client, err := pool.ConnectWithRetry(context.Background(), 2, targetAddr, policy)
	if err != nil {
		t.Fatalf("expected ConnectWithRetry to succeed after retries, got: %v", err)
	}
	if client == nil {
		t.Fatalf("expected non-nil client")
	}
}

func TestClientPool_ConnectWithRetry_ExhaustRetries(t *testing.T) {
	pool := NewClientPool()
	defer pool.Close()

	policy := RetryPolicy{
		MaxRetries:     2,
		InitialBackoff: 10 * time.Millisecond,
		MaxBackoff:     50 * time.Millisecond,
		Multiplier:     2.0,
		DialTimeout:    50 * time.Millisecond,
		ProbeTCP:       true,
	}

	_, err := pool.ConnectWithRetry(context.Background(), 99, "127.0.0.1:54321", policy)
	if err == nil {
		t.Fatalf("expected dial failure error after exhausting retries")
	}
}

func TestClientPool_ConnectWithRetry_ContextCancelled(t *testing.T) {
	pool := NewClientPool()
	defer pool.Close()

	ctx, cancel := context.WithCancel(context.Background())
	cancel() // Cancel immediately

	policy := DefaultRetryPolicy()
	_, err := pool.ConnectWithRetry(ctx, 100, "127.0.0.1:54322", policy)
	if err == nil {
		t.Fatalf("expected error on cancelled context")
	}
}

