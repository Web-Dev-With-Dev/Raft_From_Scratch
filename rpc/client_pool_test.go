package rpc

import (
	"context"
	"net"
	"testing"

	raftpb "raft/proto"

	"google.golang.org/grpc"
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
