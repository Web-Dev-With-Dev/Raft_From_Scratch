package server

import (
	"context"
	"net"
	"testing"

	raftpb "raft/proto"
	"raft/raft"
	"raft/rpc"

	"google.golang.org/grpc"
)

func TestNewServer(t *testing.T) {
	id := raft.NodeID(1)
	peers := []Peer{
		{ID: 2, Address: "localhost:8002"},
		{ID: 3, Address: "localhost:8003"},
	}

	ps := &raft.PersistentState{
		CurrentTerm: 1,
		VotedFor:    2,
		Log: []raft.LogEntry{
			{Term: 1, Index: 1, Command: "set x=1"},
		},
	}

	pool := rpc.NewClientPool()
	srv := NewServer(id, peers, ps, pool)

	if srv.ID != id {
		t.Fatalf("expected ID %d, got %d", id, srv.ID)
	}

	if len(srv.Peers) != 2 {
		t.Fatalf("expected 2 peers, got %d", len(srv.Peers))
	}

	if srv.PersistentState.CurrentTerm != 1 {
		t.Fatalf("expected CurrentTerm 1, got %d", srv.PersistentState.CurrentTerm)
	}

	if srv.PersistentState.VotedFor != 2 {
		t.Fatalf("expected VotedFor 2, got %d", srv.PersistentState.VotedFor)
	}

	if len(srv.PersistentState.Log) != 1 {
		t.Fatalf("expected 1 log entry, got %d", len(srv.PersistentState.Log))
	}

	if srv.RPCClientPool != pool {
		t.Fatalf("expected matching RPCClientPool")
	}

	if srv.ClientPool() != pool {
		t.Fatalf("expected matching ClientPool() return")
	}
}

func TestNewServer_Defaults(t *testing.T) {
	srv := NewServer(1, nil, nil, nil)

	if srv.ID != 1 {
		t.Fatalf("expected ID 1, got %d", srv.ID)
	}

	if srv.PersistentState == nil {
		t.Fatalf("expected non-nil default PersistentState")
	}

	if srv.PersistentState.CurrentTerm != 0 {
		t.Fatalf("expected default CurrentTerm 0, got %d", srv.PersistentState.CurrentTerm)
	}

	if srv.PersistentState.VotedFor != -1 {
		t.Fatalf("expected default VotedFor -1, got %d", srv.PersistentState.VotedFor)
	}

	if srv.RPCClientPool == nil {
		t.Fatalf("expected non-nil default RPCClientPool")
	}
}

func TestServer_PeerManagement(t *testing.T) {
	srv := NewServer(1, []Peer{
		{ID: 2, Address: "localhost:8002"},
	}, nil, nil)

	if len(srv.PeerIDs()) != 1 || srv.PeerIDs()[0] != 2 {
		t.Fatalf("unexpected PeerIDs: %v", srv.PeerIDs())
	}

	peer, found := srv.GetPeer(2)
	if !found || peer.Address != "localhost:8002" {
		t.Fatalf("expected peer 2 with address localhost:8002, got %v", peer)
	}

	_, found = srv.GetPeer(3)
	if found {
		t.Fatalf("expected peer 3 to not be found")
	}

	srv.AddPeer(Peer{ID: 3, Address: "localhost:8003"})
	if len(srv.GetPeers()) != 2 {
		t.Fatalf("expected 2 peers after AddPeer")
	}

	// Update existing peer address
	srv.AddPeer(Peer{ID: 2, Address: "localhost:9002"})
	peer, found = srv.GetPeer(2)
	if !found || peer.Address != "localhost:9002" {
		t.Fatalf("expected updated address localhost:9002, got %v", peer)
	}
}

type dummyRaftServer struct {
	raftpb.UnimplementedRaftServiceServer
}

func TestServer_ConnectPeersAndClose(t *testing.T) {
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("failed to listen: %v", err)
	}
	defer lis.Close()

	grpcServer := grpc.NewServer()
	raftpb.RegisterRaftServiceServer(grpcServer, &dummyRaftServer{})
	go func() {
		_ = grpcServer.Serve(lis)
	}()
	defer grpcServer.Stop()

	srv := NewServer(1, []Peer{
		{ID: 1, Address: "127.0.0.1:8001"}, // self
		{ID: 2, Address: lis.Addr().String()},
	}, nil, nil)

	if err := srv.ConnectPeers(); err != nil {
		t.Fatalf("ConnectPeers failed: %v", err)
	}

	client, ok := srv.RPCClientPool.Get(2)
	if !ok || client == nil {
		t.Fatalf("expected peer 2 to be in RPC client pool")
	}

	// Self should not be dialed
	if _, ok := srv.RPCClientPool.Get(1); ok {
		t.Fatalf("expected self (ID 1) to not be dialed")
	}

	if err := srv.Close(); err != nil {
		t.Fatalf("Close failed: %v", err)
	}
}

func TestServer_StartAndStop(t *testing.T) {
	srv := NewServer(1, nil, nil, nil)

	if srv.IsRunning() {
		t.Fatalf("expected server to not be running initially")
	}

	if err := srv.Start("127.0.0.1:0"); err != nil {
		t.Fatalf("failed to start server: %v", err)
	}

	if !srv.IsRunning() {
		t.Fatalf("expected server to be running")
	}

	if srv.Addr() == nil {
		t.Fatalf("expected non-nil Addr()")
	}

	if srv.GRPCServer() == nil {
		t.Fatalf("expected non-nil GRPCServer()")
	}

	// Calling start again should fail
	if err := srv.Start("127.0.0.1:0"); err == nil {
		t.Fatalf("expected error when starting an already running server")
	}

	srv.Stop()

	if srv.IsRunning() {
		t.Fatalf("expected server to not be running after Stop")
	}

	if srv.Addr() != nil {
		t.Fatalf("expected nil Addr() after Stop")
	}
}

func TestServer_ServeAndGracefulStop(t *testing.T) {
	srv := NewServer(1, nil, nil, nil)

	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("failed to listen: %v", err)
	}

	if err := srv.Serve(lis); err != nil {
		t.Fatalf("failed to serve: %v", err)
	}

	if !srv.IsRunning() {
		t.Fatalf("expected server to be running")
	}

	srv.GracefulStop()

	if srv.IsRunning() {
		t.Fatalf("expected server to not be running after GracefulStop")
	}
}

func TestServer_RaftServiceHandlers(t *testing.T) {
	ps := &raft.PersistentState{
		CurrentTerm: 42,
		VotedFor:    -1,
		Log:         make([]raft.LogEntry, 0),
	}
	srv := NewServer(1, nil, ps, nil)

	if err := srv.Start("127.0.0.1:0"); err != nil {
		t.Fatalf("failed to start server: %v", err)
	}
	defer srv.Close()

	// Connect to the running server via a ClientPool
	pool := rpc.NewClientPool()
	defer pool.Close()

	client, err := pool.Connect(1, srv.Addr().String())
	if err != nil {
		t.Fatalf("failed to connect to server: %v", err)
	}

	// Test RequestVote handler
	rvReply, err := client.RequestVote(context.Background(), &raftpb.RequestVoteArgs{
		Term:         42,
		CandidateId:  2,
		LastLogIndex: 0,
		LastLogTerm:  0,
	})
	if err != nil {
		t.Fatalf("RequestVote failed: %v", err)
	}
	if rvReply.Term != 42 {
		t.Fatalf("expected Term 42, got %d", rvReply.Term)
	}
	if rvReply.VoteGranted {
		t.Fatalf("expected VoteGranted false for empty stub handler")
	}

	// Test AppendEntries handler
	aeReply, err := client.AppendEntries(context.Background(), &raftpb.AppendEntriesArgs{
		Term:         42,
		LeaderId:     2,
		PrevLogIndex: 0,
		PrevLogTerm:  0,
	})
	if err != nil {
		t.Fatalf("AppendEntries failed: %v", err)
	}
	if aeReply.Term != 42 {
		t.Fatalf("expected Term 42, got %d", aeReply.Term)
	}
	if aeReply.Success {
		t.Fatalf("expected Success false for empty stub handler")
	}
}

func TestServer_ClientSideRPCStubs(t *testing.T) {
	// Start target peer server
	targetServer := NewServer(2, nil, &raft.PersistentState{
		CurrentTerm: 10,
		VotedFor:    -1,
		Log:         make([]raft.LogEntry, 0),
	}, nil)

	if err := targetServer.Start("127.0.0.1:0"); err != nil {
		t.Fatalf("failed to start target server: %v", err)
	}
	defer targetServer.Close()

	// Start local server that dials targetServer
	peer2 := Peer{ID: 2, Address: targetServer.Addr().String()}
	localServer := NewServer(1, []Peer{peer2}, nil, nil)
	defer localServer.Close()

	// 1. SendRequestVote
	rvReply, err := localServer.SendRequestVote(context.Background(), peer2, &raft.RequestVoteArgs{
		Term:         10,
		CandidateID:  1,
		LastLogIndex: 0,
		LastLogTerm:  0,
	})
	if err != nil {
		t.Fatalf("SendRequestVote failed: %v", err)
	}
	if rvReply.Term != 10 {
		t.Fatalf("expected Term 10, got %d", rvReply.Term)
	}

	// 2. sendRequestVote (convenience stub)
	rvReply2, err := localServer.sendRequestVote(peer2, &raft.RequestVoteArgs{
		Term:         10,
		CandidateID:  1,
		LastLogIndex: 0,
		LastLogTerm:  0,
	})
	if err != nil {
		t.Fatalf("sendRequestVote failed: %v", err)
	}
	if rvReply2.Term != 10 {
		t.Fatalf("expected Term 10, got %d", rvReply2.Term)
	}

	// 3. SendAppendEntries
	aeReply, err := localServer.SendAppendEntries(context.Background(), peer2, &raft.AppendEntriesArgs{
		Term:         10,
		LeaderID:     1,
		PrevLogIndex: 0,
		PrevLogTerm:  0,
		Entries:      nil,
		LeaderCommit: 0,
	})
	if err != nil {
		t.Fatalf("SendAppendEntries failed: %v", err)
	}
	if aeReply.Term != 10 {
		t.Fatalf("expected Term 10, got %d", aeReply.Term)
	}

	// 4. sendAppendEntries (convenience stub)
	aeReply2, err := localServer.sendAppendEntries(peer2, &raft.AppendEntriesArgs{
		Term:     10,
		LeaderID: 1,
	})
	if err != nil {
		t.Fatalf("sendAppendEntries failed: %v", err)
	}
	if aeReply2.Term != 10 {
		t.Fatalf("expected Term 10, got %d", aeReply2.Term)
	}

	// 5. SendRequestVoteToNode & SendAppendEntriesToNode
	rvReply3, err := localServer.SendRequestVoteToNode(context.Background(), 2, &raft.RequestVoteArgs{Term: 10})
	if err != nil {
		t.Fatalf("SendRequestVoteToNode failed: %v", err)
	}
	if rvReply3.Term != 10 {
		t.Fatalf("expected Term 10, got %d", rvReply3.Term)
	}

	aeReply3, err := localServer.SendAppendEntriesToNode(context.Background(), 2, &raft.AppendEntriesArgs{Term: 10})
	if err != nil {
		t.Fatalf("SendAppendEntriesToNode failed: %v", err)
	}
	if aeReply3.Term != 10 {
		t.Fatalf("expected Term 10, got %d", aeReply3.Term)
	}

	// Error when node does not exist
	_, err = localServer.SendRequestVoteToNode(context.Background(), 99, &raft.RequestVoteArgs{})
	if err == nil {
		t.Fatalf("expected error for non-existent node ID")
	}
}

func TestServer_ConnectPeersWithRetry(t *testing.T) {
	// Start target peer
	peerServer := NewServer(2, nil, nil, nil)
	if err := peerServer.Start("127.0.0.1:0"); err != nil {
		t.Fatalf("failed to start peer server: %v", err)
	}
	defer peerServer.Close()

	srv := NewServer(1, []Peer{
		{ID: 1, Address: "127.0.0.1:8001"}, // self (skipped)
		{ID: 2, Address: peerServer.Addr().String()},
	}, nil, nil)
	defer srv.Close()

	policy := rpc.DefaultRetryPolicy()
	srv.SetRetryPolicy(policy)

	if err := srv.ConnectPeersWithRetry(context.Background(), policy); err != nil {
		t.Fatalf("ConnectPeersWithRetry failed: %v", err)
	}

	if !srv.RPCClientPool.IsConnected(2) {
		t.Fatalf("expected peer 2 to be persistently connected")
	}
}



