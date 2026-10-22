package server

import (
	"bytes"
	"context"
	"net"
	"strings"
	"sync"
	"testing"
	"time"

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

type slowRaftServer struct {
	raftpb.UnimplementedRaftServiceServer
}

func (s *slowRaftServer) RequestVote(ctx context.Context, req *raftpb.RequestVoteArgs) (*raftpb.RequestVoteReply, error) {
	select {
	case <-time.After(500 * time.Millisecond):
		return &raftpb.RequestVoteReply{Term: req.Term, VoteGranted: false}, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

func TestServer_RPCTimeout_DeadPeerDoesNotBlock(t *testing.T) {
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("failed to listen: %v", err)
	}
	defer lis.Close()

	grpcServer := grpc.NewServer()
	raftpb.RegisterRaftServiceServer(grpcServer, &slowRaftServer{})
	go func() {
		_ = grpcServer.Serve(lis)
	}()
	defer grpcServer.Stop()

	peer2 := Peer{ID: 2, Address: lis.Addr().String()}
	srv := NewServer(1, []Peer{peer2}, nil, nil)
	defer srv.Close()

	// Configure a 100ms timeout
	srv.SetRPCTimeout(100 * time.Millisecond)

	// 1. sendRequestVote convenience method
	start := time.Now()
	_, err = srv.sendRequestVote(peer2, &raft.RequestVoteArgs{Term: 1})
	elapsed := time.Since(start)

	if err == nil {
		t.Fatalf("expected timeout error for dead/hanging peer")
	}

	if !rpc.IsTimeout(err) {
		t.Fatalf("expected rpc.IsTimeout(err) to be true, got: %v", err)
	}

	if elapsed >= 450*time.Millisecond {
		t.Fatalf("expected call to unblock around 100ms, but took %v", elapsed)
	}

	// 2. SendRequestVote with context.Background() (no explicit deadline from caller)
	start = time.Now()
	_, err = srv.SendRequestVote(context.Background(), peer2, &raft.RequestVoteArgs{Term: 1})
	elapsed = time.Since(start)

	if err == nil {
		t.Fatalf("expected timeout error when caller passes context without deadline")
	}

	if !rpc.IsTimeout(err) {
		t.Fatalf("expected rpc.IsTimeout(err) to be true, got: %v", err)
	}

	if elapsed >= 450*time.Millisecond {
		t.Fatalf("expected call to unblock around 100ms, but took %v", elapsed)
	}
}

func TestServer_StructuredLoggingWithNodePrefix(t *testing.T) {
	buf := &bytes.Buffer{}
	nodeID := raft.NodeID(5)

	// Create logger capturing output to buffer without timestamps
	testLogger := NewNodeLogger(nodeID, buf, &LogOptions{
		DisableTime: true,
	})

	srv := NewServer(nodeID, nil, nil, nil)
	srv.SetLogger(testLogger)

	if srv.GetLogger() != testLogger {
		t.Fatalf("expected GetLogger to return configured testLogger")
	}

	// 1. Add peer (should log)
	srv.AddPeer(Peer{ID: 2, Address: "127.0.0.1:8002"})

	// 2. Start and Stop server (should log)
	if err := srv.Start("127.0.0.1:0"); err != nil {
		t.Fatalf("failed to start server: %v", err)
	}

	srv.Stop()

	output := buf.String()
	lines := strings.Split(strings.TrimSpace(output), "\n")

	if len(lines) == 0 {
		t.Fatalf("expected log output, got none")
	}

	expectedPrefix := "[Node 5] "
	for i, line := range lines {
		if !strings.HasPrefix(line, expectedPrefix) {
			t.Fatalf("log line %d does not have expected prefix %q: %q", i, expectedPrefix, line)
		}
		if !strings.Contains(line, "node_id=5") {
			t.Fatalf("log line %d missing structured node_id attribute: %q", i, line)
		}
	}

	// Verify specific logged events exist
	if !strings.Contains(output, "added peer") {
		t.Fatalf("expected 'added peer' log event, output:\n%s", output)
	}
	if !strings.Contains(output, "starting gRPC server") {
		t.Fatalf("expected 'starting gRPC server' log event, output:\n%s", output)
	}
	if !strings.Contains(output, "gRPC server listening") {
		t.Fatalf("expected 'gRPC server listening' log event, output:\n%s", output)
	}
	if !strings.Contains(output, "stopped gRPC server") {
		t.Fatalf("expected 'stopped gRPC server' log event, output:\n%s", output)
	}
}

func TestCluster_FullMeshRPCReachability(t *testing.T) {
	// Start 3 server instances listening on dynamic local ports
	servers := make([]*Server, 3)
	for i := 0; i < 3; i++ {
		id := raft.NodeID(i + 1)
		srv := NewServer(id, nil, nil, nil)
		if err := srv.Start("127.0.0.1:0"); err != nil {
			t.Fatalf("failed to start server %d: %v", id, err)
		}
		defer srv.Close()
		servers[i] = srv
	}

	// Register peers across all nodes
	for i := 0; i < 3; i++ {
		for j := 0; j < 3; j++ {
			if i != j {
				servers[i].AddPeer(Peer{
					ID:      servers[j].ID,
					Address: servers[j].Addr().String(),
				})
			}
		}
	}

	// Connect all peers
	for _, srv := range servers {
		if err := srv.ConnectPeers(); err != nil {
			t.Fatalf("node %d failed to connect peers: %v", srv.ID, err)
		}
	}

	// Verify all 6 directed pairs (every node reaches every other node)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	for _, sender := range servers {
		for _, target := range servers {
			if sender.ID == target.ID {
				continue
			}

			reply, err := sender.SendRequestVoteToNode(ctx, target.ID, &raft.RequestVoteArgs{
				Term:        1,
				CandidateID: sender.ID,
			})
			if err != nil {
				t.Fatalf("node %d failed to reach node %d via RequestVote: %v", sender.ID, target.ID, err)
			}
			if reply.Term != 0 {
				t.Fatalf("unexpected reply term from node %d: %d", target.ID, reply.Term)
			}

			aeReply, err := sender.SendAppendEntriesToNode(ctx, target.ID, &raft.AppendEntriesArgs{
				Term:     1,
				LeaderID: sender.ID,
			})
			if err != nil {
				t.Fatalf("node %d failed to reach node %d via AppendEntries: %v", sender.ID, target.ID, err)
			}
			if aeReply.Term != 0 {
				t.Fatalf("unexpected reply term from node %d: %d", target.ID, aeReply.Term)
			}
		}
	}
}

func TestServer_ElectionTimerAndState(t *testing.T) {
	srv := NewServer(1, nil, nil, nil)
	defer srv.Close()

	if srv.GetState() != raft.Follower {
		t.Fatalf("expected initial state Follower, got %v", srv.GetState())
	}

	srv.SetState(raft.Candidate)
	if srv.GetState() != raft.Candidate {
		t.Fatalf("expected state Candidate, got %v", srv.GetState())
	}

	srv.SetState(raft.Leader)
	if srv.GetState() != raft.Leader {
		t.Fatalf("expected state Leader, got %v", srv.GetState())
	}

	et := srv.GetElectionTimer()
	if et == nil {
		t.Fatal("expected non-nil ElectionTimer on server")
	}

	dur := et.Duration()
	if dur < raft.DefaultMinElectionTimeout || dur > raft.DefaultMaxElectionTimeout {
		t.Fatalf("server election timeout %v outside [%v, %v]", dur, raft.DefaultMinElectionTimeout, raft.DefaultMaxElectionTimeout)
	}

	newDur := srv.ResetElectionTimeout()
	if newDur < raft.DefaultMinElectionTimeout || newDur > raft.DefaultMaxElectionTimeout {
		t.Fatalf("reset election timeout %v outside [%v, %v]", newDur, raft.DefaultMinElectionTimeout, raft.DefaultMaxElectionTimeout)
	}
}

func TestServer_TimerResetOnValidHeartbeat(t *testing.T) {
	srv := NewServer(1, nil, &raft.PersistentState{
		CurrentTerm: 5,
		VotedFor:    -1,
		Log:         make([]raft.LogEntry, 0),
	}, nil)
	defer srv.Close()

	et := srv.GetElectionTimer()
	initialReset := et.LastReset()

	time.Sleep(10 * time.Millisecond)

	// 1. Send valid heartbeat matching current term
	ok, newTimeout := srv.HandleHeartbeat(2, 5)
	if !ok {
		t.Fatalf("expected valid heartbeat to be accepted")
	}
	if newTimeout < raft.DefaultMinElectionTimeout || newTimeout > raft.DefaultMaxElectionTimeout {
		t.Fatalf("new timeout %v outside range", newTimeout)
	}
	if !et.LastReset().After(initialReset) {
		t.Fatalf("expected election timer LastReset to be updated after valid heartbeat")
	}
	if srv.LastHeartbeatTime().IsZero() {
		t.Fatalf("expected LastHeartbeatTime to be recorded")
	}

	// 2. Candidate reverts to follower on valid heartbeat
	srv.SetState(raft.Candidate)
	ok, _ = srv.HandleHeartbeat(2, 5)
	if !ok {
		t.Fatalf("expected heartbeat to be accepted")
	}
	if srv.GetState() != raft.Follower {
		t.Fatalf("expected Candidate to revert to Follower on valid heartbeat, got %v", srv.GetState())
	}
}

func TestServer_NoTimerResetOnStaleHeartbeat(t *testing.T) {
	srv := NewServer(1, nil, &raft.PersistentState{
		CurrentTerm: 10,
		VotedFor:    -1,
		Log:         make([]raft.LogEntry, 0),
	}, nil)
	defer srv.Close()

	et := srv.GetElectionTimer()
	lastResetBefore := et.LastReset()
	lastHbBefore := srv.LastHeartbeatTime()

	time.Sleep(10 * time.Millisecond)

	// Heartbeat with stale term (3 < 10) must be rejected without resetting timer
	ok, newTimeout := srv.HandleHeartbeat(2, 3)
	if ok {
		t.Fatalf("expected stale heartbeat to be rejected")
	}
	if newTimeout != 0 {
		t.Fatalf("expected 0 timeout for rejected heartbeat, got %v", newTimeout)
	}
	if et.LastReset() != lastResetBefore {
		t.Fatalf("election timer must NOT be reset on stale heartbeat")
	}
	if srv.LastHeartbeatTime() != lastHbBefore {
		t.Fatalf("lastHeartbeat must NOT be updated on stale heartbeat")
	}
}

func TestServer_PeriodicHeartbeatsSuppressElectionTimeout(t *testing.T) {
	srv := NewServer(1, nil, nil, nil)
	defer srv.Close()

	et := srv.GetElectionTimer()
	// Set a fast range for testing: 50-80ms
	et.SetRange(50*time.Millisecond, 80*time.Millisecond)
	et.Reset()

	// Send periodic heartbeats every 15ms for 100ms (longer than the 50-80ms election timeout)
	stopHeartbeats := make(chan struct{})
	doneHeartbeats := make(chan struct{})

	go func() {
		defer close(doneHeartbeats)
		ticker := time.NewTicker(15 * time.Millisecond)
		defer ticker.Stop()

		for {
			select {
			case <-stopHeartbeats:
				return
			case <-ticker.C:
				srv.HandleHeartbeat(2, 0)
			}
		}
	}()

	// During periodic heartbeats, timer must NOT fire
	select {
	case <-et.C():
		close(stopHeartbeats)
		t.Fatalf("election timer fired unexpectedly during active heartbeats")
	case <-time.After(100 * time.Millisecond):
		// Periodic heartbeats successfully suppressed timeout!
	}

	// Stop heartbeats now
	close(stopHeartbeats)
	<-doneHeartbeats

	// Now that heartbeats have stopped, timer MUST fire within 150ms
	select {
	case fireTime := <-et.C():
		if fireTime.IsZero() {
			t.Fatal("expected valid fireTime")
		}
	case <-time.After(200 * time.Millisecond):
		t.Fatal("election timer failed to fire after heartbeats ceased")
	}
}

func TestServer_AppendEntriesRPC_TriggersTimerReset(t *testing.T) {
	ps := &raft.PersistentState{
		CurrentTerm: 5,
		VotedFor:    -1,
		Log:         make([]raft.LogEntry, 0),
	}
	srv := NewServer(1, nil, ps, nil)
	if err := srv.Start("127.0.0.1:0"); err != nil {
		t.Fatalf("failed to start server: %v", err)
	}
	defer srv.Close()

	pool := rpc.NewClientPool()
	defer pool.Close()

	client, err := pool.Connect(1, srv.Addr().String())
	if err != nil {
		t.Fatalf("failed to connect to server: %v", err)
	}

	et := srv.GetElectionTimer()
	initialReset := et.LastReset()

	time.Sleep(10 * time.Millisecond)

	// 1. Send valid AppendEntries RPC via gRPC
	aeReply, err := client.AppendEntries(context.Background(), &raftpb.AppendEntriesArgs{
		Term:         5,
		LeaderId:     2,
		PrevLogIndex: 0,
		PrevLogTerm:  0,
	})
	if err != nil {
		t.Fatalf("AppendEntries RPC failed: %v", err)
	}
	if aeReply.Term != 5 {
		t.Fatalf("expected reply term 5, got %d", aeReply.Term)
	}
	if !et.LastReset().After(initialReset) {
		t.Fatalf("expected timer LastReset to be updated after valid AppendEntries RPC")
	}

	// 2. Send stale AppendEntries RPC via gRPC (term 2 < 5)
	lastResetAfterValid := et.LastReset()
	time.Sleep(10 * time.Millisecond)

	staleReply, err := client.AppendEntries(context.Background(), &raftpb.AppendEntriesArgs{
		Term:         2,
		LeaderId:     2,
		PrevLogIndex: 0,
		PrevLogTerm:  0,
	})
	if err != nil {
		t.Fatalf("stale AppendEntries RPC failed: %v", err)
	}
	if staleReply.Success {
		t.Fatalf("expected stale AppendEntries to return Success=false")
	}
	if staleReply.Term != 5 {
		t.Fatalf("expected stale reply to return current term 5, got %d", staleReply.Term)
	}
	if et.LastReset() != lastResetAfterValid {
		t.Fatalf("timer must NOT be reset after stale AppendEntries RPC")
	}
}

func TestServer_RunLoop_Lifecycle(t *testing.T) {
	srv := NewServer(1, nil, nil, nil)
	if srv.IsLoopRunning() {
		t.Fatalf("expected loop to not be running initially")
	}

	srv.StartEventLoop()
	if !srv.IsLoopRunning() {
		t.Fatalf("expected loop to be running after StartEventLoop")
	}

	srv.Stop()
	if srv.IsLoopRunning() {
		t.Fatalf("expected loop to stop after Stop")
	}
}

func TestServer_RunLoop_ElectionTimeoutFires(t *testing.T) {
	srv := NewServer(1, nil, &raft.PersistentState{
		CurrentTerm: 0,
		VotedFor:    -1,
		Log:         make([]raft.LogEntry, 0),
	}, nil)

	// Set very short election timeout for fast testing
	srv.GetElectionTimer().SetRange(25*time.Millisecond, 40*time.Millisecond)
	srv.GetElectionTimer().Reset()

	srv.StartEventLoop()
	defer srv.Stop()

	if srv.GetState() != raft.Follower {
		t.Fatalf("expected initial Follower state, got %v", srv.GetState())
	}

	// Wait for election timeout to trigger transition in run() loop
	time.Sleep(50 * time.Millisecond)

	if srv.GetState() != raft.Candidate {
		t.Fatalf("expected transition to Candidate after timeout, got %v", srv.GetState())
	}

	if srv.PersistentState.CurrentTerm < 1 {
		t.Fatalf("expected CurrentTerm at least 1, got %d", srv.PersistentState.CurrentTerm)
	}

	if srv.PersistentState.VotedFor != 1 {
		t.Fatalf("expected VotedFor self (1), got %d", srv.PersistentState.VotedFor)
	}
}

func TestServer_RunLoop_RPCChannels(t *testing.T) {
	srv := NewServer(1, nil, &raft.PersistentState{
		CurrentTerm: 2,
		VotedFor:    -1,
		Log:         make([]raft.LogEntry, 0),
	}, nil)

	srv.StartEventLoop()
	defer srv.Stop()

	// 1. Send RequestVote via RequestVoteChannel
	rvResCh := make(chan *raftpb.RequestVoteReply, 1)
	srv.RequestVoteChannel() <- RequestVoteEvent{
		Args: &raftpb.RequestVoteArgs{
			Term:        2,
			CandidateId: 2,
		},
		ResCh: rvResCh,
	}

	select {
	case reply := <-rvResCh:
		if reply.Term != 2 {
			t.Fatalf("expected reply term 2, got %d", reply.Term)
		}
	case <-time.After(500 * time.Millisecond):
		t.Fatal("timed out waiting for RequestVote reply from event loop")
	}

	// 2. Send AppendEntries via AppendEntriesChannel
	aeResCh := make(chan *raftpb.AppendEntriesReply, 1)
	srv.AppendEntriesChannel() <- AppendEntriesEvent{
		Args: &raftpb.AppendEntriesArgs{
			Term:     2,
			LeaderId: 2,
		},
		ResCh: aeResCh,
	}

	select {
	case reply := <-aeResCh:
		if reply.Term != 2 {
			t.Fatalf("expected reply term 2, got %d", reply.Term)
		}
	case <-time.After(500 * time.Millisecond):
		t.Fatal("timed out waiting for AppendEntries reply from event loop")
	}
}

func TestServer_SingleEventLoop_NoDataRaces(t *testing.T) {
	srv := NewServer(1, nil, &raft.PersistentState{
		CurrentTerm: 1,
		VotedFor:    -1,
		Log:         make([]raft.LogEntry, 0),
	}, nil)

	srv.StartEventLoop()
	defer srv.Stop()

	// Launch 20 goroutines performing concurrent operations (mutations and reads)
	// to verify that all state mutations flow cleanly through the single event loop.
	var wg sync.WaitGroup
	for i := 0; i < 20; i++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			for j := 0; j < 25; j++ {
				switch (workerID + j) % 5 {
				case 0:
					srv.SetState(raft.Candidate)
				case 1:
					srv.SetState(raft.Follower)
				case 2:
					_ = srv.ResetElectionTimeout()
				case 3:
					_, _ = srv.HandleHeartbeat(2, 1)
				case 4:
					_ = srv.GetState()
				}
			}
		}(i)
	}

	wg.Wait()
}






