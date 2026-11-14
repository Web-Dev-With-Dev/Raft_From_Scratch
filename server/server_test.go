package server

import (
	"bytes"
	"context"
	"encoding/json"
	"net"
	"net/http"
	"net/http/httptest"
	"runtime"
	"strings"
	"sync"
	"testing"
	"time"

	raftpb "raft/proto"
	"raft/raft"
	"raft/rpc"
	"raft/store"

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
	if !rvReply.VoteGranted {
		t.Fatalf("expected VoteGranted true per Figure 2 rules 1-2")
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
	if !aeReply.Success {
		t.Fatalf("expected Success true per Figure 2 rules 1-2")
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
				Term:        0,
				CandidateID: sender.ID,
			})
			if err != nil {
				t.Fatalf("node %d failed to reach node %d via RequestVote: %v", sender.ID, target.ID, err)
			}
			if reply.Term != 0 {
				t.Fatalf("unexpected reply term from node %d: %d", target.ID, reply.Term)
			}

			aeReply, err := sender.SendAppendEntriesToNode(ctx, target.ID, &raft.AppendEntriesArgs{
				Term:     0,
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

func TestServer_BecomeCandidate(t *testing.T) {
	srv := NewServer(1, []Peer{
		{ID: 2, Address: "localhost:8002"},
		{ID: 3, Address: "localhost:8003"},
	}, &raft.PersistentState{
		CurrentTerm: 2,
		VotedFor:    -1,
		Log:         make([]raft.LogEntry, 0),
	}, nil)

	if srv.State != raft.Follower {
		t.Fatalf("expected initial state Follower, got %s", srv.State)
	}

	srv.becomeCandidate()

	if srv.State != raft.Candidate {
		t.Fatalf("expected state Candidate, got %s", srv.State)
	}
	if srv.PersistentState.CurrentTerm != 3 {
		t.Fatalf("expected term 3, got %d", srv.PersistentState.CurrentTerm)
	}
	if srv.PersistentState.VotedFor != 1 {
		t.Fatalf("expected votedFor 1, got %d", srv.PersistentState.VotedFor)
	}
	if !srv.votesReceived[1] {
		t.Fatalf("expected self vote in votesReceived")
	}
	if len(srv.votesReceived) != 1 {
		t.Fatalf("expected exactly 1 vote recorded, got %d", len(srv.votesReceived))
	}
}

func TestServer_BecomeLeader(t *testing.T) {
	peers := []Peer{
		{ID: 2, Address: "localhost:8002"},
		{ID: 3, Address: "localhost:8003"},
	}
	srv := NewServer(1, peers, &raft.PersistentState{
		CurrentTerm: 3,
		VotedFor:    1,
		Log: []raft.LogEntry{
			{Term: 1, Index: 1, Command: "cmd1"},
			{Term: 2, Index: 2, Command: "cmd2"},
			{Term: 3, Index: 3, Command: "cmd3"},
		},
	}, nil)

	srv.becomeCandidate()
	srv.becomeLeader()

	if srv.State != raft.Leader {
		t.Fatalf("expected state Leader, got %s", srv.State)
	}
	if srv.VolatileStateLeader == nil {
		t.Fatalf("expected VolatileStateLeader to be initialized")
	}
	// For each peer, nextIndex should be lastLogIndex + 1 = 4, and matchIndex = 0
	for _, p := range peers {
		if next := srv.VolatileStateLeader.NextIndex[p.ID]; next != 4 {
			t.Fatalf("expected peer %d nextIndex=4, got %d", p.ID, next)
		}
		if match := srv.VolatileStateLeader.MatchIndex[p.ID]; match != 0 {
			t.Fatalf("expected peer %d matchIndex=0, got %d", p.ID, match)
		}
	}
	if srv.heartbeatTimer == nil {
		t.Fatalf("expected heartbeatTimer to be armed")
	}
}

func TestServer_BecomeFollower(t *testing.T) {
	srv := NewServer(1, []Peer{
		{ID: 2, Address: "localhost:8002"},
	}, &raft.PersistentState{
		CurrentTerm: 2,
		VotedFor:    1,
		Log:         make([]raft.LogEntry, 0),
	}, nil)

	srv.becomeCandidate() // CurrentTerm=3, VotedFor=1

	// 1. Transition to Follower with higher term -> resets votedFor
	srv.becomeFollower(5)

	if srv.State != raft.Follower {
		t.Fatalf("expected state Follower, got %s", srv.State)
	}
	if srv.PersistentState.CurrentTerm != 5 {
		t.Fatalf("expected currentTerm 5, got %d", srv.PersistentState.CurrentTerm)
	}
	if srv.PersistentState.VotedFor != -1 {
		t.Fatalf("expected votedFor -1 after higher term, got %d", srv.PersistentState.VotedFor)
	}
	if len(srv.votesReceived) != 0 {
		t.Fatalf("expected votesReceived cleared, got %v", srv.votesReceived)
	}

	// 2. Transition to Follower with same term -> preserves votedFor
	srv.PersistentState.VotedFor = 2
	srv.becomeFollower(5)
	if srv.PersistentState.VotedFor != 2 {
		t.Fatalf("expected votedFor 2 preserved when term not increased, got %d", srv.PersistentState.VotedFor)
	}
}

func TestServer_Transitions_FullLifecycle(t *testing.T) {
	peers := []Peer{
		{ID: 2, Address: "localhost:8002"},
		{ID: 3, Address: "localhost:8003"},
	}
	srv := NewServer(1, peers, &raft.PersistentState{
		CurrentTerm: 0,
		VotedFor:    -1,
		Log:         make([]raft.LogEntry, 0),
	}, nil)

	// Step 1: Starts as Follower
	if srv.GetState() != raft.Follower {
		t.Fatalf("expected Follower, got %s", srv.GetState())
	}

	// Step 2: Election timeout -> becomeCandidate
	srv.becomeCandidate()
	if srv.GetState() != raft.Candidate {
		t.Fatalf("expected Candidate, got %s", srv.GetState())
	}
	if srv.PersistentState.CurrentTerm != 1 || srv.PersistentState.VotedFor != 1 {
		t.Fatalf("expected term 1 votedFor 1, got term %d votedFor %d", srv.PersistentState.CurrentTerm, srv.PersistentState.VotedFor)
	}

	// Step 3: Candidate achieves majority -> becomeLeader
	srv.becomeLeader()
	if srv.GetState() != raft.Leader {
		t.Fatalf("expected Leader, got %s", srv.GetState())
	}
	if srv.heartbeatTimer == nil {
		t.Fatalf("expected armed heartbeatTimer")
	}

	// Step 4: Discovers higher term in RPC -> becomeFollower
	srv.becomeFollower(4)
	if srv.GetState() != raft.Follower {
		t.Fatalf("expected Follower, got %s", srv.GetState())
	}
	if srv.PersistentState.CurrentTerm != 4 {
		t.Fatalf("expected term 4, got %d", srv.PersistentState.CurrentTerm)
	}
	if srv.PersistentState.VotedFor != -1 {
		t.Fatalf("expected votedFor -1, got %d", srv.PersistentState.VotedFor)
	}
}

func TestStateTransition_FollowerCandidateLeaderFollower(t *testing.T) {
	peers := []Peer{
		{ID: 2, Address: "127.0.0.1:8002"},
		{ID: 3, Address: "127.0.0.1:8003"},
	}
	ps := &raft.PersistentState{
		CurrentTerm: 1,
		VotedFor:    -1,
		Log: []raft.LogEntry{
			{Term: 1, Index: 1, Command: "cmd1"},
		},
	}
	srv := NewServer(1, peers, ps, nil)

	// ==========================================
	// 1. Initial State: Follower
	// ==========================================
	if srv.GetState() != raft.Follower {
		t.Fatalf("expected initial role Follower, got %s", srv.GetState())
	}
	if srv.PersistentState.CurrentTerm != 1 {
		t.Fatalf("expected initial term 1, got %d", srv.PersistentState.CurrentTerm)
	}
	if srv.PersistentState.VotedFor != -1 {
		t.Fatalf("expected initial votedFor -1, got %d", srv.PersistentState.VotedFor)
	}
	if srv.heartbeatTimer != nil {
		t.Fatalf("follower must not have an active heartbeatTimer")
	}
	if srv.VolatileStateLeader != nil {
		t.Fatalf("follower must have nil VolatileStateLeader")
	}

	// ==========================================
	// 2. Transition: Follower -> Candidate
	// ==========================================
	srv.becomeCandidate()

	if srv.GetState() != raft.Candidate {
		t.Fatalf("expected role Candidate, got %s", srv.GetState())
	}
	if srv.PersistentState.CurrentTerm != 2 {
		t.Fatalf("expected incremented term 2, got %d", srv.PersistentState.CurrentTerm)
	}
	if srv.PersistentState.VotedFor != 1 {
		t.Fatalf("expected votedFor self (1), got %d", srv.PersistentState.VotedFor)
	}
	if !srv.votesReceived[1] {
		t.Fatalf("expected candidate self-vote recorded in votesReceived")
	}
	if len(srv.votesReceived) != 1 {
		t.Fatalf("expected exactly 1 vote recorded initially, got %d", len(srv.votesReceived))
	}
	if srv.heartbeatTimer != nil {
		t.Fatalf("candidate must not have an active heartbeatTimer")
	}

	// ==========================================
	// 3. Transition: Candidate -> Leader
	// ==========================================
	srv.becomeLeader()

	if srv.GetState() != raft.Leader {
		t.Fatalf("expected role Leader, got %s", srv.GetState())
	}
	if srv.PersistentState.CurrentTerm != 2 {
		t.Fatalf("expected term 2 unchanged when becoming leader, got %d", srv.PersistentState.CurrentTerm)
	}
	if srv.heartbeatTimer == nil {
		t.Fatalf("leader must have an armed heartbeatTimer")
	}
	if srv.GetHeartbeatTicker() == nil {
		t.Fatalf("leader must have an active heartbeat ticker")
	}
	if srv.VolatileStateLeader == nil {
		t.Fatalf("leader must initialize VolatileStateLeader")
	}
	// Verify nextIndex initialized to lastLogIndex + 1 = 2, and matchIndex = 0 for all peers
	for _, p := range peers {
		if next := srv.VolatileStateLeader.NextIndex[p.ID]; next != 2 {
			t.Fatalf("expected peer %d nextIndex=2, got %d", p.ID, next)
		}
		if match := srv.VolatileStateLeader.MatchIndex[p.ID]; match != 0 {
			t.Fatalf("expected peer %d matchIndex=0, got %d", p.ID, match)
		}
	}

	// ==========================================
	// 4. Transition: Leader -> Follower (higher term discovered)
	// ==========================================
	srv.becomeFollower(5)

	if srv.GetState() != raft.Follower {
		t.Fatalf("expected role Follower, got %s", srv.GetState())
	}
	if srv.GetHeartbeatTicker() != nil {
		t.Fatalf("heartbeatTicker must be stopped and nil when stepping down to follower")
	}
	if srv.heartbeatTimer != nil {
		t.Fatalf("heartbeatTimer must be stopped and nil when stepping down to follower")
	}
	if srv.PersistentState.CurrentTerm != 5 {
		t.Fatalf("expected stepped up term 5, got %d", srv.PersistentState.CurrentTerm)
	}
	if srv.PersistentState.VotedFor != -1 {
		t.Fatalf("expected votedFor reset to -1 after higher term discovered, got %d", srv.PersistentState.VotedFor)
	}
	if len(srv.votesReceived) != 0 {
		t.Fatalf("expected votesReceived cleared after becoming follower, got %v", srv.votesReceived)
	}
}

func TestLeaderHeartbeatTicker_Lifecycle(t *testing.T) {
	peers := []Peer{
		{ID: 2, Address: "127.0.0.1:8002"},
		{ID: 3, Address: "127.0.0.1:8003"},
	}
	srv := NewServer(1, peers, nil, nil)

	// 1. Follower state: ticker must not be active
	if ticker := srv.GetHeartbeatTicker(); ticker != nil {
		t.Fatalf("follower must not have an active heartbeat ticker")
	}
	if interval := srv.HeartbeatInterval(); interval != raft.DefaultHeartbeatInterval {
		t.Fatalf("expected default heartbeat interval %v, got %v", raft.DefaultHeartbeatInterval, interval)
	}

	// 2. Candidate state: ticker must not be active
	srv.becomeCandidate()
	if ticker := srv.GetHeartbeatTicker(); ticker != nil {
		t.Fatalf("candidate must not have an active heartbeat ticker")
	}

	// 3. Leader state: ticker must be created and armed
	srv.becomeLeader()
	if ticker := srv.GetHeartbeatTicker(); ticker == nil {
		t.Fatalf("leader must have an active heartbeat ticker")
	}

	// 4. Stepping down to Follower: ticker must be stopped and cleared
	srv.becomeFollower(10)
	if ticker := srv.GetHeartbeatTicker(); ticker != nil {
		t.Fatalf("follower after step-down must not have an active heartbeat ticker")
	}

	// 5. Becoming Leader again and then Candidate: ticker must be stopped and cleared
	srv.becomeLeader()
	if ticker := srv.GetHeartbeatTicker(); ticker == nil {
		t.Fatalf("leader must have an active heartbeat ticker")
	}
	srv.becomeCandidate()
	if ticker := srv.GetHeartbeatTicker(); ticker != nil {
		t.Fatalf("candidate after election timeout must not have an active heartbeat ticker")
	}
}

func TestLeaderHeartbeatTicker_PeriodicFiringInEventLoop(t *testing.T) {
	peers := []Peer{
		{ID: 2, Address: "127.0.0.1:8002"},
	}
	srv := NewServer(1, peers, &raft.PersistentState{
		CurrentTerm: 1,
		VotedFor:    1,
		Log:         make([]raft.LogEntry, 0),
	}, nil)

	// Use a short interval for responsive test execution
	shortInterval := 20 * time.Millisecond
	srv.SetHeartbeatInterval(shortInterval)

	srv.StartEventLoop()
	defer srv.Stop()

	// Initial Follower state: should not fire leader heartbeats
	time.Sleep(45 * time.Millisecond)
	if count := srv.HeartbeatsSent(); count != 0 {
		t.Fatalf("follower must not dispatch leader heartbeats, got count=%d", count)
	}

	// Transition to Leader on the event loop
	srv.SetState(raft.Leader)
	srv.becomeLeader()

	// Wait enough time for at least 3 ticker firings (20ms interval, waiting ~70ms)
	time.Sleep(75 * time.Millisecond)
	sentCount := srv.HeartbeatsSent()
	if sentCount < 2 {
		t.Fatalf("expected at least 2 heartbeat ticks while leader, got %d", sentCount)
	}

	// Step down to Follower
	srv.becomeFollower(2)
	stoppedCount := srv.HeartbeatsSent()

	// Wait another 50ms and verify no more heartbeats fire
	time.Sleep(50 * time.Millisecond)
	finalCount := srv.HeartbeatsSent()
	if finalCount != stoppedCount {
		t.Fatalf("heartbeats fired after stepping down: expected %d, got %d", stoppedCount, finalCount)
	}
}

func TestLeaderHeartbeatTicker_CustomInterval(t *testing.T) {
	srv := NewServer(1, nil, nil, nil)

	customInterval := 30 * time.Millisecond
	srv.SetHeartbeatInterval(customInterval)
	if srv.HeartbeatInterval() != customInterval {
		t.Fatalf("expected heartbeat interval %v, got %v", customInterval, srv.HeartbeatInterval())
	}

	srv.becomeLeader()
	if srv.GetHeartbeatTicker() == nil {
		t.Fatalf("expected leader heartbeat ticker to be active")
	}

	// Reset with zero/negative should fall back to default
	srv.SetHeartbeatInterval(0)
	if srv.HeartbeatInterval() != raft.DefaultHeartbeatInterval {
		t.Fatalf("expected fallback to default interval %v, got %v", raft.DefaultHeartbeatInterval, srv.HeartbeatInterval())
	}
}

func TestLeaderHeartbeat_LogsWouldSendHeartbeatToPeer(t *testing.T) {
	buf := &bytes.Buffer{}
	nodeID := raft.NodeID(1)

	testLogger := NewNodeLogger(nodeID, buf, &LogOptions{
		DisableTime: true,
	})

	peers := []Peer{
		{ID: 1, Address: "127.0.0.1:8001"}, // self
		{ID: 2, Address: "127.0.0.1:8002"},
		{ID: 3, Address: "127.0.0.1:8003"},
	}

	srv := NewServer(nodeID, peers, nil, nil)
	srv.SetLogger(testLogger)

	// In Follower state, broadcastHeartbeats should not send/log heartbeats
	srv.broadcastHeartbeats()
	if strings.Contains(buf.String(), "would send heartbeat to peer") {
		t.Fatalf("follower should not log 'would send heartbeat to peer'")
	}

	// Become leader
	srv.becomeLeader()
	buf.Reset()

	// Trigger heartbeat tick
	srv.handleHeartbeatTick()

	output := buf.String()
	if !strings.Contains(output, "would send heartbeat to peer 2") {
		t.Fatalf("expected log to contain 'would send heartbeat to peer 2', got:\n%s", output)
	}
	if !strings.Contains(output, "would send heartbeat to peer 3") {
		t.Fatalf("expected log to contain 'would send heartbeat to peer 3', got:\n%s", output)
	}
	if strings.Contains(output, "would send heartbeat to peer 1") {
		t.Fatalf("leader must not send heartbeat to itself (peer 1)")
	}
}

func TestLeaderHeartbeatTicker_StopsCleanlyOnStepDown(t *testing.T) {
	peers := []Peer{
		{ID: 2, Address: "127.0.0.1:8002"},
	}
	srv := NewServer(1, peers, &raft.PersistentState{
		CurrentTerm: 1,
		VotedFor:    1,
		Log:         make([]raft.LogEntry, 0),
	}, nil)

	srv.SetHeartbeatInterval(20 * time.Millisecond)
	srv.StartEventLoop()
	defer srv.Stop()

	// 1. Transition to leader and verify ticker is active
	srv.becomeLeader()
	if srv.GetHeartbeatTicker() == nil {
		t.Fatalf("expected leader heartbeat ticker to be active")
	}

	// Wait for heartbeats to tick
	time.Sleep(50 * time.Millisecond)
	if srv.HeartbeatsSent() == 0 {
		t.Fatalf("expected heartbeats to have fired while leader")
	}

	// 2. Step down to follower
	srv.becomeFollower(3)
	countAtStepDown := srv.HeartbeatsSent()

	// Verify ticker is stopped and nil
	if ticker := srv.GetHeartbeatTicker(); ticker != nil {
		t.Fatalf("heartbeat ticker must be nil after stepping down, got %v", ticker)
	}

	// Wait 60ms (several heartbeat intervals) and verify no additional heartbeats fired
	time.Sleep(60 * time.Millisecond)
	if finalCount := srv.HeartbeatsSent(); finalCount != countAtStepDown {
		t.Fatalf("heartbeat ticker continued firing after step down: sent %d before, %d after", countAtStepDown, finalCount)
	}
}

func TestLeaderHeartbeatTicker_StopsCleanlyOnSetState(t *testing.T) {
	srv := NewServer(1, nil, nil, nil)

	srv.becomeLeader()
	if srv.GetHeartbeatTicker() == nil {
		t.Fatalf("expected heartbeat ticker to be active for leader")
	}

	// SetState to Follower should cleanly stop and nil out ticker
	srv.SetState(raft.Follower)
	if srv.GetHeartbeatTicker() != nil {
		t.Fatalf("heartbeat ticker must be nil after SetState(Follower)")
	}

	// Re-arm as leader and SetState to Candidate
	srv.becomeLeader()
	if srv.GetHeartbeatTicker() == nil {
		t.Fatalf("expected heartbeat ticker to be re-armed")
	}

	srv.SetState(raft.Candidate)
	if srv.GetHeartbeatTicker() != nil {
		t.Fatalf("heartbeat ticker must be nil after SetState(Candidate)")
	}
}

func TestLeaderHeartbeatTicker_StopsCleanlyOnHigherTermRPC(t *testing.T) {
	srv := NewServer(1, []Peer{
		{ID: 2, Address: "127.0.0.1:8002"},
	}, &raft.PersistentState{
		CurrentTerm: 2,
		VotedFor:    1,
		Log:         make([]raft.LogEntry, 0),
	}, nil)

	srv.becomeLeader()
	if srv.GetHeartbeatTicker() == nil {
		t.Fatalf("expected leader heartbeat ticker to be active")
	}

	// Leader receives AppendEntries from a new leader with higher term (term 4)
	reply := srv.processAppendEntries(&raftpb.AppendEntriesArgs{
		Term:     4,
		LeaderId: 2,
	})

	if reply.Term != 4 {
		t.Fatalf("expected reply term 4, got %d", reply.Term)
	}
	if srv.GetState() != raft.Follower {
		t.Fatalf("expected leader to step down to Follower on higher term AppendEntries, got %s", srv.GetState())
	}
	if srv.GetHeartbeatTicker() != nil {
		t.Fatalf("expected heartbeat ticker to be nil after higher term AppendEntries step down")
	}
}

func TestServer_Persistence_PersistAndLoad(t *testing.T) {
	initialState := &raft.PersistentState{
		CurrentTerm: 3,
		VotedFor:    2,
		Log: []raft.LogEntry{
			{Index: 1, Term: 1, Command: []byte("set a=1")},
			{Index: 2, Term: 2, Command: []byte("set b=2")},
		},
	}

	srv := NewServer(1, nil, initialState, nil)
	defer srv.Close()

	st := srv.GetStorage()
	if st == nil {
		t.Fatal("expected non-nil storage by default")
	}

	// Verify initialState was persisted during NewServer
	loaded, err := st.Load()
	if err != nil {
		t.Fatalf("storage.Load failed: %v", err)
	}
	if loaded.CurrentTerm != 3 || loaded.VotedFor != 2 || len(loaded.Log) != 2 {
		t.Fatalf("unexpected persisted initial state: %+v", loaded)
	}

	// Update PersistentState and call Persist()
	srv.PersistentState.CurrentTerm = 4
	srv.PersistentState.VotedFor = 1
	srv.PersistentState.Log = append(srv.PersistentState.Log, raft.LogEntry{
		Index: 3, Term: 4, Command: []byte("set c=3"),
	})

	if err := srv.Persist(); err != nil {
		t.Fatalf("srv.Persist failed: %v", err)
	}

	// Overwrite server's in-memory state with blank struct
	srv.PersistentState = &raft.PersistentState{
		CurrentTerm: 0,
		VotedFor:    -1,
		Log:         nil,
	}

	// Call Load() and verify restored state
	if err := srv.Load(); err != nil {
		t.Fatalf("srv.Load failed: %v", err)
	}

	if srv.PersistentState.CurrentTerm != 4 {
		t.Errorf("expected CurrentTerm 4, got %d", srv.PersistentState.CurrentTerm)
	}
	if srv.PersistentState.VotedFor != 1 {
		t.Errorf("expected VotedFor 1, got %d", srv.PersistentState.VotedFor)
	}
	if len(srv.PersistentState.Log) != 3 {
		t.Fatalf("expected Log length 3, got %d", len(srv.PersistentState.Log))
	}
	if srv.PersistentState.Log[2].Index != 3 || srv.PersistentState.Log[2].Term != 4 {
		t.Errorf("unexpected Log[2]: %+v", srv.PersistentState.Log[2])
	}
}

func TestServer_Persistence_StateTransitionsAutoPersist(t *testing.T) {
	srv := NewServer(1, nil, &raft.PersistentState{
		CurrentTerm: 1,
		VotedFor:    -1,
		Log:         make([]raft.LogEntry, 0),
	}, nil)
	defer srv.Close()

	// 1. Transition to Candidate increments term and votes for self; must auto-persist
	srv.becomeCandidate()

	st := srv.GetStorage()
	loaded, err := st.Load()
	if err != nil {
		t.Fatalf("st.Load failed: %v", err)
	}
	if loaded.CurrentTerm != 2 {
		t.Fatalf("expected persisted CurrentTerm 2 after becomeCandidate, got %d", loaded.CurrentTerm)
	}
	if loaded.VotedFor != 1 {
		t.Fatalf("expected persisted VotedFor 1 after becomeCandidate, got %d", loaded.VotedFor)
	}

	// 2. Transition to Follower on higher term must auto-persist
	srv.becomeFollower(10)

	loaded2, err := st.Load()
	if err != nil {
		t.Fatalf("st.Load failed: %v", err)
	}
	if loaded2.CurrentTerm != 10 {
		t.Fatalf("expected persisted CurrentTerm 10 after becomeFollower(10), got %d", loaded2.CurrentTerm)
	}
	if loaded2.VotedFor != -1 {
		t.Fatalf("expected persisted VotedFor -1 after higher term follower transition, got %d", loaded2.VotedFor)
	}
}

func TestServer_SetStorage(t *testing.T) {
	srv := NewServer(1, nil, nil, nil)
	defer srv.Close()

	customStore := store.NewMemoryStorage()
	srv.SetStorage(customStore)

	if srv.GetStorage() != customStore {
		t.Fatal("expected GetStorage() to return custom storage")
	}

	srv.PersistentState.CurrentTerm = 9
	if err := srv.Persist(); err != nil {
		t.Fatalf("Persist with custom store failed: %v", err)
	}

	loaded, err := customStore.Load()
	if err != nil {
		t.Fatalf("customStore.Load failed: %v", err)
	}
	if loaded.CurrentTerm != 9 {
		t.Fatalf("expected loaded CurrentTerm 9 from custom store, got %d", loaded.CurrentTerm)
	}
}

func TestServer_Persistence_EventLoopSerialization(t *testing.T) {
	srv := NewServer(1, nil, &raft.PersistentState{
		CurrentTerm: 1,
		VotedFor:    -1,
		Log:         make([]raft.LogEntry, 0),
	}, nil)
	defer srv.Close()

	srv.StartEventLoop()

	// Update term and persist through active event loop
	srv.PersistentState.CurrentTerm = 7
	srv.PersistentState.VotedFor = 1
	if err := srv.Persist(); err != nil {
		t.Fatalf("Persist through active event loop failed: %v", err)
	}

	loaded, err := srv.GetStorage().Load()
	if err != nil {
		t.Fatalf("GetStorage().Load failed: %v", err)
	}
	if loaded.CurrentTerm != 7 || loaded.VotedFor != 1 {
		t.Fatalf("unexpected state after event loop Persist: %+v", loaded)
	}

	// Load through active event loop
	srv.PersistentState.CurrentTerm = 0
	if err := srv.Load(); err != nil {
		t.Fatalf("Load through active event loop failed: %v", err)
	}
	if srv.PersistentState.CurrentTerm != 7 {
		t.Fatalf("expected CurrentTerm 7 after event loop Load, got %d", srv.PersistentState.CurrentTerm)
	}
}

func TestPersistence_RaftPaper_Points(t *testing.T) {
	// Point 1: Candidate state transition increments currentTerm, votes for self, and persists
	t.Run("CandidateStateTransition_PersistsTermAndSelfVote", func(t *testing.T) {
		srv := NewServer(1, nil, &raft.PersistentState{
			CurrentTerm: 2,
			VotedFor:    -1,
			Log:         make([]raft.LogEntry, 0),
		}, nil)
		defer srv.Close()

		srv.becomeCandidate()

		loaded, err := srv.GetStorage().Load()
		if err != nil {
			t.Fatalf("failed to load storage: %v", err)
		}
		if loaded.CurrentTerm != 3 {
			t.Errorf("expected stored currentTerm 3, got %d", loaded.CurrentTerm)
		}
		if loaded.VotedFor != 1 {
			t.Errorf("expected stored votedFor 1 (self), got %d", loaded.VotedFor)
		}
	})

	// Point 2: Discovering higher term in RequestVote RPC updates term, persists, and grants vote per Figure 2
	t.Run("RequestVote_HigherTerm_PersistsAndGrantsVote", func(t *testing.T) {
		srv := NewServer(1, nil, &raft.PersistentState{
			CurrentTerm: 1,
			VotedFor:    1,
			Log:         make([]raft.LogEntry, 0),
		}, nil)
		defer srv.Close()
		srv.SetState(raft.Candidate)

		reply := srv.processRequestVote(&raftpb.RequestVoteArgs{
			Term:        5,
			CandidateId: 2,
		})

		if reply.Term != 5 {
			t.Errorf("expected reply term 5, got %d", reply.Term)
		}
		if !reply.VoteGranted {
			t.Errorf("expected vote to be granted to candidate 2")
		}

		loaded, err := srv.GetStorage().Load()
		if err != nil {
			t.Fatalf("failed to load storage: %v", err)
		}
		if loaded.CurrentTerm != 5 {
			t.Errorf("expected stored currentTerm 5, got %d", loaded.CurrentTerm)
		}
		if loaded.VotedFor != 2 {
			t.Errorf("expected stored votedFor 2, got %d", loaded.VotedFor)
		}
	})

	// Point 3: Discovering higher term in AppendEntries RPC updates term, resets votedFor, and persists
	t.Run("AppendEntries_HigherTerm_PersistsAndResetsVote", func(t *testing.T) {
		srv := NewServer(1, nil, &raft.PersistentState{
			CurrentTerm: 2,
			VotedFor:    1,
			Log:         make([]raft.LogEntry, 0),
		}, nil)
		defer srv.Close()
		srv.SetState(raft.Leader)

		reply := srv.processAppendEntries(&raftpb.AppendEntriesArgs{
			Term:     8,
			LeaderId: 3,
		})

		if reply.Term != 8 {
			t.Errorf("expected reply term 8, got %d", reply.Term)
		}

		loaded, err := srv.GetStorage().Load()
		if err != nil {
			t.Fatalf("failed to load storage: %v", err)
		}
		if loaded.CurrentTerm != 8 {
			t.Errorf("expected stored currentTerm 8, got %d", loaded.CurrentTerm)
		}
		if loaded.VotedFor != -1 {
			t.Errorf("expected stored votedFor reset to -1, got %d", loaded.VotedFor)
		}
	})

	// Point 4: Discovering higher term in RPC response updates term, resets votedFor, and persists
	t.Run("RPCResponse_HigherTerm_PersistsAndResetsVote", func(t *testing.T) {
		srv := NewServer(1, nil, &raft.PersistentState{
			CurrentTerm: 3,
			VotedFor:    1,
			Log:         make([]raft.LogEntry, 0),
		}, nil)
		defer srv.Close()

		srv.checkReplyTerm(12)

		loaded, err := srv.GetStorage().Load()
		if err != nil {
			t.Fatalf("failed to load storage: %v", err)
		}
		if loaded.CurrentTerm != 12 {
			t.Errorf("expected stored currentTerm 12, got %d", loaded.CurrentTerm)
		}
		if loaded.VotedFor != -1 {
			t.Errorf("expected stored votedFor reset to -1, got %d", loaded.VotedFor)
		}
	})

	// Point 5: Explicit SetCurrentTerm persists state and resets votedFor when advancing term
	t.Run("SetCurrentTerm_PersistsAndResetsVote", func(t *testing.T) {
		srv := NewServer(1, nil, &raft.PersistentState{
			CurrentTerm: 2,
			VotedFor:    1,
			Log:         make([]raft.LogEntry, 0),
		}, nil)
		defer srv.Close()

		if err := srv.SetCurrentTerm(6); err != nil {
			t.Fatalf("SetCurrentTerm failed: %v", err)
		}

		loaded, err := srv.GetStorage().Load()
		if err != nil {
			t.Fatalf("failed to load storage: %v", err)
		}
		if loaded.CurrentTerm != 6 {
			t.Errorf("expected stored currentTerm 6, got %d", loaded.CurrentTerm)
		}
		if loaded.VotedFor != -1 {
			t.Errorf("expected stored votedFor reset to -1, got %d", loaded.VotedFor)
		}
	})

	// Point 6: Explicit SetVotedFor persists candidate ID
	t.Run("SetVotedFor_PersistsCandidateID", func(t *testing.T) {
		srv := NewServer(1, nil, &raft.PersistentState{
			CurrentTerm: 4,
			VotedFor:    -1,
			Log:         make([]raft.LogEntry, 0),
		}, nil)
		defer srv.Close()

		if err := srv.SetVotedFor(2); err != nil {
			t.Fatalf("SetVotedFor failed: %v", err)
		}

		loaded, err := srv.GetStorage().Load()
		if err != nil {
			t.Fatalf("failed to load storage: %v", err)
		}
		if loaded.CurrentTerm != 4 {
			t.Errorf("expected stored currentTerm 4, got %d", loaded.CurrentTerm)
		}
		if loaded.VotedFor != 2 {
			t.Errorf("expected stored votedFor 2, got %d", loaded.VotedFor)
		}
	})
}

// spyStorage is a mock/spy implementation of store.Storage used to track and assert Persist() calls.
type spyStorage struct {
	mu           sync.Mutex
	persistCount int
	loadCount    int
	lastState    *raft.PersistentState
	history      []raft.PersistentState
	injectErr    error
}

func newSpyStorage() *spyStorage {
	return &spyStorage{
		history: make([]raft.PersistentState, 0),
	}
}

func (s *spyStorage) Persist(state *raft.PersistentState) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.persistCount++
	if state != nil {
		copied := raft.PersistentState{
			CurrentTerm: state.CurrentTerm,
			VotedFor:    state.VotedFor,
			Log:         make([]raft.LogEntry, len(state.Log)),
		}
		copy(copied.Log, state.Log)
		s.lastState = &copied
		s.history = append(s.history, copied)
	} else {
		s.lastState = nil
	}
	return s.injectErr
}

func (s *spyStorage) Load() (*raft.PersistentState, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.loadCount++
	if s.lastState == nil {
		return nil, s.injectErr
	}
	copied := &raft.PersistentState{
		CurrentTerm: s.lastState.CurrentTerm,
		VotedFor:    s.lastState.VotedFor,
		Log:         make([]raft.LogEntry, len(s.lastState.Log)),
	}
	copy(copied.Log, s.lastState.Log)
	return copied, s.injectErr
}

func (s *spyStorage) SaveSnapshot(snapshot []byte) error { return nil }
func (s *spyStorage) LoadSnapshot() ([]byte, error)      { return nil, nil }
func (s *spyStorage) Close() error                      { return nil }

func (s *spyStorage) PersistCount() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.persistCount
}

func (s *spyStorage) LastState() *raft.PersistentState {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.lastState == nil {
		return nil
	}
	copied := *s.lastState
	return &copied
}

func (s *spyStorage) ResetCount() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.persistCount = 0
}

func TestServer_Persistence_SpyMockAssertions(t *testing.T) {
	t.Run("BecomeCandidate_InvokesPersistOnceWithIncrementedTermAndSelfVote", func(t *testing.T) {
		srv := NewServer(1, nil, &raft.PersistentState{
			CurrentTerm: 1,
			VotedFor:    -1,
		}, nil)
		defer srv.Close()

		spy := newSpyStorage()
		srv.SetStorage(spy)

		srv.becomeCandidate()

		if count := spy.PersistCount(); count != 1 {
			t.Fatalf("expected Persist() to be called exactly 1 time on becomeCandidate, called %d times", count)
		}
		last := spy.LastState()
		if last == nil {
			t.Fatal("expected LastState to be captured")
		}
		if last.CurrentTerm != 2 {
			t.Errorf("expected persisted currentTerm 2, got %d", last.CurrentTerm)
		}
		if last.VotedFor != 1 {
			t.Errorf("expected persisted votedFor self (1), got %d", last.VotedFor)
		}
	})

	t.Run("BecomeFollower_HigherTerm_InvokesPersistOnceAndResetsVote", func(t *testing.T) {
		srv := NewServer(1, nil, &raft.PersistentState{
			CurrentTerm: 2,
			VotedFor:    1,
		}, nil)
		defer srv.Close()

		spy := newSpyStorage()
		srv.SetStorage(spy)

		srv.becomeFollower(5)

		if count := spy.PersistCount(); count != 1 {
			t.Fatalf("expected Persist() to be called exactly 1 time on higher term becomeFollower, called %d times", count)
		}
		last := spy.LastState()
		if last.CurrentTerm != 5 {
			t.Errorf("expected persisted currentTerm 5, got %d", last.CurrentTerm)
		}
		if last.VotedFor != -1 {
			t.Errorf("expected persisted votedFor -1 (reset), got %d", last.VotedFor)
		}
	})

	t.Run("BecomeFollower_SameOrLowerTerm_DoesNotInvokePersist", func(t *testing.T) {
		srv := NewServer(1, nil, &raft.PersistentState{
			CurrentTerm: 5,
			VotedFor:    2,
		}, nil)
		defer srv.Close()

		spy := newSpyStorage()
		srv.SetStorage(spy)

		// Same term
		srv.becomeFollower(5)
		if count := spy.PersistCount(); count != 0 {
			t.Fatalf("Persist() should not be invoked when term is not increased, got count %d", count)
		}

		// Lower term
		srv.becomeFollower(3)
		if count := spy.PersistCount(); count != 0 {
			t.Fatalf("Persist() should not be invoked on lower term, got count %d", count)
		}
	})

	t.Run("BecomeLeader_DoesNotInvokePersist", func(t *testing.T) {
		srv := NewServer(1, nil, &raft.PersistentState{
			CurrentTerm: 3,
			VotedFor:    1,
		}, nil)
		defer srv.Close()

		spy := newSpyStorage()
		srv.SetStorage(spy)

		// Transition to Leader (candidate already persisted term and vote before election)
		srv.becomeLeader()

		if count := spy.PersistCount(); count != 0 {
			t.Fatalf("becomeLeader() should not invoke Persist(), got count %d", count)
		}
	})

	t.Run("SetCurrentTerm_HigherTerm_InvokesPersistOnce", func(t *testing.T) {
		srv := NewServer(1, nil, &raft.PersistentState{
			CurrentTerm: 2,
			VotedFor:    1,
		}, nil)
		defer srv.Close()

		spy := newSpyStorage()
		srv.SetStorage(spy)

		if err := srv.SetCurrentTerm(4); err != nil {
			t.Fatalf("SetCurrentTerm failed: %v", err)
		}
		if count := spy.PersistCount(); count != 1 {
			t.Fatalf("expected Persist() to be called 1 time on SetCurrentTerm(4), called %d times", count)
		}
		last := spy.LastState()
		if last.CurrentTerm != 4 || last.VotedFor != -1 {
			t.Errorf("unexpected state: term %d, votedFor %d", last.CurrentTerm, last.VotedFor)
		}

		// Calling again with same term does not persist
		if err := srv.SetCurrentTerm(4); err != nil {
			t.Fatalf("SetCurrentTerm(4) repeat failed: %v", err)
		}
		if count := spy.PersistCount(); count != 1 {
			t.Fatalf("Persist() must not be called when term is unchanged, got count %d", count)
		}
	})

	t.Run("SetVotedFor_NewVote_InvokesPersistOnce", func(t *testing.T) {
		srv := NewServer(1, nil, &raft.PersistentState{
			CurrentTerm: 3,
			VotedFor:    -1,
		}, nil)
		defer srv.Close()

		spy := newSpyStorage()
		srv.SetStorage(spy)

		if err := srv.SetVotedFor(2); err != nil {
			t.Fatalf("SetVotedFor failed: %v", err)
		}
		if count := spy.PersistCount(); count != 1 {
			t.Fatalf("expected Persist() called 1 time on SetVotedFor, called %d times", count)
		}
		if last := spy.LastState(); last.VotedFor != 2 {
			t.Errorf("expected persisted votedFor 2, got %d", last.VotedFor)
		}

		// Repeat with same votedFor does not persist
		if err := srv.SetVotedFor(2); err != nil {
			t.Fatalf("SetVotedFor repeat failed: %v", err)
		}
		if count := spy.PersistCount(); count != 1 {
			t.Fatalf("Persist() must not be called when votedFor is unchanged, got count %d", count)
		}
	})

	t.Run("ProcessRequestVote_HigherTerm_InvokesPersistOnce", func(t *testing.T) {
		srv := NewServer(1, nil, &raft.PersistentState{
			CurrentTerm: 1,
			VotedFor:    1,
		}, nil)
		defer srv.Close()
		srv.SetState(raft.Candidate)

		spy := newSpyStorage()
		srv.SetStorage(spy)

		reply := srv.processRequestVote(&raftpb.RequestVoteArgs{
			Term:        6,
			CandidateId: 2,
		})

		if !reply.VoteGranted {
			t.Fatalf("expected vote to be granted to candidate 2")
		}
		if count := spy.PersistCount(); count != 1 {
			t.Fatalf("expected Persist() called 1 time on higher term RequestVote, called %d times", count)
		}
		if last := spy.LastState(); last.CurrentTerm != 6 || last.VotedFor != 2 {
			t.Errorf("unexpected persisted state: term %d, votedFor %d", last.CurrentTerm, last.VotedFor)
		}
	})

	t.Run("ProcessAppendEntries_HigherTerm_InvokesPersistOnce", func(t *testing.T) {
		srv := NewServer(1, nil, &raft.PersistentState{
			CurrentTerm: 2,
			VotedFor:    1,
		}, nil)
		defer srv.Close()
		srv.SetState(raft.Leader)

		spy := newSpyStorage()
		srv.SetStorage(spy)

		srv.processAppendEntries(&raftpb.AppendEntriesArgs{
			Term:     7,
			LeaderId: 3,
		})

		if count := spy.PersistCount(); count != 1 {
			t.Fatalf("expected Persist() called 1 time on higher term AppendEntries, called %d times", count)
		}
		if last := spy.LastState(); last.CurrentTerm != 7 || last.VotedFor != -1 {
			t.Errorf("unexpected persisted state: term %d, votedFor %d", last.CurrentTerm, last.VotedFor)
		}
	})

	t.Run("ActiveEventLoop_MultipleMutations_StrictInvocationCounts", func(t *testing.T) {
		srv := NewServer(1, nil, &raft.PersistentState{
			CurrentTerm: 1,
			VotedFor:    -1,
		}, nil)
		defer srv.Close()

		spy := newSpyStorage()
		srv.SetStorage(spy)

		srv.StartEventLoop()

		// 1. Candidate transition (count -> 1)
		srv.becomeCandidate()
		if count := spy.PersistCount(); count != 1 {
			t.Fatalf("expected Persist() count 1 after becomeCandidate on event loop, got %d", count)
		}

		// 2. Leader step up (count stays 1)
		srv.becomeLeader()
		if count := spy.PersistCount(); count != 1 {
			t.Fatalf("expected Persist() count unchanged (1) on becomeLeader, got %d", count)
		}

		// 3. Step down on higher term (count -> 2)
		srv.becomeFollower(10)
		if count := spy.PersistCount(); count != 2 {
			t.Fatalf("expected Persist() count 2 after becomeFollower(10), got %d", count)
		}

		// 4. Same term step down (count stays 2)
		srv.becomeFollower(10)
		if count := spy.PersistCount(); count != 2 {
			t.Fatalf("expected Persist() count unchanged (2) on repeat follower term, got %d", count)
		}
	})
}

func TestServer_DebugStatusEndpoint(t *testing.T) {
	srv := NewServer(1, nil, &raft.PersistentState{
		CurrentTerm: 3,
		VotedFor:    2,
		Log: []raft.LogEntry{
			{Index: 1, Term: 1, Command: []byte("cmd1")},
		},
	}, nil)
	defer srv.Close()

	// 1. Test Status() method directly
	status := srv.Status()
	if status.ID != 1 {
		t.Errorf("expected ID 1, got %d", status.ID)
	}
	if status.Term != 3 {
		t.Errorf("expected Term 3, got %d", status.Term)
	}
	if status.State != "Follower" {
		t.Errorf("expected State 'Follower', got %q", status.State)
	}
	if status.VotedFor != 2 {
		t.Errorf("expected VotedFor 2, got %d", status.VotedFor)
	}
	if status.LogLength != 1 {
		t.Errorf("expected LogLength 1, got %d", status.LogLength)
	}

	// 2. Test HTTP GET /status via httptest.ResponseRecorder
	handler := srv.HTTPHandler()
	req := httptest.NewRequest(http.MethodGet, "/status", nil)
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected HTTP 200 OK, got %d", rec.Code)
	}
	if ct := rec.Header().Get("Content-Type"); !strings.Contains(ct, "application/json") {
		t.Fatalf("expected application/json content-type, got %q", ct)
	}

	var jsonMap map[string]interface{}
	if err := json.Unmarshal(rec.Body.Bytes(), &jsonMap); err != nil {
		t.Fatalf("failed to decode JSON response: %v", err)
	}

	// Assert required fields: term, state, votedFor
	if termVal, ok := jsonMap["term"].(float64); !ok || uint64(termVal) != 3 {
		t.Errorf("JSON 'term' field mismatch: got %v, want 3", jsonMap["term"])
	}
	if stateVal, ok := jsonMap["state"].(string); !ok || stateVal != "Follower" {
		t.Errorf("JSON 'state' field mismatch: got %v, want 'Follower'", jsonMap["state"])
	}
	if votedVal, ok := jsonMap["votedFor"].(float64); !ok || int64(votedVal) != 2 {
		t.Errorf("JSON 'votedFor' field mismatch: got %v, want 2", jsonMap["votedFor"])
	}

	// 3. Test Method Not Allowed for non-GET methods
	postReq := httptest.NewRequest(http.MethodPost, "/status", nil)
	postRec := httptest.NewRecorder()
	handler.ServeHTTP(postRec, postReq)
	if postRec.Code != http.StatusMethodNotAllowed {
		t.Errorf("expected POST /status to return 405 Method Not Allowed, got %d", postRec.Code)
	}
}

func TestServer_DebugStatusEndpoint_DynamicUpdates(t *testing.T) {
	srv := NewServer(1, nil, &raft.PersistentState{
		CurrentTerm: 1,
		VotedFor:    -1,
	}, nil)
	defer srv.Close()

	handler := srv.HTTPHandler()

	// Initial Follower status
	req := httptest.NewRequest(http.MethodGet, "/status", nil)
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	var s1 NodeStatus
	if err := json.Unmarshal(rec.Body.Bytes(), &s1); err != nil {
		t.Fatalf("failed to unmarshal status: %v", err)
	}
	if s1.Term != 1 || s1.State != "Follower" || s1.VotedFor != -1 {
		t.Fatalf("initial status mismatch: %+v", s1)
	}

	// Transition to Candidate (term 2, votedFor self)
	srv.becomeCandidate()

	rec2 := httptest.NewRecorder()
	handler.ServeHTTP(rec2, httptest.NewRequest(http.MethodGet, "/status", nil))

	var s2 NodeStatus
	if err := json.Unmarshal(rec2.Body.Bytes(), &s2); err != nil {
		t.Fatalf("failed to unmarshal status: %v", err)
	}
	if s2.Term != 2 || s2.State != "Candidate" || s2.VotedFor != 1 {
		t.Fatalf("candidate status mismatch: %+v", s2)
	}

	// Transition to Leader
	srv.becomeLeader()

	rec3 := httptest.NewRecorder()
	handler.ServeHTTP(rec3, httptest.NewRequest(http.MethodGet, "/status", nil))

	var s3 NodeStatus
	if err := json.Unmarshal(rec3.Body.Bytes(), &s3); err != nil {
		t.Fatalf("failed to unmarshal status: %v", err)
	}
	if s3.Term != 2 || s3.State != "Leader" || s3.VotedFor != 1 {
		t.Fatalf("leader status mismatch: %+v", s3)
	}
}

func TestServer_DebugHTTPServer_Lifecycle(t *testing.T) {
	srv := NewServer(1, nil, &raft.PersistentState{
		CurrentTerm: 5,
		VotedFor:    3,
	}, nil)
	defer srv.Close()

	// Start in-process httptest server using srv.HTTPHandler()
	ts := httptest.NewServer(srv.HTTPHandler())
	defer ts.Close()

	resp, err := http.Get(ts.URL + "/status")
	if err != nil {
		t.Fatalf("GET /status failed: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("expected HTTP 200, got %d", resp.StatusCode)
	}

	var status NodeStatus
	if err := json.NewDecoder(resp.Body).Decode(&status); err != nil {
		t.Fatalf("failed to decode response: %v", err)
	}
	if status.Term != 5 || status.VotedFor != 3 || status.State != "Follower" {
		t.Fatalf("unexpected status values: %+v", status)
	}
}

func TestServer_NoGoroutineLeak(t *testing.T) {
	// Let any runtime background tasks settle
	for i := 0; i < 5; i++ {
		runtime.GC()
		time.Sleep(20 * time.Millisecond)
	}
	baseline := runtime.NumGoroutine()

	for iter := 0; iter < 3; iter++ {
		srv := NewServer(1, []Peer{
			{ID: 2, Address: "127.0.0.1:54321"},
		}, &raft.PersistentState{
			CurrentTerm: 0,
			VotedFor:    -1,
		}, rpc.NewClientPool())

		lis, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatalf("failed to listen: %v", err)
		}

		if err := srv.Serve(lis); err != nil {
			t.Fatalf("failed to serve: %v", err)
		}

		httpLis, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatalf("failed to listen http: %v", err)
		}
		if err := srv.ServeHTTPDebug(httpLis); err != nil {
			t.Fatalf("failed to start debug http: %v", err)
		}

		// exercise debug http
		resp, err := http.Get("http://" + httpLis.Addr().String() + "/status")
		if err == nil {
			_ = resp.Body.Close()
		}

		// exercise state mutation and election timer reset
		srv.ResetElectionTimeout()
		srv.SetState(raft.Follower)

		// Close server
		if err := srv.Close(); err != nil {
			t.Fatalf("failed to close server: %v", err)
		}
	}

	// Verify all goroutines terminate and return to baseline
	var current int
	settled := false
	for deadline := time.Now().Add(3 * time.Second); time.Now().Before(deadline); {
		runtime.GC()
		current = runtime.NumGoroutine()
		if current <= baseline {
			settled = true
			break
		}
		time.Sleep(50 * time.Millisecond)
	}

	if !settled {
		t.Fatalf("goroutine leak detected: baseline=%d, current=%d", baseline, current)
	}
}

// ---------------------------------------------------------------------------
// Raft Paper Figure 2 Rules 1-2 Tests (RequestVote & AppendEntries)
// ---------------------------------------------------------------------------

func TestRequestVote_Figure2_Rule1_RejectStaleTerm(t *testing.T) {
	srv := NewServer(1, nil, &raft.PersistentState{
		CurrentTerm: 4,
		VotedFor:    -1,
	}, nil)
	defer srv.Close()

	initialTimer := srv.lastHeartbeat

	// Candidate with term 2 (stale compared to receiver's term 4)
	reply := srv.processRequestVote(&raftpb.RequestVoteArgs{
		Term:         2,
		CandidateId:  2,
		LastLogIndex: 0,
		LastLogTerm:  0,
	})

	if reply.VoteGranted {
		t.Fatalf("Figure 2 Rule 1: expected VoteGranted false for stale term")
	}
	if reply.Term != 4 {
		t.Fatalf("Figure 2 Rule 1: expected reply term 4 (currentTerm), got %d", reply.Term)
	}
	if srv.PersistentState.CurrentTerm != 4 {
		t.Fatalf("receiver currentTerm should not change on stale RPC, got %d", srv.PersistentState.CurrentTerm)
	}
	if srv.PersistentState.VotedFor != -1 {
		t.Fatalf("receiver votedFor should remain -1, got %d", srv.PersistentState.VotedFor)
	}
	if !srv.lastHeartbeat.Equal(initialTimer) {
		t.Fatalf("stale RequestVote must not reset election timer")
	}
}

func TestRequestVote_Figure2_StepDownOnHigherTerm(t *testing.T) {
	t.Run("CandidateStepsDownOnHigherTerm", func(t *testing.T) {
		srv := NewServer(1, nil, &raft.PersistentState{
			CurrentTerm: 2,
			VotedFor:    1,
		}, nil)
		defer srv.Close()
		srv.SetState(raft.Candidate)

		reply := srv.processRequestVote(&raftpb.RequestVoteArgs{
			Term:         5,
			CandidateId:  2,
			LastLogIndex: 0,
			LastLogTerm:  0,
		})

		if !reply.VoteGranted {
			t.Fatalf("expected vote granted to candidate 2 on higher term")
		}
		if reply.Term != 5 {
			t.Fatalf("expected reply term 5, got %d", reply.Term)
		}
		if srv.GetState() != raft.Follower {
			t.Fatalf("expected node to convert to Follower, got %s", srv.GetState())
		}
		if srv.PersistentState.CurrentTerm != 5 {
			t.Fatalf("expected currentTerm 5, got %d", srv.PersistentState.CurrentTerm)
		}
		if srv.PersistentState.VotedFor != 2 {
			t.Fatalf("expected votedFor 2, got %d", srv.PersistentState.VotedFor)
		}
	})

	t.Run("LeaderStepsDownOnHigherTermAndStopsHeartbeat", func(t *testing.T) {
		srv := NewServer(1, nil, &raft.PersistentState{
			CurrentTerm: 3,
			VotedFor:    1,
		}, nil)
		defer srv.Close()
		srv.becomeLeader()

		if srv.GetHeartbeatTicker() == nil {
			t.Fatalf("expected active heartbeat ticker for leader")
		}

		reply := srv.processRequestVote(&raftpb.RequestVoteArgs{
			Term:         6,
			CandidateId:  3,
			LastLogIndex: 0,
			LastLogTerm:  0,
		})

		if !reply.VoteGranted {
			t.Fatalf("expected vote granted to candidate 3")
		}
		if reply.Term != 6 {
			t.Fatalf("expected reply term 6, got %d", reply.Term)
		}
		if srv.GetState() != raft.Follower {
			t.Fatalf("expected leader to step down to Follower, got %s", srv.GetState())
		}
		if srv.GetHeartbeatTicker() != nil {
			t.Fatalf("expected heartbeat ticker stopped after stepping down to Follower")
		}
	})
}

func TestRequestVote_Figure2_Rule2_GrantVoteWhenVotedForNull(t *testing.T) {
	srv := NewServer(1, nil, &raft.PersistentState{
		CurrentTerm: 2,
		VotedFor:    -1, // null vote
	}, nil)
	defer srv.Close()

	reply := srv.processRequestVote(&raftpb.RequestVoteArgs{
		Term:         2,
		CandidateId:  3,
		LastLogIndex: 0,
		LastLogTerm:  0,
	})

	if !reply.VoteGranted {
		t.Fatalf("Figure 2 Rule 2: expected vote to be granted when votedFor is null")
	}
	if reply.Term != 2 {
		t.Fatalf("expected reply term 2, got %d", reply.Term)
	}
	if srv.PersistentState.VotedFor != 3 {
		t.Fatalf("expected votedFor to be updated to 3, got %d", srv.PersistentState.VotedFor)
	}

	loaded, err := srv.GetStorage().Load()
	if err != nil {
		t.Fatalf("failed to load storage: %v", err)
	}
	if loaded.VotedFor != 3 || loaded.CurrentTerm != 2 {
		t.Fatalf("expected persisted state (term=2, votedFor=3), got %+v", loaded)
	}
}

func TestRequestVote_Figure2_Rule2_GrantVoteWhenAlreadyVotedForSameCandidate(t *testing.T) {
	srv := NewServer(1, nil, &raft.PersistentState{
		CurrentTerm: 2,
		VotedFor:    3, // already voted for candidate 3
	}, nil)
	defer srv.Close()

	// Retransmitted RequestVote from candidate 3
	reply := srv.processRequestVote(&raftpb.RequestVoteArgs{
		Term:         2,
		CandidateId:  3,
		LastLogIndex: 0,
		LastLogTerm:  0,
	})

	if !reply.VoteGranted {
		t.Fatalf("Figure 2 Rule 2: expected vote granted when already voted for same candidate")
	}
	if srv.PersistentState.VotedFor != 3 {
		t.Fatalf("votedFor should remain 3, got %d", srv.PersistentState.VotedFor)
	}
}

func TestRequestVote_Figure2_Rule2_RejectVoteWhenAlreadyVotedForDifferentCandidate(t *testing.T) {
	srv := NewServer(1, nil, &raft.PersistentState{
		CurrentTerm: 2,
		VotedFor:    3, // already voted for candidate 3
	}, nil)
	defer srv.Close()

	initialTimer := srv.lastHeartbeat

	// Competing candidate 4 requests vote in the same term
	reply := srv.processRequestVote(&raftpb.RequestVoteArgs{
		Term:         2,
		CandidateId:  4,
		LastLogIndex: 0,
		LastLogTerm:  0,
	})

	if reply.VoteGranted {
		t.Fatalf("Figure 2 Rule 2: must reject vote when already voted for a different candidate in same term")
	}
	if reply.Term != 2 {
		t.Fatalf("expected reply term 2, got %d", reply.Term)
	}
	if srv.PersistentState.VotedFor != 3 {
		t.Fatalf("votedFor must remain 3, got %d", srv.PersistentState.VotedFor)
	}
	if !srv.lastHeartbeat.Equal(initialTimer) {
		t.Fatalf("denying vote must not reset election timer")
	}
}

func TestRequestVote_Figure2_Rule2_RejectWhenLogNotUpToDate(t *testing.T) {
	srv := NewServer(1, nil, &raft.PersistentState{
		CurrentTerm: 1,
		VotedFor:    -1,
	}, nil)
	defer srv.Close()

	// Configure mock/hook to simulate candidate's log being stale
	srv.SetLogUpToDateChecker(func(lastLogIndex uint64, lastLogTerm uint64) bool {
		return false
	})

	reply := srv.processRequestVote(&raftpb.RequestVoteArgs{
		Term:         1,
		CandidateId:  2,
		LastLogIndex: 0,
		LastLogTerm:  0,
	})

	if reply.VoteGranted {
		t.Fatalf("Figure 2 Rule 2: must deny vote when candidate's log is not up to date")
	}
	if srv.PersistentState.VotedFor != -1 {
		t.Fatalf("votedFor must remain -1 on rejected vote, got %d", srv.PersistentState.VotedFor)
	}
}

func TestServer_LastLogInfo(t *testing.T) {
	// Empty log should return (0, 0)
	srv := NewServer(1, nil, &raft.PersistentState{
		CurrentTerm: 1,
		VotedFor:    -1,
		Log:         []raft.LogEntry{},
	}, nil)
	defer srv.Close()

	lastIndex, lastTerm := srv.LastLogInfo()
	if lastIndex != 0 || lastTerm != 0 {
		t.Fatalf("expected (0, 0) for empty server log, got (%d, %d)", lastIndex, lastTerm)
	}

	// Populated log should return last entry's (Index, Term)
	srv.PersistentState.Log = []raft.LogEntry{
		{Index: 1, Term: 1},
		{Index: 2, Term: 1},
		{Index: 3, Term: 2},
	}
	lastIndex, lastTerm = srv.LastLogInfo()
	if lastIndex != 3 || lastTerm != 2 {
		t.Fatalf("expected (3, 2) for populated server log, got (%d, %d)", lastIndex, lastTerm)
	}
}

func TestRequestVote_Figure2_Rule2_LogUpToDate_TableDriven(t *testing.T) {
	// Raft paper §5.4.1:
	// "If the logs have last entries with different terms, then the log with the later term is more up-to-date.
	//  If the logs end with the same term, then whichever log is longer is more up-to-date."
	//
	// Receiver log baseline: 3 entries ending at Index: 3, Term: 2.
	receiverLog := []raft.LogEntry{
		{Index: 1, Term: 1, Command: "x<-1"},
		{Index: 2, Term: 1, Command: "x<-2"},
		{Index: 3, Term: 2, Command: "x<-3"},
	}

	tests := []struct {
		name          string
		candLastIndex uint64
		candLastTerm  uint64
		wantGranted   bool
		paperCase     string
	}{
		// Case 1: Candidate last log term > Receiver last log term -> Grant
		{
			name:          "Case 1a: Higher term with shorter log",
			candLastIndex: 1,
			candLastTerm:  3,
			wantGranted:   true,
			paperCase:     "Case 1: candidateLastLogTerm > receiverLastLogTerm",
		},
		{
			name:          "Case 1b: Higher term with longer log",
			candLastIndex: 5,
			candLastTerm:  3,
			wantGranted:   true,
			paperCase:     "Case 1: candidateLastLogTerm > receiverLastLogTerm",
		},

		// Case 2: Candidate last log term < Receiver last log term -> Deny
		{
			name:          "Case 2a: Lower term with longer log",
			candLastIndex: 10,
			candLastTerm:  1,
			wantGranted:   false,
			paperCase:     "Case 2: candidateLastLogTerm < receiverLastLogTerm",
		},
		{
			name:          "Case 2b: Lower term with shorter log",
			candLastIndex: 2,
			candLastTerm:  1,
			wantGranted:   false,
			paperCase:     "Case 2: candidateLastLogTerm < receiverLastLogTerm",
		},

		// Case 3: Candidate last log term == Receiver last log term AND candidateLastIndex >= receiverLastIndex -> Grant
		{
			name:          "Case 3a: Same term with strictly longer log",
			candLastIndex: 4,
			candLastTerm:  2,
			wantGranted:   true,
			paperCase:     "Case 3: candidateLastLogTerm == receiverLastLogTerm && candidateLastIndex >= receiverLastIndex",
		},
		{
			name:          "Case 3b: Same term with equal log length",
			candLastIndex: 3,
			candLastTerm:  2,
			wantGranted:   true,
			paperCase:     "Case 3: candidateLastLogTerm == receiverLastLogTerm && candidateLastIndex >= receiverLastIndex",
		},

		// Case 4: Candidate last log term == Receiver last log term AND candidateLastIndex < receiverLastIndex -> Deny
		{
			name:          "Case 4a: Same term with strictly shorter log",
			candLastIndex: 2,
			candLastTerm:  2,
			wantGranted:   false,
			paperCase:     "Case 4: candidateLastLogTerm == receiverLastLogTerm && candidateLastIndex < receiverLastIndex",
		},
		{
			name:          "Case 4b: Same term with zero index",
			candLastIndex: 0,
			candLastTerm:  2,
			wantGranted:   false,
			paperCase:     "Case 4: candidateLastLogTerm == receiverLastLogTerm && candidateLastIndex < receiverLastIndex",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			storage := store.NewMemoryStorage()
			srv := NewServer(1, nil, &raft.PersistentState{
				CurrentTerm: 5,
				VotedFor:    -1,
				Log:         append([]raft.LogEntry(nil), receiverLog...),
			}, nil)
			srv.SetStorage(storage)
			defer srv.Close()

			// Test Server.IsLogUpToDate direct method
			isUpToDate := srv.IsLogUpToDate(tc.candLastIndex, tc.candLastTerm)
			if isUpToDate != tc.wantGranted {
				t.Fatalf("[%s] IsLogUpToDate(%d, %d) = %v; want %v",
					tc.paperCase, tc.candLastIndex, tc.candLastTerm, isUpToDate, tc.wantGranted)
			}

			// Test processRequestVote RPC handler with real check
			reply := srv.processRequestVote(&raftpb.RequestVoteArgs{
				Term:         5,
				CandidateId:  2,
				LastLogIndex: tc.candLastIndex,
				LastLogTerm:  tc.candLastTerm,
			})

			if reply.VoteGranted != tc.wantGranted {
				t.Fatalf("[%s] processRequestVote: VoteGranted = %v; want %v",
					tc.paperCase, reply.VoteGranted, tc.wantGranted)
			}

			if tc.wantGranted {
				if srv.PersistentState.VotedFor != 2 {
					t.Fatalf("expected votedFor=2 on granted vote, got %d", srv.PersistentState.VotedFor)
				}
				// Verify persistence
				stored, err := storage.Load()
				if err != nil || stored.VotedFor != 2 {
					t.Fatalf("expected persisted votedFor=2, got err=%v, votedFor=%v", err, stored.VotedFor)
				}
			} else {
				if srv.PersistentState.VotedFor != -1 {
					t.Fatalf("expected votedFor=-1 on denied vote, got %d", srv.PersistentState.VotedFor)
				}
			}
		})
	}
}

func TestRequestVote_Figure2_HigherTermWithStaleLog_StepsDownButDeniesVote(t *testing.T) {
	// If candidate has higher term (T > currentTerm), receiver steps down to Follower with term T.
	// However, if candidate's log is stale, the receiver must deny the vote and persist votedFor = -1.
	storage := store.NewMemoryStorage()
	srv := NewServer(1, nil, &raft.PersistentState{
		CurrentTerm: 5,
		VotedFor:    -1,
		Log: []raft.LogEntry{
			{Index: 1, Term: 1},
			{Index: 2, Term: 3},
			{Index: 3, Term: 3},
		},
	}, nil)
	srv.SetStorage(storage)
	defer srv.Close()

	// Candidate has term 6 (higher than 5), but lastLogTerm 2 < 3 (Case 2: stale log)
	reply := srv.processRequestVote(&raftpb.RequestVoteArgs{
		Term:         6,
		CandidateId:  2,
		LastLogIndex: 10,
		LastLogTerm:  2,
	})

	if reply.VoteGranted {
		t.Fatalf("vote must be denied when candidate log is stale even with higher term")
	}
	if reply.Term != 6 {
		t.Fatalf("reply term must be updated term 6, got %d", reply.Term)
	}
	if srv.PersistentState.CurrentTerm != 6 {
		t.Fatalf("server currentTerm must update to 6, got %d", srv.PersistentState.CurrentTerm)
	}
	if srv.PersistentState.VotedFor != -1 {
		t.Fatalf("votedFor must remain -1 when vote is denied despite term step-down, got %d", srv.PersistentState.VotedFor)
	}

	// Verify atomic persistence of term update with unvoted state
	stored, err := storage.Load()
	if err != nil {
		t.Fatalf("failed to load state from storage: %v", err)
	}
	if stored.CurrentTerm != 6 || stored.VotedFor != -1 {
		t.Fatalf("expected persisted state term=6, votedFor=-1; got term=%d, votedFor=%d", stored.CurrentTerm, stored.VotedFor)
	}
}

func TestRequestVote_Figure2_EmptyLogComparisons(t *testing.T) {
	// Subtest 1: Both receiver and candidate have empty logs -> vote granted (Case 3)
	t.Run("BothLogsEmpty_GrantsVote", func(t *testing.T) {
		srv := NewServer(1, nil, &raft.PersistentState{
			CurrentTerm: 1,
			VotedFor:    -1,
			Log:         nil,
		}, nil)
		defer srv.Close()

		reply := srv.processRequestVote(&raftpb.RequestVoteArgs{
			Term:         1,
			CandidateId:  2,
			LastLogIndex: 0,
			LastLogTerm:  0,
		})
		if !reply.VoteGranted {
			t.Fatalf("expected vote granted when both logs are empty")
		}
	})

	// Subtest 2: Receiver has empty log, candidate has entries -> vote granted (Case 1)
	t.Run("ReceiverEmpty_CandidatePopulated_GrantsVote", func(t *testing.T) {
		srv := NewServer(1, nil, &raft.PersistentState{
			CurrentTerm: 1,
			VotedFor:    -1,
			Log:         nil,
		}, nil)
		defer srv.Close()

		reply := srv.processRequestVote(&raftpb.RequestVoteArgs{
			Term:         1,
			CandidateId:  2,
			LastLogIndex: 2,
			LastLogTerm:  1,
		})
		if !reply.VoteGranted {
			t.Fatalf("expected vote granted when receiver log is empty and candidate has entries")
		}
	})

	// Subtest 3: Receiver has entries, candidate has empty log -> vote denied (Case 2)
	t.Run("ReceiverPopulated_CandidateEmpty_DeniesVote", func(t *testing.T) {
		srv := NewServer(1, nil, &raft.PersistentState{
			CurrentTerm: 1,
			VotedFor:    -1,
			Log: []raft.LogEntry{
				{Index: 1, Term: 1},
			},
		}, nil)
		defer srv.Close()

		reply := srv.processRequestVote(&raftpb.RequestVoteArgs{
			Term:         1,
			CandidateId:  2,
			LastLogIndex: 0,
			LastLogTerm:  0,
		})
		if reply.VoteGranted {
			t.Fatalf("expected vote denied when receiver has entries and candidate has empty log")
		}
	})
}

func TestAppendEntries_Figure2_Rule1_RejectStaleTerm(t *testing.T) {
	srv := NewServer(1, nil, &raft.PersistentState{
		CurrentTerm: 5,
		VotedFor:    -1,
	}, nil)
	defer srv.Close()

	initialTimer := srv.lastHeartbeat

	// Leader with term 3 sending AppendEntries to follower in term 5
	reply := srv.processAppendEntries(&raftpb.AppendEntriesArgs{
		Term:         3,
		LeaderId:     2,
		PrevLogIndex: 0,
		PrevLogTerm:  0,
	})

	if reply.Success {
		t.Fatalf("Figure 2 AppendEntries Rule 1: expected Success false for stale term")
	}
	if reply.Term != 5 {
		t.Fatalf("expected reply term 5 (currentTerm), got %d", reply.Term)
	}
	if !srv.lastHeartbeat.Equal(initialTimer) {
		t.Fatalf("stale AppendEntries must not reset election timer")
	}
}

func TestAppendEntries_Figure2_StepDownOnHigherTerm(t *testing.T) {
	srv := NewServer(1, nil, &raft.PersistentState{
		CurrentTerm: 2,
		VotedFor:    1,
	}, nil)
	defer srv.Close()
	srv.becomeLeader()

	// Incoming AppendEntries with term 7 from new leader 3
	reply := srv.processAppendEntries(&raftpb.AppendEntriesArgs{
		Term:         7,
		LeaderId:     3,
		PrevLogIndex: 0,
		PrevLogTerm:  0,
	})

	if reply.Term != 7 {
		t.Fatalf("expected reply term 7, got %d", reply.Term)
	}
	if !reply.Success {
		t.Fatalf("expected valid heartbeat on higher term to succeed")
	}
	if srv.GetState() != raft.Follower {
		t.Fatalf("expected leader to step down to Follower, got %s", srv.GetState())
	}
	if srv.PersistentState.CurrentTerm != 7 {
		t.Fatalf("expected currentTerm 7, got %d", srv.PersistentState.CurrentTerm)
	}
	if srv.PersistentState.VotedFor != -1 {
		t.Fatalf("expected votedFor reset to -1, got %d", srv.PersistentState.VotedFor)
	}
	if srv.GetHeartbeatTicker() != nil {
		t.Fatalf("expected heartbeat ticker to be stopped")
	}
}

func TestAppendEntries_Figure2_CandidateRevertsOnSameTermLeader(t *testing.T) {
	srv := NewServer(1, nil, &raft.PersistentState{
		CurrentTerm: 3,
		VotedFor:    1,
	}, nil)
	defer srv.Close()
	srv.SetState(raft.Candidate)

	// AppendEntries with same term 3 from elected leader 2 (§5.2)
	reply := srv.processAppendEntries(&raftpb.AppendEntriesArgs{
		Term:         3,
		LeaderId:     2,
		PrevLogIndex: 0,
		PrevLogTerm:  0,
	})

	if !reply.Success {
		t.Fatalf("expected heartbeat from current leader to succeed")
	}
	if srv.GetState() != raft.Follower {
		t.Fatalf("candidate should revert to Follower on receiving AppendEntries from current leader, got %s", srv.GetState())
	}
}

func TestAppendEntries_Figure2_Rule2_LogMatching(t *testing.T) {
	logEntries := []raft.LogEntry{
		{Index: 1, Term: 1, Command: []byte("set a=1")},
		{Index: 2, Term: 2, Command: []byte("set b=2")},
		{Index: 3, Term: 2, Command: []byte("set c=3")},
	}

	srv := NewServer(1, nil, &raft.PersistentState{
		CurrentTerm: 3,
		VotedFor:    -1,
		Log:         logEntries,
	}, nil)
	defer srv.Close()

	// 1. PrevLogIndex == 0 always matches (start of log)
	reply1 := srv.processAppendEntries(&raftpb.AppendEntriesArgs{
		Term:         3,
		LeaderId:     2,
		PrevLogIndex: 0,
		PrevLogTerm:  0,
	})
	if !reply1.Success {
		t.Fatalf("expected success for PrevLogIndex=0")
	}

	// 2. Matching PrevLogIndex and PrevLogTerm (index 2, term 2)
	reply2 := srv.processAppendEntries(&raftpb.AppendEntriesArgs{
		Term:         3,
		LeaderId:     2,
		PrevLogIndex: 2,
		PrevLogTerm:  2,
	})
	if !reply2.Success {
		t.Fatalf("expected success when prevLogIndex and prevLogTerm match log entry")
	}

	// 3. Term mismatch at PrevLogIndex (index 2, actual term 2 != prevLogTerm 1)
	reply3 := srv.processAppendEntries(&raftpb.AppendEntriesArgs{
		Term:         3,
		LeaderId:     2,
		PrevLogIndex: 2,
		PrevLogTerm:  1,
	})
	if reply3.Success {
		t.Fatalf("Figure 2 Rule 2: expected failure when term at prevLogIndex does not match")
	}

	// 4. Missing entry at PrevLogIndex (index 5 > log length 3)
	reply4 := srv.processAppendEntries(&raftpb.AppendEntriesArgs{
		Term:         3,
		LeaderId:     2,
		PrevLogIndex: 5,
		PrevLogTerm:  2,
	})
	if reply4.Success {
		t.Fatalf("Figure 2 Rule 2: expected failure when log does not contain entry at prevLogIndex")
	}
}

// ---------------------------------------------------------------------------
// Leader Election: Parallel RequestVote, Vote Counting, and Term Handling
// ---------------------------------------------------------------------------

func TestElection_OnTimeout_TransitionsToCandidate_IncrementsTerm_VotesForSelf(t *testing.T) {
	srv := NewServer(1, []Peer{
		{ID: 2, Address: "127.0.0.1:60002"},
		{ID: 3, Address: "127.0.0.1:60003"},
	}, &raft.PersistentState{
		CurrentTerm: 2,
		VotedFor:    -1,
		Log:         make([]raft.LogEntry, 0),
	}, nil)
	defer srv.Close()

	if srv.GetState() != raft.Follower {
		t.Fatalf("expected initial Follower state, got %v", srv.GetState())
	}

	// Trigger election timeout
	srv.handleElectionTimeout(time.Now())

	// 1. Transition to Candidate
	if srv.GetState() != raft.Candidate {
		t.Fatalf("expected state Candidate after election timeout, got %v", srv.GetState())
	}

	// 2. Increment term (2 -> 3)
	if srv.PersistentState.CurrentTerm != 3 {
		t.Fatalf("expected CurrentTerm incremented to 3, got %d", srv.PersistentState.CurrentTerm)
	}

	// 3. Vote for self
	if srv.PersistentState.VotedFor != 1 {
		t.Fatalf("expected VotedFor self (1), got %d", srv.PersistentState.VotedFor)
	}

	// 4. Record self-vote in votesReceived
	votes := srv.VotesReceived()
	if !votes[1] {
		t.Fatalf("expected self-vote recorded in votesReceived")
	}
	if len(votes) != 1 {
		t.Fatalf("expected exactly 1 vote (self) initially, got %d", len(votes))
	}

	// 5. Persisted state verified
	loaded, err := srv.GetStorage().Load()
	if err != nil {
		t.Fatalf("failed to load storage: %v", err)
	}
	if loaded.CurrentTerm != 3 || loaded.VotedFor != 1 {
		t.Fatalf("expected persisted state (term=3, votedFor=1), got %+v", loaded)
	}
}

func TestElection_SendRequestVoteInParallel_ResultsChannel(t *testing.T) {
	// Start two peer servers that will grant votes
	peer2 := NewServer(2, nil, &raft.PersistentState{CurrentTerm: 0, VotedFor: -1}, nil)
	defer peer2.Close()
	lis2, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("failed to listen peer 2: %v", err)
	}
	if err := peer2.Serve(lis2); err != nil {
		t.Fatalf("failed to serve peer 2: %v", err)
	}

	peer3 := NewServer(3, nil, &raft.PersistentState{CurrentTerm: 0, VotedFor: -1}, nil)
	defer peer3.Close()
	lis3, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("failed to listen peer 3: %v", err)
	}
	if err := peer3.Serve(lis3); err != nil {
		t.Fatalf("failed to serve peer 3: %v", err)
	}

	peers := []Peer{
		{ID: 2, Address: lis2.Addr().String()},
		{ID: 3, Address: lis3.Addr().String()},
	}

	srv := NewServer(1, peers, &raft.PersistentState{CurrentTerm: 1, VotedFor: 1}, nil)
	defer srv.Close()

	// Connect peers in client pool
	if err := srv.ConnectPeers(); err != nil {
		t.Fatalf("failed to connect peers: %v", err)
	}

	resCh := make(chan RequestVoteResult, len(peers))
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	// Send RequestVote RPCs in parallel to all peers using goroutines + results channel
	srv.SendRequestVotesParallel(ctx, 2, peers, resCh)

	// Collect responses from the results channel
	collected := make(map[raft.NodeID]*raft.RequestVoteReply)
	for i := 0; i < len(peers); i++ {
		select {
		case res := <-resCh:
			if res.Err != nil {
				t.Fatalf("unexpected RPC error from peer %d: %v", res.PeerID, res.Err)
			}
			if res.Reply == nil {
				t.Fatalf("nil reply received from peer %d", res.PeerID)
			}
			collected[res.PeerID] = res.Reply
		case <-time.After(2 * time.Second):
			t.Fatalf("timed out collecting parallel RequestVote responses (collected %d/%d)", len(collected), len(peers))
		}
	}

	// Verify both peers responded and granted vote
	for _, p := range peers {
		reply, exists := collected[p.ID]
		if !exists {
			t.Fatalf("missing response for peer %d", p.ID)
		}
		if !reply.VoteGranted {
			t.Fatalf("expected vote granted from peer %d", p.ID)
		}
		if reply.Term != 2 {
			t.Fatalf("expected reply term 2 from peer %d, got %d", p.ID, reply.Term)
		}
	}
}

func TestElection_VoteCounting_AchievesMajority_BecomesLeader(t *testing.T) {
	// 3-node cluster: majority is 2 out of 3
	peers := []Peer{
		{ID: 2, Address: "127.0.0.1:60002"},
		{ID: 3, Address: "127.0.0.1:60003"},
	}

	srv := NewServer(1, peers, &raft.PersistentState{
		CurrentTerm: 1,
		VotedFor:    -1,
	}, nil)
	defer srv.Close()

	// Transition to Candidate for Term 2 (has self-vote: 1 vote out of 3)
	srv.becomeCandidate()
	if srv.GetState() != raft.Candidate {
		t.Fatalf("expected Candidate state, got %s", srv.GetState())
	}

	// Deliver vote from Peer 2 -> 2 votes out of 3 (Majority reached!)
	srv.HandleVoteResult(RequestVoteResult{
		PeerID:       2,
		ElectionTerm: 2,
		Reply: &raft.RequestVoteReply{
			Term:        2,
			VoteGranted: true,
		},
	})

	if srv.GetState() != raft.Leader {
		t.Fatalf("expected Leader state after achieving majority, got %s", srv.GetState())
	}
	if srv.GetHeartbeatTicker() == nil {
		t.Fatalf("expected leader heartbeat ticker to be armed")
	}

	// Deliver redundant vote from Peer 3 -> 3 votes out of 3
	srv.HandleVoteResult(RequestVoteResult{
		PeerID:       3,
		ElectionTerm: 2,
		Reply: &raft.RequestVoteReply{
			Term:        2,
			VoteGranted: true,
		},
	})

	// Must remain Leader cleanly
	if srv.GetState() != raft.Leader {
		t.Fatalf("expected to remain Leader, got %s", srv.GetState())
	}
	votes := srv.VotesReceived()
	if len(votes) < 2 {
		t.Fatalf("expected at least majority votes recorded, got %d", len(votes))
	}
}

func TestElection_HigherTermSeen_StepsDownToFollower(t *testing.T) {
	peers := []Peer{
		{ID: 2, Address: "127.0.0.1:60002"},
		{ID: 3, Address: "127.0.0.1:60003"},
	}

	srv := NewServer(1, peers, &raft.PersistentState{
		CurrentTerm: 2,
		VotedFor:    1,
	}, nil)
	defer srv.Close()
	srv.SetState(raft.Candidate)

	// Peer 2 responds with higher term (term 5)
	srv.HandleVoteResult(RequestVoteResult{
		PeerID:       2,
		ElectionTerm: 2,
		Reply: &raft.RequestVoteReply{
			Term:        5,
			VoteGranted: false,
		},
	})

	// Must step down to Follower immediately (§5.1)
	if srv.GetState() != raft.Follower {
		t.Fatalf("expected Candidate to step down to Follower on seeing higher term, got %s", srv.GetState())
	}
	if srv.PersistentState.CurrentTerm != 5 {
		t.Fatalf("expected currentTerm updated to 5, got %d", srv.PersistentState.CurrentTerm)
	}
	if srv.PersistentState.VotedFor != -1 {
		t.Fatalf("expected votedFor reset to -1, got %d", srv.PersistentState.VotedFor)
	}

	loaded, err := srv.GetStorage().Load()
	if err != nil {
		t.Fatalf("failed to load storage: %v", err)
	}
	if loaded.CurrentTerm != 5 || loaded.VotedFor != -1 {
		t.Fatalf("expected persisted state (term=5, votedFor=-1), got %+v", loaded)
	}
}

func TestElection_IgnoresStaleElectionResults(t *testing.T) {
	peers := []Peer{
		{ID: 2, Address: "127.0.0.1:60002"},
		{ID: 3, Address: "127.0.0.1:60003"},
	}

	srv := NewServer(1, peers, &raft.PersistentState{
		CurrentTerm: 5,
		VotedFor:    1,
	}, nil)
	defer srv.Close()
	srv.SetState(raft.Candidate)

	// Late response arriving from an earlier election cycle (term 3)
	srv.HandleVoteResult(RequestVoteResult{
		PeerID:       2,
		ElectionTerm: 3,
		Reply: &raft.RequestVoteReply{
			Term:        3,
			VoteGranted: true,
		},
	})

	// Stale vote should NOT be counted in current term 5
	votes := srv.VotesReceived()
	if votes[2] {
		t.Fatalf("vote from old election term must not be recorded in current election")
	}
	if srv.GetState() != raft.Candidate {
		t.Fatalf("state should remain Candidate, got %s", srv.GetState())
	}
}

func TestElection_FullClusterElectionsViaEventLoop(t *testing.T) {
	// Spin up 3 live servers with event loops and gRPC listeners
	srv2 := NewServer(2, nil, &raft.PersistentState{CurrentTerm: 0, VotedFor: -1}, nil)
	defer srv2.Close()
	lis2, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen 2: %v", err)
	}
	if err := srv2.Serve(lis2); err != nil {
		t.Fatalf("serve 2: %v", err)
	}

	srv3 := NewServer(3, nil, &raft.PersistentState{CurrentTerm: 0, VotedFor: -1}, nil)
	defer srv3.Close()
	lis3, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen 3: %v", err)
	}
	if err := srv3.Serve(lis3); err != nil {
		t.Fatalf("serve 3: %v", err)
	}

	peers1 := []Peer{
		{ID: 2, Address: lis2.Addr().String()},
		{ID: 3, Address: lis3.Addr().String()},
	}

	srv1 := NewServer(1, peers1, &raft.PersistentState{CurrentTerm: 0, VotedFor: -1}, nil)
	defer srv1.Close()
	lis1, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen 1: %v", err)
	}
	if err := srv1.Serve(lis1); err != nil {
		t.Fatalf("serve 1: %v", err)
	}

	// Connect srv1 to peers
	if err := srv1.ConnectPeers(); err != nil {
		t.Fatalf("connect peers: %v", err)
	}

	// Start election on Node 1 via event loop
	srv1.StartElection()

	// Wait for election to resolve across the network
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if srv1.GetState() == raft.Leader {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}

	if srv1.GetState() != raft.Leader {
		t.Fatalf("expected Node 1 to achieve majority and become Leader, got %s", srv1.GetState())
	}
	if srv1.PersistentState.CurrentTerm < 1 {
		t.Fatalf("expected term >= 1, got %d", srv1.PersistentState.CurrentTerm)
	}
	if srv1.GetHeartbeatTicker() == nil {
		t.Fatalf("expected leader heartbeat ticker to be active")
	}
}

func TestRequestVote_ResetTimerOnGrantingVote_ReducesSplitVotes(t *testing.T) {
	// Raft paper §5.2 / Figure 2:
	// "If election timeout elapses without receiving AppendEntries RPC from current leader
	//  or granting vote to candidate: convert to candidate"
	//
	// Resetting the election timer when granting a vote reduces the likelihood of split votes
	// by giving the granted candidate time to collect remaining votes and establish leadership.

	t.Run("GrantVote_ResetsElectionTimerAndUpdatesHeartbeat", func(t *testing.T) {
		srv := NewServer(1, nil, &raft.PersistentState{
			CurrentTerm: 2,
			VotedFor:    -1,
		}, nil)
		defer srv.Close()

		// Allow initial timer time to elapse slightly
		time.Sleep(15 * time.Millisecond)
		hbBefore := srv.LastHeartbeatTime()
		grantedBefore := srv.VotesGranted()

		reply := srv.processRequestVote(&raftpb.RequestVoteArgs{
			Term:         2,
			CandidateId:  3,
			LastLogIndex: 0,
			LastLogTerm:  0,
		})

		if !reply.VoteGranted {
			t.Fatalf("expected vote to be granted")
		}
		if !srv.LastHeartbeatTime().After(hbBefore) {
			t.Fatalf("granting vote MUST update lastHeartbeat (reset election timer)")
		}
		if srv.VotesGranted() != grantedBefore+1 {
			t.Fatalf("VotesGranted counter should increment by 1, got %d", srv.VotesGranted())
		}
	})

	t.Run("DenyVote_AlreadyVoted_DoesNotResetElectionTimer", func(t *testing.T) {
		srv := NewServer(1, nil, &raft.PersistentState{
			CurrentTerm: 2,
			VotedFor:    3, // already voted for candidate 3 in term 2
		}, nil)
		defer srv.Close()

		time.Sleep(15 * time.Millisecond)
		hbBefore := srv.LastHeartbeatTime()
		grantedBefore := srv.VotesGranted()

		// Candidate 4 requests vote in same term 2 -> denied
		reply := srv.processRequestVote(&raftpb.RequestVoteArgs{
			Term:         2,
			CandidateId:  4,
			LastLogIndex: 0,
			LastLogTerm:  0,
		})

		if reply.VoteGranted {
			t.Fatalf("expected vote to be denied")
		}
		if !srv.LastHeartbeatTime().Equal(hbBefore) {
			t.Fatalf("denying vote MUST NOT reset election timer, before=%v, after=%v", hbBefore, srv.LastHeartbeatTime())
		}
		if srv.VotesGranted() != grantedBefore {
			t.Fatalf("VotesGranted counter should NOT change on denied vote")
		}
	})

	t.Run("DenyVote_StaleLog_DoesNotResetElectionTimer", func(t *testing.T) {
		srv := NewServer(1, nil, &raft.PersistentState{
			CurrentTerm: 2,
			VotedFor:    -1,
			Log: []raft.LogEntry{
				{Index: 1, Term: 2},
			},
		}, nil)
		defer srv.Close()

		time.Sleep(15 * time.Millisecond)
		hbBefore := srv.LastHeartbeatTime()
		grantedBefore := srv.VotesGranted()

		// Candidate 3 has empty log (stale compared to receiver's log) -> denied
		reply := srv.processRequestVote(&raftpb.RequestVoteArgs{
			Term:         2,
			CandidateId:  3,
			LastLogIndex: 0,
			LastLogTerm:  0,
		})

		if reply.VoteGranted {
			t.Fatalf("expected vote denied due to stale log")
		}
		if !srv.LastHeartbeatTime().Equal(hbBefore) {
			t.Fatalf("denying vote due to stale log MUST NOT reset election timer")
		}
		if srv.VotesGranted() != grantedBefore {
			t.Fatalf("VotesGranted counter should NOT change on denied vote")
		}
	})
}

func TestElection_SplitVote_TwoCandidatesSameTerm_ResolvesToOneLeader(t *testing.T) {
	// Scenario:
	// 1. A 3-node cluster (nodes 1, 2, 3) is started with event loops and full RPC connectivity.
	// 2. Both Candidate 1 and Candidate 2 start elections simultaneously in Term 1 (forced via test hook).
	// 3. Node 3 withholds its vote in Term 1 via test filter hook, ensuring a split vote in Term 1 (Node 1 gets 1 vote, Node 2 gets 1 vote).
	// 4. In Term 1, neither candidate reaches majority (majority is 2).
	// 5. As election timeouts expire, randomized durations ensure one candidate times out first.
	// 6. That candidate increments term to Term 2, requests votes, and steps down the other candidate.
	// 7. Node 3 and the stepped-down candidate grant votes and reset their election timers.
	// 8. Exactly ONE candidate receives majority and becomes the sole cluster Leader.

	servers := make([]*Server, 3)
	for i := 0; i < 3; i++ {
		id := raft.NodeID(i + 1)
		srv := NewServer(id, nil, &raft.PersistentState{CurrentTerm: 0, VotedFor: -1}, nil)
		// Configure fast election timeout range [80ms, 180ms] for responsive test resolution
		if srv.GetElectionTimer() != nil {
			srv.GetElectionTimer().SetRange(80*time.Millisecond, 180*time.Millisecond)
			srv.ResetElectionTimeout()
		}
		if err := srv.Start("127.0.0.1:0"); err != nil {
			t.Fatalf("failed to start server %d: %v", id, err)
		}
		defer srv.Close()
		servers[i] = srv
	}

	// Build full mesh peer routing
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

	for _, srv := range servers {
		if err := srv.ConnectPeers(); err != nil {
			t.Fatalf("node %d failed to connect peers: %v", srv.ID, err)
		}
	}

	node1, node2, node3 := servers[0], servers[1], servers[2]

	// Node 3 test hook: reject/withhold votes in Term 1 to guarantee a split vote in Term 1
	node3.SetRequestVoteFilter(func(req *raftpb.RequestVoteArgs) bool {
		return req.Term > 1
	})

	// Force both Node 1 and Node 2 to start elections in Term 1 simultaneously
	node1.StartElectionInTerm(1)
	node2.StartElectionInTerm(1)

	// Short pause to let Term 1 RequestVote RPCs complete
	time.Sleep(50 * time.Millisecond)

	// Verify split vote in Term 1: neither node should have achieved leadership in Term 1
	s1 := node1.GetState()
	s2 := node2.GetState()
	if s1 == raft.Leader && node1.PersistentState.CurrentTerm == 1 {
		t.Fatalf("Node 1 should not become leader in Term 1 during forced split vote")
	}
	if s2 == raft.Leader && node2.PersistentState.CurrentTerm == 1 {
		t.Fatalf("Node 2 should not become leader in Term 1 during forced split vote")
	}

	// Now wait for randomized timeouts + term increments to resolve to exactly ONE leader
	deadline := time.Now().Add(3 * time.Second)
	var electedLeader *Server

	for time.Now().Before(deadline) {
		leaders := 0
		for _, srv := range servers {
			if srv.GetState() == raft.Leader {
				leaders++
				electedLeader = srv
			}
		}
		if leaders == 1 {
			// Found exactly one leader!
			break
		}
		time.Sleep(20 * time.Millisecond)
	}

	if electedLeader == nil {
		t.Fatalf("cluster failed to elect a leader after split vote within deadline; states: node1=%s (term %d), node2=%s (term %d), node3=%s (term %d)",
			node1.GetState(), node1.PersistentState.CurrentTerm,
			node2.GetState(), node2.PersistentState.CurrentTerm,
			node3.GetState(), node3.PersistentState.CurrentTerm,
		)
	}

	// Verify that the winning leader's term is at least 2 (proves split-vote timeout and term increment occurred)
	leaderTerm := electedLeader.PersistentState.CurrentTerm
	if leaderTerm < 2 {
		t.Fatalf("expected winning leader term >= 2 after split vote resolution, got %d", leaderTerm)
	}

	// Verify all other nodes stepped down to Follower
	followerCount := 0
	for _, srv := range servers {
		if srv.ID != electedLeader.ID {
			if srv.GetState() != raft.Follower {
				t.Fatalf("expected node %d to be Follower, got %s", srv.ID, srv.GetState())
			}
			followerCount++
		}
	}
	if followerCount != 2 {
		t.Fatalf("expected exactly 2 followers, got %d", followerCount)
	}

	// Verify exactly 1 leader in cluster
	leaders := 0
	for _, srv := range servers {
		if srv.GetState() == raft.Leader {
			leaders++
		}
	}
	if leaders != 1 {
		t.Fatalf("expected exactly 1 leader, got %d", leaders)
	}
}

func TestAppendEntries_FollowerResetsElectionTimerOnlyOnValidLeader(t *testing.T) {
	// Raft §5.2 / Figure 2:
	// A follower resets its election timer only on receiving valid AppendEntries
	// from the current leader (term == currentTerm) or a new leader (term > currentTerm).
	// Stale leaders (term < currentTerm) must be rejected and must NOT reset the timer.

	t.Run("CurrentTermLeader_ResetsTimerAndIncrementsCount", func(t *testing.T) {
		srv := NewServer(1, nil, &raft.PersistentState{
			CurrentTerm: 2,
			VotedFor:    -1,
		}, nil)
		defer srv.Close()

		time.Sleep(15 * time.Millisecond)
		hbBefore := srv.LastHeartbeatTime()
		hbCountBefore := srv.HeartbeatsReceived()

		reply := srv.processAppendEntries(&raftpb.AppendEntriesArgs{
			Term:     2,
			LeaderId: 2,
		})

		if !reply.Success {
			t.Fatalf("expected AppendEntries from current leader to succeed")
		}
		if !srv.LastHeartbeatTime().After(hbBefore) {
			t.Fatalf("valid AppendEntries MUST update LastHeartbeatTime (reset election timer)")
		}
		if srv.HeartbeatsReceived() != hbCountBefore+1 {
			t.Fatalf("HeartbeatsReceived should increment to %d, got %d", hbCountBefore+1, srv.HeartbeatsReceived())
		}
	})

	t.Run("HigherTermLeader_StepsDownUpdatesTermAndResetsTimer", func(t *testing.T) {
		srv := NewServer(1, nil, &raft.PersistentState{
			CurrentTerm: 2,
			VotedFor:    1,
		}, nil)
		defer srv.Close()

		time.Sleep(15 * time.Millisecond)
		hbBefore := srv.LastHeartbeatTime()
		hbCountBefore := srv.HeartbeatsReceived()

		reply := srv.processAppendEntries(&raftpb.AppendEntriesArgs{
			Term:     4,
			LeaderId: 3,
		})

		if !reply.Success {
			t.Fatalf("expected AppendEntries from higher term leader to succeed")
		}
		if reply.Term != 4 {
			t.Fatalf("expected reply term 4, got %d", reply.Term)
		}
		if srv.PersistentState.CurrentTerm != 4 {
			t.Fatalf("expected currentTerm updated to 4, got %d", srv.PersistentState.CurrentTerm)
		}
		if !srv.LastHeartbeatTime().After(hbBefore) {
			t.Fatalf("AppendEntries from higher term leader MUST update LastHeartbeatTime")
		}
		if srv.HeartbeatsReceived() != hbCountBefore+1 {
			t.Fatalf("HeartbeatsReceived should increment to %d, got %d", hbCountBefore+1, srv.HeartbeatsReceived())
		}
	})

	t.Run("StaleLeader_RejectsAndDoesNotResetTimer", func(t *testing.T) {
		srv := NewServer(1, nil, &raft.PersistentState{
			CurrentTerm: 5,
			VotedFor:    -1,
		}, nil)
		defer srv.Close()

		time.Sleep(15 * time.Millisecond)
		hbBefore := srv.LastHeartbeatTime()
		hbCountBefore := srv.HeartbeatsReceived()

		reply := srv.processAppendEntries(&raftpb.AppendEntriesArgs{
			Term:     3, // Stale: 3 < 5
			LeaderId: 2,
		})

		if reply.Success {
			t.Fatalf("AppendEntries from stale leader must fail")
		}
		if reply.Term != 5 {
			t.Fatalf("expected reply term 5, got %d", reply.Term)
		}
		if !srv.LastHeartbeatTime().Equal(hbBefore) {
			t.Fatalf("stale AppendEntries MUST NOT update LastHeartbeatTime (must NOT reset election timer)")
		}
		if srv.HeartbeatsReceived() != hbCountBefore {
			t.Fatalf("HeartbeatsReceived should NOT change on stale AppendEntries")
		}
	})
}

func TestLeader_BroadcastHeartbeats_SendsRealEmptyAppendEntries(t *testing.T) {
	// Verify that the leader dispatches real empty AppendEntries RPCs over the network
	// to connected cluster peers.
	follower := NewServer(2, nil, &raft.PersistentState{CurrentTerm: 1, VotedFor: -1}, nil)
	if err := follower.Start("127.0.0.1:0"); err != nil {
		t.Fatalf("failed to start follower: %v", err)
	}
	defer follower.Close()

	leader := NewServer(1, []Peer{
		{ID: 2, Address: follower.Addr().String()},
	}, &raft.PersistentState{CurrentTerm: 1, VotedFor: 1}, nil)
	leader.SetHeartbeatInterval(25 * time.Millisecond)
	if err := leader.Start("127.0.0.1:0"); err != nil {
		t.Fatalf("failed to start leader: %v", err)
	}
	defer leader.Close()

	if err := leader.ConnectPeers(); err != nil {
		t.Fatalf("leader failed to connect to follower: %v", err)
	}

	// Transition leader to Leader state
	leader.becomeLeader()

	// Wait for several heartbeat intervals to fire
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if follower.HeartbeatsReceived() >= 3 {
			break
		}
		time.Sleep(15 * time.Millisecond)
	}

	received := follower.HeartbeatsReceived()
	if received < 3 {
		t.Fatalf("expected follower to receive at least 3 real empty AppendEntries heartbeats, got %d", received)
	}
}

func TestCluster_LeaderSuppressesFollowerElectionsWhileHealthy(t *testing.T) {
	// Raft §5.2:
	// "A leader sends periodic heartbeats (AppendEntries RPCs that carry no log entries)
	//  to all followers in order to maintain its authority... If election timeout elapses
	//  without receiving AppendEntries RPC from current leader or granting vote to candidate: convert to candidate"
	//
	// Once a leader is elected, as long as it sends periodic heartbeats, no follower should time out,
	// and no further elections should occur while the leader is healthy.

	servers := make([]*Server, 3)
	for i := 0; i < 3; i++ {
		id := raft.NodeID(i + 1)
		srv := NewServer(id, nil, &raft.PersistentState{CurrentTerm: 0, VotedFor: -1}, nil)
		srv.SetHeartbeatInterval(20 * time.Millisecond)
		// Followers have election timeout range [80ms, 160ms]
		if srv.GetElectionTimer() != nil {
			srv.GetElectionTimer().SetRange(80*time.Millisecond, 160*time.Millisecond)
			srv.ResetElectionTimeout()
		}
		if err := srv.Start("127.0.0.1:0"); err != nil {
			t.Fatalf("failed to start server %d: %v", id, err)
		}
		defer srv.Close()
		servers[i] = srv
	}

	// Full mesh routing
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

	for _, srv := range servers {
		if err := srv.ConnectPeers(); err != nil {
			t.Fatalf("node %d failed to connect peers: %v", srv.ID, err)
		}
	}

	leaderNode := servers[0]
	followerA := servers[1]
	followerB := servers[2]

	// Start election on Node 1 to elect it leader
	leaderNode.StartElection()

	// Wait for Node 1 to become leader
	electionDeadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(electionDeadline) {
		if leaderNode.GetState() == raft.Leader {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}

	if leaderNode.GetState() != raft.Leader {
		t.Fatalf("expected Node 1 to become Leader, got %s", leaderNode.GetState())
	}

	initialTerm := leaderNode.PersistentState.CurrentTerm
	if initialTerm < 1 {
		t.Fatalf("expected leader term >= 1, got %d", initialTerm)
	}

	// Record heartbeats received so far by followers
	hbAStart := followerA.HeartbeatsReceived()
	hbBStart := followerB.HeartbeatsReceived()

	// Let the cluster run for 400ms:
	// - Heartbeat interval is 20ms (leader sends ~20 heartbeats)
	// - Follower election timeout is 80-160ms (3 to 5 timeout windows elapse)
	// While healthy heartbeats are being sent, neither follower should time out!
	time.Sleep(400 * time.Millisecond)

	// Verify Node 1 is STILL the ONLY leader
	if leaderNode.GetState() != raft.Leader {
		t.Fatalf("expected Node 1 to remain Leader, got %s", leaderNode.GetState())
	}
	if leaderNode.PersistentState.CurrentTerm != initialTerm {
		t.Fatalf("leader term changed from %d to %d (unexpected re-election occurred)", initialTerm, leaderNode.PersistentState.CurrentTerm)
	}

	// Verify both followers remained in Follower state
	if followerA.GetState() != raft.Follower {
		t.Fatalf("follower A transitioned to %s; expected to remain Follower", followerA.GetState())
	}
	if followerB.GetState() != raft.Follower {
		t.Fatalf("follower B transitioned to %s; expected to remain Follower", followerB.GetState())
	}

	// Verify followers received numerous heartbeats from the healthy leader
	if followerA.HeartbeatsReceived() <= hbAStart+5 {
		t.Fatalf("follower A received insufficient heartbeats: started with %d, now %d", hbAStart, followerA.HeartbeatsReceived())
	}
	if followerB.HeartbeatsReceived() <= hbBStart+5 {
		t.Fatalf("follower B received insufficient heartbeats: started with %d, now %d", hbBStart, followerB.HeartbeatsReceived())
	}

	// Verify no split brain: exactly 1 leader in the entire cluster
	leaderCount := 0
	for _, srv := range servers {
		if srv.GetState() == raft.Leader {
			leaderCount++
		}
	}
	if leaderCount != 1 {
		t.Fatalf("expected exactly 1 leader in cluster, got %d", leaderCount)
	}
}

func TestNode_Kill_And_PauseResume(t *testing.T) {
	srv := NewServer(1, nil, &raft.PersistentState{CurrentTerm: 1, VotedFor: -1}, nil)
	if err := srv.Start("127.0.0.1:0"); err != nil {
		t.Fatalf("failed to start server: %v", err)
	}
	defer srv.Close()

	srv.StartEventLoop()

	if srv.IsKilled() {
		t.Fatalf("expected node not killed initially")
	}
	if srv.IsPaused() {
		t.Fatalf("expected node not paused initially")
	}

	// Test Pause
	srv.Pause()
	if !srv.IsPaused() {
		t.Fatalf("expected node to report IsPaused() == true")
	}

	// Double pause is an idempotent no-op
	srv.Pause()
	if !srv.IsPaused() {
		t.Fatalf("expected node to still report IsPaused() == true")
	}

	// Test Resume
	srv.Resume()
	if srv.IsPaused() {
		t.Fatalf("expected node to report IsPaused() == false after Resume()")
	}

	// Double resume is an idempotent no-op
	srv.Resume()
	if srv.IsPaused() {
		t.Fatalf("expected node to still report IsPaused() == false")
	}

	// Test Kill
	srv.Kill()
	if !srv.IsKilled() {
		t.Fatalf("expected node to report IsKilled() == true after Kill()")
	}
}

func TestCluster_KillLeader_RemainingNodesElectNewLeader(t *testing.T) {
	// Raft §5.2: Leader failure and re-election.
	// 1. Setup a 3-node live cluster with full mesh connectivity and active event loops.
	// 2. Elect Node 1 as the initial leader in Term 1.
	// 3. Abruptly kill the leader process mid-test (simulating crash).
	// 4. Remaining followers detect leader silence via election timeout and elect a single new leader.
	// 5. Measure and log time-to-new-leader (should be roughly 1-3x election timeout).

	minTimeout := 120 * time.Millisecond
	maxTimeout := 240 * time.Millisecond
	heartbeatInterval := 25 * time.Millisecond

	servers := make([]*Server, 3)
	for i := 0; i < 3; i++ {
		id := raft.NodeID(i + 1)
		srv := NewServer(id, nil, &raft.PersistentState{CurrentTerm: 0, VotedFor: -1}, nil)
		srv.SetHeartbeatInterval(heartbeatInterval)
		srv.SetRPCTimeout(60 * time.Millisecond)
		if srv.GetElectionTimer() != nil {
			srv.GetElectionTimer().SetRange(minTimeout, maxTimeout)
			srv.ResetElectionTimeout()
		}
		if err := srv.Start("127.0.0.1:0"); err != nil {
			t.Fatalf("failed to start server %d: %v", id, err)
		}
		defer srv.Close()
		servers[i] = srv
	}

	// Full mesh peer setup
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

	for _, srv := range servers {
		if err := srv.ConnectPeers(); err != nil {
			t.Fatalf("node %d failed to connect peers: %v", srv.ID, err)
		}
	}

	// Start event loops on all servers
	for _, srv := range servers {
		srv.StartEventLoop()
	}

	leaderNode := servers[0]

	// Trigger election on Node 1 to establish initial leadership
	leaderNode.StartElection()

	// Wait for Node 1 to become leader
	electionDeadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(electionDeadline) {
		if leaderNode.GetState() == raft.Leader {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}

	if leaderNode.GetState() != raft.Leader {
		t.Fatalf("expected Node 1 to become initial Leader, got %s", leaderNode.GetState())
	}
	initialTerm := leaderNode.Status().Term
	if initialTerm < 1 {
		t.Fatalf("expected initial leader term >= 1, got %d", initialTerm)
	}

	// Allow heartbeats to stabilize leadership across followers
	time.Sleep(80 * time.Millisecond)

	// Verify both followers are in Follower state
	if servers[1].GetState() != raft.Follower {
		t.Fatalf("expected node 2 to be Follower, got %s", servers[1].GetState())
	}
	if servers[2].GetState() != raft.Follower {
		t.Fatalf("expected node 3 to be Follower, got %s", servers[2].GetState())
	}

	// 3. Kill the current leader process abruptly mid-test
	killTime := time.Now()
	leaderNode.Kill()

	if !leaderNode.IsKilled() {
		t.Fatalf("expected leaderNode.IsKilled() == true after Kill()")
	}

	// 4. Poll remaining nodes (servers[1] and servers[2]) for new leader election
	reElectionDeadline := time.Now().Add(3 * time.Second)
	var newLeader *Server
	for time.Now().Before(reElectionDeadline) {
		if servers[1].GetState() == raft.Leader {
			newLeader = servers[1]
			break
		}
		if servers[2].GetState() == raft.Leader {
			newLeader = servers[2]
			break
		}
		time.Sleep(5 * time.Millisecond)
	}

	timeToNewLeader := time.Since(killTime)

	if newLeader == nil {
		t.Fatalf("remaining nodes failed to elect a new leader within %v (node2 state: %s, node3 state: %s)",
			time.Since(killTime), servers[1].GetState(), servers[2].GetState())
	}

	newTerm := newLeader.Status().Term

	// 5. Measure and log time-to-new-leader
	t.Logf("================================================================================")
	t.Logf("ELECTION FAILOVER MEASUREMENT:")
	t.Logf("Leader Node %d killed at: %v", leaderNode.ID, killTime.Format("15:04:05.000"))
	t.Logf("New Leader elected: Node %d at term %d", newLeader.ID, newTerm)
	t.Logf("Time to elect new leader: %v (%.2f ms)", timeToNewLeader, float64(timeToNewLeader.Microseconds())/1000.0)
	t.Logf("Configured Election Timeout Range: [%v, %v]", minTimeout, maxTimeout)
	t.Logf("Configured Heartbeat Interval: %v", heartbeatInterval)
	t.Logf("Ratio to Min Election Timeout: %.2fx", float64(timeToNewLeader)/float64(minTimeout))
	t.Logf("Ratio to Max Election Timeout: %.2fx", float64(timeToNewLeader)/float64(maxTimeout))
	t.Logf("================================================================================")

	// Assertions
	if newLeader.ID == leaderNode.ID {
		t.Fatalf("new leader cannot be the killed node")
	}
	if newTerm <= initialTerm {
		t.Fatalf("expected new leader term > %d, got %d", initialTerm, newTerm)
	}

	// Wait briefly for new leader's heartbeats to suppress the other follower
	time.Sleep(60 * time.Millisecond)

	var otherNode *Server
	if newLeader.ID == servers[1].ID {
		otherNode = servers[2]
	} else {
		otherNode = servers[1]
	}

	if otherNode.GetState() != raft.Follower {
		t.Fatalf("expected remaining node %d to be Follower, got %s", otherNode.ID, otherNode.GetState())
	}

	// Verify time-to-new-leader bounds (~1-3x election timeout)
	if timeToNewLeader < minTimeout/2 {
		t.Fatalf("time-to-new-leader %v unexpectedly faster than minTimeout/2 (%v)", timeToNewLeader, minTimeout/2)
	}
	maxAllowed := 3*maxTimeout + 500*time.Millisecond
	if timeToNewLeader > maxAllowed {
		t.Fatalf("time-to-new-leader %v exceeded upper bound %v (~3x timeout)", timeToNewLeader, maxAllowed)
	}
}

func TestCluster_PauseLeader_RemainingNodesElectNewLeader(t *testing.T) {
	// Raft §5.2: Mid-test goroutine pause (simulating SIGSTOP / freeze).
	// 1. Setup a 3-node live cluster with full mesh connectivity and active event loops.
	// 2. Elect Node 1 as initial leader.
	// 3. Pause Node 1's event loop mid-test.
	// 4. Verify remaining nodes detect timeout and elect a new leader.
	// 5. Resume Node 1; verify Node 1 steps down to Follower upon seeing the higher term.

	minTimeout := 120 * time.Millisecond
	maxTimeout := 240 * time.Millisecond
	heartbeatInterval := 25 * time.Millisecond

	servers := make([]*Server, 3)
	for i := 0; i < 3; i++ {
		id := raft.NodeID(i + 1)
		srv := NewServer(id, nil, &raft.PersistentState{CurrentTerm: 0, VotedFor: -1}, nil)
		srv.SetHeartbeatInterval(heartbeatInterval)
		srv.SetRPCTimeout(60 * time.Millisecond)
		if srv.GetElectionTimer() != nil {
			srv.GetElectionTimer().SetRange(minTimeout, maxTimeout)
			srv.ResetElectionTimeout()
		}
		if err := srv.Start("127.0.0.1:0"); err != nil {
			t.Fatalf("failed to start server %d: %v", id, err)
		}
		defer srv.Close()
		servers[i] = srv
	}

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

	for _, srv := range servers {
		if err := srv.ConnectPeers(); err != nil {
			t.Fatalf("node %d failed to connect peers: %v", srv.ID, err)
		}
	}

	for _, srv := range servers {
		srv.StartEventLoop()
	}

	leaderNode := servers[0]
	leaderNode.StartElection()

	electionDeadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(electionDeadline) {
		if leaderNode.GetState() == raft.Leader {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}

	if leaderNode.GetState() != raft.Leader {
		t.Fatalf("expected Node 1 to become initial Leader, got %s", leaderNode.GetState())
	}
	initialTerm := leaderNode.Status().Term

	time.Sleep(80 * time.Millisecond)

	// Pause leader goroutine
	pauseTime := time.Now()
	leaderNode.Pause()
	if !leaderNode.IsPaused() {
		t.Fatalf("expected leaderNode.IsPaused() == true")
	}

	// Poll remaining nodes for new leader
	reElectionDeadline := time.Now().Add(3 * time.Second)
	var newLeader *Server
	for time.Now().Before(reElectionDeadline) {
		if servers[1].GetState() == raft.Leader {
			newLeader = servers[1]
			break
		}
		if servers[2].GetState() == raft.Leader {
			newLeader = servers[2]
			break
		}
		time.Sleep(5 * time.Millisecond)
	}

	timeToNewLeader := time.Since(pauseTime)

	if newLeader == nil {
		t.Fatalf("remaining nodes failed to elect new leader while leader was paused")
	}

	t.Logf("Leader paused: new leader %d elected in %v (term %d > initial %d)",
		newLeader.ID, timeToNewLeader, newLeader.Status().Term, initialTerm)

	if newLeader.Status().Term <= initialTerm {
		t.Fatalf("expected new leader term > %d, got %d", initialTerm, newLeader.Status().Term)
	}

	// Resume old leader
	leaderNode.Resume()
	if leaderNode.IsPaused() {
		t.Fatalf("expected leaderNode.IsPaused() == false after Resume()")
	}

	// Wait for new leader's heartbeat to reach resumed Node 1
	stepDownDeadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(stepDownDeadline) {
		if leaderNode.GetState() == raft.Follower {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}

	if leaderNode.GetState() != raft.Follower {
		t.Fatalf("expected resumed old leader to step down to Follower on seeing higher term, got %s", leaderNode.GetState())
	}
}











