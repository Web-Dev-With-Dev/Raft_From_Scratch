package rpc

import (
	"context"
	"errors"
	"net"
	"testing"
	"time"

	raftpb "raft/proto"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestProtoConversions_RequestVote(t *testing.T) {
	args := &RequestVoteArgs{
		Term:         3,
		CandidateID:  1,
		LastLogIndex: 10,
		LastLogTerm:  2,
	}

	pbArgs := ToProtoRequestVoteArgs(args)
	if pbArgs.Term != 3 || pbArgs.CandidateId != 1 || pbArgs.LastLogIndex != 10 || pbArgs.LastLogTerm != 2 {
		t.Fatalf("unexpected pbArgs: %+v", pbArgs)
	}

	backArgs := FromProtoRequestVoteArgs(pbArgs)
	if *backArgs != *args {
		t.Fatalf("expected %+v, got %+v", args, backArgs)
	}

	reply := &RequestVoteReply{
		Term:        4,
		VoteGranted: true,
	}
	pbReply := ToProtoRequestVoteReply(reply)
	if pbReply.Term != 4 || !pbReply.VoteGranted {
		t.Fatalf("unexpected pbReply: %+v", pbReply)
	}

	backReply := FromProtoRequestVoteReply(pbReply)
	if *backReply != *reply {
		t.Fatalf("expected %+v, got %+v", reply, backReply)
	}
}

func TestProtoConversions_AppendEntries(t *testing.T) {
	args := &AppendEntriesArgs{
		Term:         5,
		LeaderID:     2,
		PrevLogIndex: 8,
		PrevLogTerm:  4,
		Entries: []LogEntry{
			{Term: 5, Index: 9, Command: "set a=1"},
			{Term: 5, Index: 10, Command: "set b=2"},
		},
		LeaderCommit: 8,
	}

	pbArgs, err := ToProtoAppendEntriesArgs(args)
	if err != nil {
		t.Fatalf("ToProtoAppendEntriesArgs failed: %v", err)
	}

	if pbArgs.Term != 5 || pbArgs.LeaderId != 2 || len(pbArgs.Entries) != 2 {
		t.Fatalf("unexpected pbArgs: %+v", pbArgs)
	}

	backArgs := FromProtoAppendEntriesArgs(pbArgs)
	if backArgs.Term != args.Term || backArgs.LeaderID != args.LeaderID || len(backArgs.Entries) != 2 {
		t.Fatalf("unexpected backArgs: %+v", backArgs)
	}
	if backArgs.Entries[0].Command != "set a=1" || backArgs.Entries[1].Command != "set b=2" {
		t.Fatalf("unexpected command in converted entries: %+v", backArgs.Entries)
	}

	reply := &AppendEntriesReply{
		Term:          5,
		Success:       false,
		ConflictIndex: 7,
		ConflictTerm:  3,
	}
	pbReply := ToProtoAppendEntriesReply(reply)
	backReply := FromProtoAppendEntriesReply(pbReply)
	if *backReply != *reply {
		t.Fatalf("expected %+v, got %+v", reply, backReply)
	}
}

func TestClientPool_SendRequestVoteAndAppendEntries(t *testing.T) {
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

	peer := Peer{
		ID:      2,
		Address: lis.Addr().String(),
	}

	// Test SendRequestVote via pool
	rvReply, err := pool.SendRequestVote(context.Background(), peer, &RequestVoteArgs{
		Term:         1,
		CandidateID:  1,
		LastLogIndex: 0,
		LastLogTerm:  0,
	})
	if err != nil {
		t.Fatalf("SendRequestVote failed: %v", err)
	}
	if !rvReply.VoteGranted || rvReply.Term != 1 {
		t.Fatalf("unexpected rvReply: %+v", rvReply)
	}

	// Test sendRequestVote convenience method
	rvReply2, err := pool.sendRequestVote(peer, &RequestVoteArgs{
		Term:         2,
		CandidateID:  1,
		LastLogIndex: 1,
		LastLogTerm:  1,
	})
	if err != nil {
		t.Fatalf("sendRequestVote failed: %v", err)
	}
	if !rvReply2.VoteGranted || rvReply2.Term != 2 {
		t.Fatalf("unexpected rvReply2: %+v", rvReply2)
	}

	// Test SendAppendEntries via pool
	aeReply, err := pool.SendAppendEntries(context.Background(), peer, &AppendEntriesArgs{
		Term:         2,
		LeaderID:     1,
		PrevLogIndex: 1,
		PrevLogTerm:  1,
		Entries:      nil, // Heartbeat
		LeaderCommit: 1,
	})
	if err != nil {
		t.Fatalf("SendAppendEntries failed: %v", err)
	}
	if !aeReply.Success || aeReply.Term != 2 {
		t.Fatalf("unexpected aeReply: %+v", aeReply)
	}

	// Test sendAppendEntries convenience method
	aeReply2, err := pool.sendAppendEntries(peer, &AppendEntriesArgs{
		Term:         3,
		LeaderID:     1,
		PrevLogIndex: 1,
		PrevLogTerm:  1,
	})
	if err != nil {
		t.Fatalf("sendAppendEntries failed: %v", err)
	}
	if !aeReply2.Success || aeReply2.Term != 3 {
		t.Fatalf("unexpected aeReply2: %+v", aeReply2)
	}
}

func TestClientPool_SendRPC_Errors(t *testing.T) {
	pool := NewClientPool()
	defer pool.Close()

	// Peer not connected and empty address
	_, err := pool.SendRequestVote(context.Background(), Peer{ID: 99, Address: ""}, &RequestVoteArgs{})
	if err == nil {
		t.Fatalf("expected error for unconnected peer with empty address")
	}

	_, err = pool.SendAppendEntries(context.Background(), Peer{ID: 99, Address: ""}, &AppendEntriesArgs{})
	if err == nil {
		t.Fatalf("expected error for unconnected peer with empty address")
	}

	// Nil args
	_, err = pool.SendRequestVote(context.Background(), Peer{ID: 99, Address: "127.0.0.1:9999"}, nil)
	if err == nil {
		t.Fatalf("expected error for nil args")
	}
}

// hangingRaftServer simulates a dead or unresponsive peer that never responds in time.
type hangingRaftServer struct {
	raftpb.UnimplementedRaftServiceServer
	delay time.Duration
}

func (h *hangingRaftServer) RequestVote(ctx context.Context, req *raftpb.RequestVoteArgs) (*raftpb.RequestVoteReply, error) {
	time.Sleep(h.delay)
	return &raftpb.RequestVoteReply{Term: req.Term, VoteGranted: false}, nil
}

func (h *hangingRaftServer) AppendEntries(ctx context.Context, req *raftpb.AppendEntriesArgs) (*raftpb.AppendEntriesReply, error) {
	time.Sleep(h.delay)
	return &raftpb.AppendEntriesReply{Term: req.Term, Success: false}, nil
}

func TestRPCTimeout_DeadPeerDoesNotBlockIndefinitely(t *testing.T) {
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("failed to listen: %v", err)
	}
	defer lis.Close()

	grpcServer := grpc.NewServer()
	// Server handler delays by 500ms
	raftpb.RegisterRaftServiceServer(grpcServer, &hangingRaftServer{delay: 500 * time.Millisecond})
	go func() {
		_ = grpcServer.Serve(lis)
	}()
	defer grpcServer.Stop()

	pool := NewClientPool()
	defer pool.Close()

	// Configure pool timeout to 100ms
	pool.SetRPCTimeout(100 * time.Millisecond)

	peer := Peer{ID: 2, Address: lis.Addr().String()}

	// 1. SendRequestVote without an explicit deadline on context:
	// MUST unblock within ~100-200ms rather than hanging for 500ms!
	start := time.Now()
	_, err = pool.SendRequestVote(context.Background(), peer, &RequestVoteArgs{Term: 1})
	elapsed := time.Since(start)

	if err == nil {
		t.Fatalf("expected timeout error for hanging peer, got nil")
	}

	if !IsTimeout(err) {
		t.Fatalf("expected IsTimeout(err) to be true, got error: %v", err)
	}

	if elapsed >= 450*time.Millisecond {
		t.Fatalf("call took %v, expected it to timeout around 100ms and not block indefinitely", elapsed)
	}

	// 2. sendAppendEntries convenience method:
	start = time.Now()
	_, err = pool.sendAppendEntries(peer, &AppendEntriesArgs{Term: 1})
	elapsed = time.Since(start)

	if err == nil {
		t.Fatalf("expected timeout error for hanging peer on AppendEntries")
	}

	if !IsTimeout(err) {
		t.Fatalf("expected IsTimeout(err) to be true on AppendEntries, got: %v", err)
	}

	if elapsed >= 450*time.Millisecond {
		t.Fatalf("call took %v, expected it to timeout around 100ms and not block indefinitely", elapsed)
	}
}

func TestIsTimeout_Helper(t *testing.T) {
	if IsTimeout(nil) {
		t.Fatalf("expected IsTimeout(nil) to be false")
	}

	if !IsTimeout(context.DeadlineExceeded) {
		t.Fatalf("expected IsTimeout(context.DeadlineExceeded) to be true")
	}

	grpcTimeoutErr := status.Error(codes.DeadlineExceeded, "context deadline exceeded")
	if !IsTimeout(grpcTimeoutErr) {
		t.Fatalf("expected IsTimeout(grpcTimeoutErr) to be true")
	}

	otherErr := errors.New("connection reset by peer")
	if IsTimeout(otherErr) {
		t.Fatalf("expected IsTimeout(otherErr) to be false")
	}
}

