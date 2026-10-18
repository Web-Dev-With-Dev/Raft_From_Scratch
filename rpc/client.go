package rpc

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	raftpb "raft/proto"
	"raft/raft"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// DefaultRPCTimeout is the default timeout (150ms) applied to Raft RPC calls
// to prevent dead or partitioned peers from blocking operations indefinitely.
const DefaultRPCTimeout = 150 * time.Millisecond

// withRPCTimeout wraps ctx with the given timeout if ctx does not already have a deadline.
func withRPCTimeout(ctx context.Context, timeout time.Duration) (context.Context, context.CancelFunc) {
	if timeout <= 0 {
		timeout = DefaultRPCTimeout
	}
	if _, hasDeadline := ctx.Deadline(); !hasDeadline {
		return context.WithTimeout(ctx, timeout)
	}
	return ctx, func() {}
}

// IsTimeout returns true if the error was caused by a deadline expiration or timeout.
func IsTimeout(err error) bool {
	if err == nil {
		return false
	}
	if errors.Is(err, context.DeadlineExceeded) {
		return true
	}
	st, ok := status.FromError(err)
	return ok && st.Code() == codes.DeadlineExceeded
}

// ToProtoRequestVoteArgs converts a domain RequestVoteArgs to a protobuf RequestVoteArgs.
func ToProtoRequestVoteArgs(args *RequestVoteArgs) *raftpb.RequestVoteArgs {
	if args == nil {
		return nil
	}
	return &raftpb.RequestVoteArgs{
		Term:         uint64(args.Term),
		CandidateId:  int64(args.CandidateID),
		LastLogIndex: uint64(args.LastLogIndex),
		LastLogTerm:  uint64(args.LastLogTerm),
	}
}

// FromProtoRequestVoteArgs converts a protobuf RequestVoteArgs to a domain RequestVoteArgs.
func FromProtoRequestVoteArgs(pb *raftpb.RequestVoteArgs) *RequestVoteArgs {
	if pb == nil {
		return nil
	}
	return &RequestVoteArgs{
		Term:         raft.Term(pb.Term),
		CandidateID:  raft.NodeID(pb.CandidateId),
		LastLogIndex: raft.LogIndex(pb.LastLogIndex),
		LastLogTerm:  raft.Term(pb.LastLogTerm),
	}
}

// ToProtoRequestVoteReply converts a domain RequestVoteReply to a protobuf RequestVoteReply.
func ToProtoRequestVoteReply(reply *RequestVoteReply) *raftpb.RequestVoteReply {
	if reply == nil {
		return nil
	}
	return &raftpb.RequestVoteReply{
		Term:        uint64(reply.Term),
		VoteGranted: reply.VoteGranted,
	}
}

// FromProtoRequestVoteReply converts a protobuf RequestVoteReply to a domain RequestVoteReply.
func FromProtoRequestVoteReply(pb *raftpb.RequestVoteReply) *RequestVoteReply {
	if pb == nil {
		return nil
	}
	return &RequestVoteReply{
		Term:        raft.Term(pb.Term),
		VoteGranted: pb.VoteGranted,
	}
}

// encodeCommand serializes a log entry command into bytes.
func encodeCommand(cmd any) ([]byte, error) {
	if cmd == nil {
		return nil, nil
	}
	switch v := cmd.(type) {
	case []byte:
		return v, nil
	case string:
		return []byte(v), nil
	default:
		return json.Marshal(v)
	}
}

// decodeCommand deserializes command bytes. If valid JSON, it unmarshals into an object,
// otherwise returns the raw string.
func decodeCommand(data []byte) any {
	if len(data) == 0 {
		return nil
	}
	var val any
	if err := json.Unmarshal(data, &val); err == nil {
		return val
	}
	return string(data)
}

// ToProtoAppendEntriesArgs converts a domain AppendEntriesArgs to a protobuf AppendEntriesArgs.
func ToProtoAppendEntriesArgs(args *AppendEntriesArgs) (*raftpb.AppendEntriesArgs, error) {
	if args == nil {
		return nil, nil
	}

	entries := make([]*raftpb.LogEntry, len(args.Entries))
	for i, entry := range args.Entries {
		cmdBytes, err := encodeCommand(entry.Command)
		if err != nil {
			return nil, fmt.Errorf("failed to encode log entry command at index %d: %w", i, err)
		}
		entries[i] = &raftpb.LogEntry{
			Term:    uint64(entry.Term),
			Index:   uint64(entry.Index),
			Command: cmdBytes,
		}
	}

	return &raftpb.AppendEntriesArgs{
		Term:         uint64(args.Term),
		LeaderId:     int64(args.LeaderID),
		PrevLogIndex: uint64(args.PrevLogIndex),
		PrevLogTerm:  uint64(args.PrevLogTerm),
		Entries:      entries,
		LeaderCommit: uint64(args.LeaderCommit),
	}, nil
}

// FromProtoAppendEntriesArgs converts a protobuf AppendEntriesArgs to a domain AppendEntriesArgs.
func FromProtoAppendEntriesArgs(pb *raftpb.AppendEntriesArgs) *AppendEntriesArgs {
	if pb == nil {
		return nil
	}

	entries := make([]raft.LogEntry, len(pb.Entries))
	for i, entry := range pb.Entries {
		entries[i] = raft.LogEntry{
			Term:    raft.Term(entry.Term),
			Index:   raft.LogIndex(entry.Index),
			Command: decodeCommand(entry.Command),
		}
	}

	return &AppendEntriesArgs{
		Term:         raft.Term(pb.Term),
		LeaderID:     raft.NodeID(pb.LeaderId),
		PrevLogIndex: raft.LogIndex(pb.PrevLogIndex),
		PrevLogTerm:  raft.Term(pb.PrevLogTerm),
		Entries:      entries,
		LeaderCommit: raft.LogIndex(pb.LeaderCommit),
	}
}

// ToProtoAppendEntriesReply converts a domain AppendEntriesReply to a protobuf AppendEntriesReply.
func ToProtoAppendEntriesReply(reply *AppendEntriesReply) *raftpb.AppendEntriesReply {
	if reply == nil {
		return nil
	}
	return &raftpb.AppendEntriesReply{
		Term:          uint64(reply.Term),
		Success:       reply.Success,
		ConflictIndex: uint64(reply.ConflictIndex),
		ConflictTerm:  uint64(reply.ConflictTerm),
	}
}

// FromProtoAppendEntriesReply converts a protobuf AppendEntriesReply to a domain AppendEntriesReply.
func FromProtoAppendEntriesReply(pb *raftpb.AppendEntriesReply) *AppendEntriesReply {
	if pb == nil {
		return nil
	}
	return &AppendEntriesReply{
		Term:          raft.Term(pb.Term),
		Success:       pb.Success,
		ConflictIndex: raft.LogIndex(pb.ConflictIndex),
		ConflictTerm:  raft.Term(pb.ConflictTerm),
	}
}

// SendRequestVote sends a RequestVote RPC to the specified client stub with domain arguments.
// If ctx does not have a deadline, DefaultRPCTimeout (150ms) is automatically enforced.
func SendRequestVote(ctx context.Context, client raftpb.RaftServiceClient, args *RequestVoteArgs) (*RequestVoteReply, error) {
	if client == nil {
		return nil, fmt.Errorf("client is nil")
	}
	if args == nil {
		return nil, fmt.Errorf("args is nil")
	}

	callCtx, cancel := withRPCTimeout(ctx, DefaultRPCTimeout)
	defer cancel()

	pbArgs := ToProtoRequestVoteArgs(args)
	pbReply, err := client.RequestVote(callCtx, pbArgs)
	if err != nil {
		return nil, err
	}

	return FromProtoRequestVoteReply(pbReply), nil
}

// SendAppendEntries sends an AppendEntries RPC to the specified client stub with domain arguments.
// If ctx does not have a deadline, DefaultRPCTimeout (150ms) is automatically enforced.
func SendAppendEntries(ctx context.Context, client raftpb.RaftServiceClient, args *AppendEntriesArgs) (*AppendEntriesReply, error) {
	if client == nil {
		return nil, fmt.Errorf("client is nil")
	}
	if args == nil {
		return nil, fmt.Errorf("args is nil")
	}

	callCtx, cancel := withRPCTimeout(ctx, DefaultRPCTimeout)
	defer cancel()

	pbArgs, err := ToProtoAppendEntriesArgs(args)
	if err != nil {
		return nil, err
	}

	pbReply, err := client.AppendEntries(callCtx, pbArgs)
	if err != nil {
		return nil, err
	}

	return FromProtoAppendEntriesReply(pbReply), nil
}

// SendRequestVote sends a RequestVote RPC to the specified peer using the client pool.
// If ctx has no deadline, the pool's RPCTimeout is automatically enforced.
func (p *ClientPool) SendRequestVote(ctx context.Context, peer Peer, args *RequestVoteArgs) (*RequestVoteReply, error) {
	if p == nil {
		return nil, fmt.Errorf("client pool is nil")
	}

	callCtx, cancel := withRPCTimeout(ctx, p.RPCTimeout())
	defer cancel()

	client, exists := p.Get(peer.ID)
	if !exists {
		if peer.Address == "" {
			return nil, fmt.Errorf("peer %d not connected and no address provided", peer.ID)
		}
		var err error
		client, err = p.Connect(peer.ID, peer.Address)
		if err != nil {
			return nil, fmt.Errorf("failed to connect to peer %d at %s: %w", peer.ID, peer.Address, err)
		}
	}

	return SendRequestVote(callCtx, client, args)
}

// SendAppendEntries sends an AppendEntries RPC to the specified peer using the client pool.
// If ctx has no deadline, the pool's RPCTimeout is automatically enforced.
func (p *ClientPool) SendAppendEntries(ctx context.Context, peer Peer, args *AppendEntriesArgs) (*AppendEntriesReply, error) {
	if p == nil {
		return nil, fmt.Errorf("client pool is nil")
	}

	callCtx, cancel := withRPCTimeout(ctx, p.RPCTimeout())
	defer cancel()

	client, exists := p.Get(peer.ID)
	if !exists {
		if peer.Address == "" {
			return nil, fmt.Errorf("peer %d not connected and no address provided", peer.ID)
		}
		var err error
		client, err = p.Connect(peer.ID, peer.Address)
		if err != nil {
			return nil, fmt.Errorf("failed to connect to peer %d at %s: %w", peer.ID, peer.Address, err)
		}
	}

	return SendAppendEntries(callCtx, client, args)
}

// sendRequestVote is a convenience stub matching the Raft paper convention, using the pool's RPCTimeout.
func (p *ClientPool) sendRequestVote(peer Peer, args *RequestVoteArgs) (*RequestVoteReply, error) {
	ctx, cancel := context.WithTimeout(context.Background(), p.RPCTimeout())
	defer cancel()
	return p.SendRequestVote(ctx, peer, args)
}

// sendAppendEntries is a convenience stub matching the Raft paper convention, using the pool's RPCTimeout.
func (p *ClientPool) sendAppendEntries(peer Peer, args *AppendEntriesArgs) (*AppendEntriesReply, error) {
	ctx, cancel := context.WithTimeout(context.Background(), p.RPCTimeout())
	defer cancel()
	return p.SendAppendEntries(ctx, peer, args)
}
