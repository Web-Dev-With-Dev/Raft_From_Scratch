package server

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"net"
	"os"
	"sync"
	"time"

	"raft/logger"
	raftpb "raft/proto"
	"raft/raft"
	"raft/rpc"

	"google.golang.org/grpc"
	"google.golang.org/grpc/keepalive"
)

// Type aliases for convenience
type Peer = raft.Peer
type ClientPool = rpc.ClientPool
type RPCClientPool = rpc.ClientPool
type LogOptions = logger.Options
type ElectionTimer = raft.ElectionTimer

// NewNodeLogger creates a structured logger with [Node <ID>] prefix on every line.
func NewNodeLogger(id raft.NodeID, w io.Writer, opts *LogOptions) *slog.Logger {
	return logger.NewNodeLogger(id, w, opts)
}

// Server represents a Raft cluster node hosting the consensus state machine and RPC services.
type Server struct {
	raftpb.UnimplementedRaftServiceServer

	mu sync.RWMutex

	// ID is the unique identifier for this Raft node.
	ID raft.NodeID `json:"id"`

	// Peers is the list of peer nodes in the Raft cluster.
	Peers []Peer `json:"peers"`

	// PersistentState holds the persistent consensus state (currentTerm, votedFor, log entries).
	PersistentState *raft.PersistentState `json:"persistent_state"`

	// State represents the current consensus role (Follower, Candidate, Leader).
	State raft.RaftState `json:"state"`

	// ElectionTimer manages the randomized election timeout backed by time.Timer.
	ElectionTimer *raft.ElectionTimer `json:"-"`

	// RPCClientPool manages gRPC client connections to peers.
	RPCClientPool *rpc.ClientPool `json:"-"`

	// Logger is the structured logger for this node with [Node <ID>] prefix on every line.
	Logger *slog.Logger `json:"-"`

	lastHeartbeat time.Time

	grpcServer *grpc.Server
	listener   net.Listener
}

// NewServer creates and initializes a new Raft Server.
// If persistentState is nil, a default initialized PersistentState is provided.
// If rpcClientPool is nil, a new empty ClientPool is initialized.
func NewServer(
	id raft.NodeID,
	peers []Peer,
	persistentState *raft.PersistentState,
	rpcClientPool *rpc.ClientPool,
) *Server {
	if persistentState == nil {
		persistentState = &raft.PersistentState{
			CurrentTerm: 0,
			VotedFor:    -1,
			Log:         make([]raft.LogEntry, 0),
		}
	}

	nodeLogger := logger.NewNodeLogger(id, os.Stderr, nil)

	if rpcClientPool == nil {
		rpcClientPool = rpc.NewClientPool()
	}
	rpcClientPool.SetLogger(nodeLogger)

	return &Server{
		ID:              id,
		Peers:           peers,
		PersistentState: persistentState,
		State:           raft.Follower,
		ElectionTimer:   raft.NewElectionTimer(raft.DefaultMinElectionTimeout, raft.DefaultMaxElectionTimeout),
		RPCClientPool:   rpcClientPool,
		Logger:          nodeLogger,
		lastHeartbeat:   time.Now(),
	}
}

// GetID returns the server node ID.
func (s *Server) GetID() raft.NodeID {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.ID
}

// GetState returns the current consensus state (Follower, Candidate, Leader).
func (s *Server) GetState() raft.RaftState {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.State
}

// SetState updates the server's consensus state.
func (s *Server) SetState(state raft.RaftState) {
	s.mu.Lock()
	defer s.mu.Unlock()
	old := s.State
	s.State = state
	s.GetLogger().Info("transitioned consensus state", "from", old.String(), "to", state.String())
}

// GetElectionTimer returns the server's election timer.
func (s *Server) GetElectionTimer() *raft.ElectionTimer {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.ElectionTimer
}

// ResetElectionTimeout resets the election timer with a newly selected
// randomized duration in [150ms, 300ms]. Returns the new timeout duration.
func (s *Server) ResetElectionTimeout() time.Duration {
	s.mu.RLock()
	timer := s.ElectionTimer
	s.mu.RUnlock()

	if timer == nil {
		return 0
	}
	d := timer.Reset()
	s.GetLogger().Info("reset election timeout", "timeout_ms", d.Milliseconds())
	return d
}

// HandleHeartbeat processes a heartbeat event from a leader.
// If the heartbeat is valid (term >= currentTerm), it resets the election timer,
// updates the last heartbeat timestamp, reverts Candidates to Followers,
// and returns true along with the newly assigned election timeout.
// If the heartbeat term is stale (term < currentTerm), the heartbeat is rejected
// without resetting the timer, returning false and 0.
func (s *Server) HandleHeartbeat(leaderID raft.NodeID, term raft.Term) (bool, time.Duration) {
	s.mu.Lock()
	defer s.mu.Unlock()

	var currentTerm raft.Term
	if s.PersistentState != nil {
		currentTerm = s.PersistentState.CurrentTerm
	}

	if term < currentTerm {
		s.GetLogger().Warn("rejected heartbeat from stale leader",
			"leader_id", int(leaderID),
			"leader_term", uint64(term),
			"current_term", uint64(currentTerm),
		)
		return false, 0
	}

	if s.State == raft.Candidate {
		s.State = raft.Follower
		s.GetLogger().Info("reverted from Candidate to Follower upon valid heartbeat",
			"leader_id", int(leaderID),
			"term", uint64(term),
		)
	}

	s.lastHeartbeat = time.Now()
	var newTimeout time.Duration
	if s.ElectionTimer != nil {
		newTimeout = s.ElectionTimer.Reset()
		s.GetLogger().Info("reset election timer on valid heartbeat",
			"leader_id", int(leaderID),
			"term", uint64(term),
			"timeout_ms", newTimeout.Milliseconds(),
		)
	}

	return true, newTimeout
}

// LastHeartbeatTime returns the timestamp of the last valid heartbeat received by the server.
func (s *Server) LastHeartbeatTime() time.Time {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.lastHeartbeat
}

// GetPeers returns a copy of the peer list.
func (s *Server) GetPeers() []Peer {
	s.mu.RLock()
	defer s.mu.RUnlock()

	peersCopy := make([]Peer, len(s.Peers))
	copy(peersCopy, s.Peers)
	return peersCopy
}

// PeerIDs returns a slice containing the IDs of all configured peers.
func (s *Server) PeerIDs() []raft.NodeID {
	s.mu.RLock()
	defer s.mu.RUnlock()

	ids := make([]raft.NodeID, 0, len(s.Peers))
	for _, p := range s.Peers {
		ids = append(ids, p.ID)
	}
	return ids
}

// GetPeer retrieves peer information for a specific node ID.
func (s *Server) GetPeer(id raft.NodeID) (Peer, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	for _, p := range s.Peers {
		if p.ID == id {
			return p, true
		}
	}
	return Peer{}, false
}

// AddPeer appends a new peer to the peer list if it does not already exist.
func (s *Server) AddPeer(peer Peer) {
	s.mu.Lock()
	defer s.mu.Unlock()

	for i, p := range s.Peers {
		if p.ID == peer.ID {
			s.Peers[i] = peer
			s.GetLogger().Info("updated peer", "peer_id", int(peer.ID), "address", peer.Address)
			return
		}
	}
	s.Peers = append(s.Peers, peer)
	s.GetLogger().Info("added peer", "peer_id", int(peer.ID), "address", peer.Address)
}

// SetLogger configures the structured logger for the server and its client pool.
func (s *Server) SetLogger(l *slog.Logger) {
	if l == nil {
		l = logger.NewNodeLogger(s.ID, os.Stderr, nil)
	}
	s.Logger = l
	if s.RPCClientPool != nil {
		s.RPCClientPool.SetLogger(l)
	}
}

// GetLogger returns the server's structured logger, defaulting to a node-prefixed logger if nil.
func (s *Server) GetLogger() *slog.Logger {
	if s == nil || s.Logger == nil {
		return slog.Default()
	}
	return s.Logger
}

// ClientPool returns the server's RPC client pool.
func (s *Server) ClientPool() *rpc.ClientPool {
	return s.RPCClientPool
}

// ConnectPeers dials and registers all peers into the RPC client pool.
func (s *Server) ConnectPeers() error {
	s.mu.RLock()
	peers := make([]Peer, len(s.Peers))
	copy(peers, s.Peers)
	s.mu.RUnlock()

	s.GetLogger().Info("connecting to cluster peers", "peer_count", len(peers))
	for _, peer := range peers {
		if peer.ID == s.ID {
			continue // Skip dialing self
		}
		if _, err := s.RPCClientPool.Connect(peer.ID, peer.Address); err != nil {
			s.GetLogger().Warn("failed to connect to peer", "peer_id", int(peer.ID), "address", peer.Address, "error", err)
			return err
		}
	}
	return nil
}

// ConnectPeersWithRetry dials and registers all peers into the RPC client pool with the specified retry policy.
func (s *Server) ConnectPeersWithRetry(ctx context.Context, policy rpc.RetryPolicy) error {
	s.mu.RLock()
	peers := make([]Peer, len(s.Peers))
	copy(peers, s.Peers)
	s.mu.RUnlock()

	s.GetLogger().Info("connecting to cluster peers with retry", "peer_count", len(peers))
	for _, peer := range peers {
		if peer.ID == s.ID {
			continue // Skip dialing self
		}
		if _, err := s.RPCClientPool.ConnectWithRetry(ctx, peer.ID, peer.Address, policy); err != nil {
			s.GetLogger().Warn("failed to connect to peer with retry", "peer_id", int(peer.ID), "address", peer.Address, "error", err)
			return err
		}
	}
	return nil
}

// SetRetryPolicy updates the dial retry policy on the server's RPCClientPool.
func (s *Server) SetRetryPolicy(policy rpc.RetryPolicy) {
	if s.RPCClientPool != nil {
		s.RPCClientPool.SetRetryPolicy(policy)
	}
}

// RegisterService registers the Server's RaftService handlers with the gRPC server.
func (s *Server) RegisterService(grpcServer grpc.ServiceRegistrar) {
	raftpb.RegisterRaftServiceServer(grpcServer, s)
}

// Start starts the gRPC server listening on the specified network address in the background.
// It registers the Raft service handlers and begins serving incoming RPC requests.
func (s *Server) Start(address string, opts ...grpc.ServerOption) error {
	s.GetLogger().Info("starting gRPC server", "address", address)
	lis, err := net.Listen("tcp", address)
	if err != nil {
		s.GetLogger().Error("failed to listen", "address", address, "error", err)
		return fmt.Errorf("failed to listen on %s: %w", address, err)
	}
	return s.Serve(lis, opts...)
}

// Serve starts the gRPC server on the provided listener in the background.
// It registers the Raft service handlers and begins serving incoming RPC requests.
func (s *Server) Serve(lis net.Listener, opts ...grpc.ServerOption) error {
	s.mu.Lock()
	if s.grpcServer != nil {
		s.mu.Unlock()
		_ = lis.Close()
		return fmt.Errorf("server is already running")
	}

	defaultOpts := []grpc.ServerOption{
		grpc.KeepaliveEnforcementPolicy(keepalive.EnforcementPolicy{
			MinTime:             5 * time.Second, // Allow client keepalive pings every 5s
			PermitWithoutStream: true,            // Allow pings even when idle with no active RPC streams
		}),
		grpc.KeepaliveParams(keepalive.ServerParameters{
			MaxConnectionIdle: 15 * time.Minute,
			Time:              15 * time.Second,
			Timeout:           5 * time.Second,
		}),
	}
	serverOpts := append(defaultOpts, opts...)

	grpcServer := grpc.NewServer(serverOpts...)
	s.RegisterService(grpcServer)

	s.grpcServer = grpcServer
	s.listener = lis
	addrStr := lis.Addr().String()
	s.mu.Unlock()

	s.GetLogger().Info("gRPC server listening", "address", addrStr)

	go func() {
		if err := grpcServer.Serve(lis); err != nil && err != grpc.ErrServerStopped {
			s.GetLogger().Error("gRPC server error during serve", "error", err)
		}
	}()

	return nil
}

// Stop immediately stops the gRPC server and closes the active listener.
func (s *Server) Stop() {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.ElectionTimer != nil {
		s.ElectionTimer.Stop()
	}

	if s.grpcServer != nil {
		s.grpcServer.Stop()
		s.grpcServer = nil
		s.GetLogger().Info("stopped gRPC server")
	}
	if s.listener != nil {
		_ = s.listener.Close()
		s.listener = nil
	}
}

// GracefulStop gracefully stops the gRPC server, waiting for pending RPCs to finish.
func (s *Server) GracefulStop() {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.ElectionTimer != nil {
		s.ElectionTimer.Stop()
	}

	if s.grpcServer != nil {
		s.grpcServer.GracefulStop()
		s.grpcServer = nil
		s.GetLogger().Info("gracefully stopped gRPC server")
	}
	if s.listener != nil {
		_ = s.listener.Close()
		s.listener = nil
	}
}

// Addr returns the network address the gRPC server is listening on, or nil if not running.
func (s *Server) Addr() net.Addr {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if s.listener != nil {
		return s.listener.Addr()
	}
	return nil
}

// IsRunning returns true if the gRPC server is actively listening and serving.
func (s *Server) IsRunning() bool {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.grpcServer != nil
}

// GRPCServer returns the underlying gRPC server instance, or nil if not running.
func (s *Server) GRPCServer() *grpc.Server {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.grpcServer
}

// RequestVote handles incoming RequestVote RPCs.
// When a vote is granted to a candidate, it resets the server's election timer (§5.2).
func (s *Server) RequestVote(ctx context.Context, req *raftpb.RequestVoteArgs) (*raftpb.RequestVoteReply, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.GetLogger().Info("received RequestVote",
		"candidate_id", req.CandidateId,
		"term", req.Term,
		"last_log_index", req.LastLogIndex,
		"last_log_term", req.LastLogTerm,
	)

	var currentTerm uint64
	if s.PersistentState != nil {
		currentTerm = uint64(s.PersistentState.CurrentTerm)
	}

	// 1. Reply false if term < currentTerm (§5.1)
	if req.Term < currentTerm {
		s.GetLogger().Warn("rejected RequestVote from candidate with stale term",
			"candidate_id", req.CandidateId,
			"req_term", req.Term,
			"current_term", currentTerm,
		)
		return &raftpb.RequestVoteReply{
			Term:        currentTerm,
			VoteGranted: false,
		}, nil
	}

	voteGranted := false

	// In Raft (§5.2):
	// A server resets its election timer when it grants a vote to a candidate.
	if voteGranted && s.ElectionTimer != nil {
		s.lastHeartbeat = time.Now()
		newTimeout := s.ElectionTimer.Reset()
		s.GetLogger().Info("reset election timer on granting vote",
			"candidate_id", req.CandidateId,
			"term", req.Term,
			"timeout_ms", newTimeout.Milliseconds(),
		)
	}

	return &raftpb.RequestVoteReply{
		Term:        currentTerm,
		VoteGranted: voteGranted,
	}, nil
}

// AppendEntries handles incoming AppendEntries RPCs (heartbeats and log replication).
// A valid heartbeat (req.Term >= currentTerm) resets the node's election timer (§5.2).
func (s *Server) AppendEntries(ctx context.Context, req *raftpb.AppendEntriesArgs) (*raftpb.AppendEntriesReply, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.GetLogger().Info("received AppendEntries",
		"leader_id", req.LeaderId,
		"term", req.Term,
		"prev_log_index", req.PrevLogIndex,
		"prev_log_term", req.PrevLogTerm,
		"entries_count", len(req.Entries),
		"leader_commit", req.LeaderCommit,
	)

	var currentTerm uint64
	if s.PersistentState != nil {
		currentTerm = uint64(s.PersistentState.CurrentTerm)
	}

	// 1. Reply false if term < currentTerm (§5.1)
	if req.Term < currentTerm {
		s.GetLogger().Warn("rejected AppendEntries from stale leader",
			"leader_id", req.LeaderId,
			"req_term", req.Term,
			"current_term", currentTerm,
		)
		return &raftpb.AppendEntriesReply{
			Term:    currentTerm,
			Success: false,
		}, nil
	}

	// 2. Valid heartbeat/RPC from current or new leader:
	// If candidate or leader with stale term, step down to follower (§5.2)
	if s.State == raft.Candidate {
		s.State = raft.Follower
		s.GetLogger().Info("reverted from Candidate to Follower upon valid AppendEntries",
			"leader_id", req.LeaderId,
			"term", req.Term,
		)
	}

	// 3. Reset election timer triggered by valid heartbeat/RPC received (§5.2)
	s.lastHeartbeat = time.Now()
	if s.ElectionTimer != nil {
		newTimeout := s.ElectionTimer.Reset()
		s.GetLogger().Info("reset election timer on valid heartbeat/AppendEntries",
			"leader_id", req.LeaderId,
			"term", req.Term,
			"timeout_ms", newTimeout.Milliseconds(),
		)
	}

	return &raftpb.AppendEntriesReply{
		Term:    currentTerm,
		Success: false, // Stub until log replication logic is added
	}, nil
}

// SendRequestVote sends a RequestVote RPC to a peer using the server's RPC client pool.
func (s *Server) SendRequestVote(ctx context.Context, peer Peer, args *raft.RequestVoteArgs) (*raft.RequestVoteReply, error) {
	if s.RPCClientPool == nil {
		return nil, fmt.Errorf("server %d RPCClientPool is nil", s.ID)
	}
	if peer.Address == "" {
		if p, ok := s.GetPeer(peer.ID); ok {
			peer = p
		}
	}

	s.GetLogger().Info("sending RequestVote", "peer_id", int(peer.ID), "term", uint64(args.Term))
	reply, err := s.RPCClientPool.SendRequestVote(ctx, peer, args)
	if err != nil {
		s.GetLogger().Warn("failed to send RequestVote", "peer_id", int(peer.ID), "error", err, "is_timeout", rpc.IsTimeout(err))
		return nil, err
	}
	return reply, nil
}

// SendAppendEntries sends an AppendEntries RPC to a peer using the server's RPC client pool.
func (s *Server) SendAppendEntries(ctx context.Context, peer Peer, args *raft.AppendEntriesArgs) (*raft.AppendEntriesReply, error) {
	if s.RPCClientPool == nil {
		return nil, fmt.Errorf("server %d RPCClientPool is nil", s.ID)
	}
	if peer.Address == "" {
		if p, ok := s.GetPeer(peer.ID); ok {
			peer = p
		}
	}

	s.GetLogger().Info("sending AppendEntries", "peer_id", int(peer.ID), "term", uint64(args.Term), "entries_count", len(args.Entries))
	reply, err := s.RPCClientPool.SendAppendEntries(ctx, peer, args)
	if err != nil {
		s.GetLogger().Warn("failed to send AppendEntries", "peer_id", int(peer.ID), "error", err, "is_timeout", rpc.IsTimeout(err))
		return nil, err
	}
	return reply, nil
}

// SendRequestVoteToNode sends a RequestVote RPC to a peer identified by NodeID.
func (s *Server) SendRequestVoteToNode(ctx context.Context, id raft.NodeID, args *raft.RequestVoteArgs) (*raft.RequestVoteReply, error) {
	peer, ok := s.GetPeer(id)
	if !ok {
		return nil, fmt.Errorf("peer %d not found in server peer list", id)
	}
	return s.SendRequestVote(ctx, peer, args)
}

// SendAppendEntriesToNode sends an AppendEntries RPC to a peer identified by NodeID.
func (s *Server) SendAppendEntriesToNode(ctx context.Context, id raft.NodeID, args *raft.AppendEntriesArgs) (*raft.AppendEntriesReply, error) {
	peer, ok := s.GetPeer(id)
	if !ok {
		return nil, fmt.Errorf("peer %d not found in server peer list", id)
	}
	return s.SendAppendEntries(ctx, peer, args)
}

// SetRPCTimeout configures the default RPC timeout for peer calls.
func (s *Server) SetRPCTimeout(timeout time.Duration) {
	if s.RPCClientPool != nil {
		s.RPCClientPool.SetRPCTimeout(timeout)
	}
}

// RPCTimeout returns the current default RPC timeout for peer calls.
func (s *Server) RPCTimeout() time.Duration {
	if s.RPCClientPool != nil {
		return s.RPCClientPool.RPCTimeout()
	}
	return rpc.DefaultRPCTimeout
}

// sendRequestVote is a convenience stub matching the Raft paper specification, using the server's RPCTimeout.
func (s *Server) sendRequestVote(peer Peer, args *raft.RequestVoteArgs) (*raft.RequestVoteReply, error) {
	ctx, cancel := context.WithTimeout(context.Background(), s.RPCTimeout())
	defer cancel()
	return s.SendRequestVote(ctx, peer, args)
}

// sendAppendEntries is a convenience stub matching the Raft paper specification, using the server's RPCTimeout.
func (s *Server) sendAppendEntries(peer Peer, args *raft.AppendEntriesArgs) (*raft.AppendEntriesReply, error) {
	ctx, cancel := context.WithTimeout(context.Background(), s.RPCTimeout())
	defer cancel()
	return s.SendAppendEntries(ctx, peer, args)
}

// Close stops the gRPC server and closes all peer RPC connections.
func (s *Server) Close() error {
	s.GetLogger().Info("closing server")
	s.Stop()
	if s.RPCClientPool != nil {
		return s.RPCClientPool.Close()
	}
	return nil
}


