package server

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"net"
	"os"
	"sync"
	"sync/atomic"
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

// RequestVoteEvent packages an incoming RequestVote RPC with a response channel.
type RequestVoteEvent struct {
	Args  *raftpb.RequestVoteArgs
	ResCh chan<- *raftpb.RequestVoteReply
}

// AppendEntriesEvent packages an incoming AppendEntries RPC with a response channel.
type AppendEntriesEvent struct {
	Args  *raftpb.AppendEntriesArgs
	ResCh chan<- *raftpb.AppendEntriesReply
}

type stateAction struct {
	fn    func()
	resCh chan struct{}
}

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

	// Volatile consensus state
	VolatileState       *raft.VolatileState       `json:"volatile_state"`
	VolatileStateLeader *raft.VolatileStateLeader `json:"volatile_state_leader,omitempty"`

	// ElectionTimer manages the randomized election timeout backed by time.Timer.
	ElectionTimer *raft.ElectionTimer `json:"-"`

	// Leader heartbeat timer
	heartbeatTimer    *time.Timer
	heartbeatInterval time.Duration

	// Candidate vote tracking
	votesReceived map[raft.NodeID]bool

	// RPCClientPool manages gRPC client connections to peers.
	RPCClientPool *rpc.ClientPool `json:"-"`

	// Logger is the structured logger for this node with [Node <ID>] prefix on every line.
	Logger *slog.Logger `json:"-"`

	lastHeartbeat time.Time

	// Concurrency channels for the single event loop goroutine:
	stopCh          chan struct{}
	stopOnce        sync.Once
	requestVoteCh   chan RequestVoteEvent
	appendEntriesCh chan AppendEntriesEvent
	stateActionCh   chan stateAction
	loopDone        chan struct{}
	loopRunning     atomic.Bool

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

	stopCh := make(chan struct{})
	requestVoteCh := make(chan RequestVoteEvent, 64)
	appendEntriesCh := make(chan AppendEntriesEvent, 64)
	stateActionCh := make(chan stateAction, 64)
	loopDone := make(chan struct{})

	return &Server{
		ID:                id,
		Peers:             peers,
		PersistentState:   persistentState,
		State:             raft.Follower,
		VolatileState:     &raft.VolatileState{CommitIndex: 0, LastApplied: 0},
		ElectionTimer:     raft.NewElectionTimer(raft.DefaultMinElectionTimeout, raft.DefaultMaxElectionTimeout),
		RPCClientPool:     rpcClientPool,
		Logger:            nodeLogger,
		lastHeartbeat:     time.Now(),
		heartbeatInterval: raft.DefaultHeartbeatInterval,
		votesReceived:     make(map[raft.NodeID]bool),
		stopCh:            stopCh,
		requestVoteCh:     requestVoteCh,
		appendEntriesCh:   appendEntriesCh,
		stateActionCh:     stateActionCh,
		loopDone:          loopDone,
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
// When the event loop is active, the mutation is executed on the single event-loop goroutine.
func (s *Server) SetState(state raft.RaftState) {
	if s.loopRunning.Load() {
		resCh := make(chan struct{}, 1)
		select {
		case s.stateActionCh <- stateAction{
			fn: func() {
				old := s.State
				s.State = state
				s.GetLogger().Info("transitioned consensus state", "from", old.String(), "to", state.String())
			},
			resCh: resCh,
		}:
		case <-s.stopCh:
			return
		}
		select {
		case <-resCh:
		case <-s.stopCh:
		}
		return
	}

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
// When the event loop is active, the mutation is executed on the single event-loop goroutine.
func (s *Server) ResetElectionTimeout() time.Duration {
	if s.loopRunning.Load() {
		resCh := make(chan time.Duration, 1)
		select {
		case s.stateActionCh <- stateAction{
			fn: func() {
				if s.ElectionTimer != nil {
					d := s.ElectionTimer.Reset()
					s.GetLogger().Info("reset election timeout", "timeout_ms", d.Milliseconds())
					resCh <- d
				} else {
					resCh <- 0
				}
			},
		}:
		case <-s.stopCh:
			return 0
		}
		select {
		case d := <-resCh:
			return d
		case <-s.stopCh:
			return 0
		}
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	if s.ElectionTimer == nil {
		return 0
	}
	d := s.ElectionTimer.Reset()
	s.GetLogger().Info("reset election timeout", "timeout_ms", d.Milliseconds())
	return d
}

// HandleHeartbeat processes a heartbeat event from a leader.
// When the event loop is active, the heartbeat is routed through the single event loop.
func (s *Server) HandleHeartbeat(leaderID raft.NodeID, term raft.Term) (bool, time.Duration) {
	if s.loopRunning.Load() {
		resCh := make(chan *raftpb.AppendEntriesReply, 1)
		args := &raftpb.AppendEntriesArgs{
			LeaderId: int64(leaderID),
			Term:     uint64(term),
		}
		select {
		case s.appendEntriesCh <- AppendEntriesEvent{Args: args, ResCh: resCh}:
		case <-s.stopCh:
			return false, 0
		}

		select {
		case reply := <-resCh:
			valid := (uint64(term) >= reply.Term)
			var timeout time.Duration
			if s.ElectionTimer != nil {
				timeout = s.ElectionTimer.Duration()
			}
			return valid, timeout
		case <-s.stopCh:
			return false, 0
		}
	}

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

	// Launch the single event-loop goroutine
	s.StartEventLoop()

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

	s.stopOnce.Do(func() {
		close(s.stopCh)
	})

	if s.loopRunning.Load() {
		select {
		case <-s.loopDone:
		case <-time.After(500 * time.Millisecond):
		}
	}

	if s.ElectionTimer != nil {
		s.ElectionTimer.Stop()
	}
	if s.heartbeatTimer != nil {
		s.heartbeatTimer.Stop()
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

	s.stopOnce.Do(func() {
		close(s.stopCh)
	})

	if s.loopRunning.Load() {
		select {
		case <-s.loopDone:
		case <-time.After(500 * time.Millisecond):
		}
	}

	if s.ElectionTimer != nil {
		s.ElectionTimer.Stop()
	}
	if s.heartbeatTimer != nil {
		s.heartbeatTimer.Stop()
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
// When the event loop is active, the request is dispatched over requestVoteCh.
// When a vote is granted to a candidate, it resets the server's election timer (§5.2).
func (s *Server) RequestVote(ctx context.Context, req *raftpb.RequestVoteArgs) (*raftpb.RequestVoteReply, error) {
	if s.loopRunning.Load() {
		resCh := make(chan *raftpb.RequestVoteReply, 1)
		select {
		case s.requestVoteCh <- RequestVoteEvent{Args: req, ResCh: resCh}:
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-s.stopCh:
			return nil, fmt.Errorf("server %d stopped", s.ID)
		}

		select {
		case reply := <-resCh:
			return reply, nil
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-s.stopCh:
			return nil, fmt.Errorf("server %d stopped", s.ID)
		}
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	return s.processRequestVote(req), nil
}

// AppendEntries handles incoming AppendEntries RPCs (heartbeats and log replication).
// When the event loop is active, the request is dispatched over appendEntriesCh.
// A valid heartbeat (req.Term >= currentTerm) resets the node's election timer (§5.2).
func (s *Server) AppendEntries(ctx context.Context, req *raftpb.AppendEntriesArgs) (*raftpb.AppendEntriesReply, error) {
	if s.loopRunning.Load() {
		resCh := make(chan *raftpb.AppendEntriesReply, 1)
		select {
		case s.appendEntriesCh <- AppendEntriesEvent{Args: req, ResCh: resCh}:
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-s.stopCh:
			return nil, fmt.Errorf("server %d stopped", s.ID)
		}

		select {
		case reply := <-resCh:
			return reply, nil
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-s.stopCh:
			return nil, fmt.Errorf("server %d stopped", s.ID)
		}
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	return s.processAppendEntries(req), nil
}

// processRequestVote evaluates an incoming RequestVote RPC.
func (s *Server) processRequestVote(req *raftpb.RequestVoteArgs) *raftpb.RequestVoteReply {
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
		}
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
	}
}

// processAppendEntries evaluates an incoming AppendEntries RPC.
func (s *Server) processAppendEntries(req *raftpb.AppendEntriesArgs) *raftpb.AppendEntriesReply {
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
		}
	}

	// 2. Valid heartbeat/RPC from current or new leader:
	// If candidate receives AppendEntries from leader, revert to follower (§5.2)
	if s.State == raft.Candidate {
		s.becomeFollower(raft.Term(req.Term))
	} else {
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
	}

	return &raftpb.AppendEntriesReply{
		Term:    currentTerm,
		Success: false, // Stub until log replication logic is added
	}
	
}

// becomeFollower transitions the node to the Follower role for the specified term.
// In Raft (§5.1 & §5.2):
// - Updates state to Follower
// - If term > currentTerm, updates currentTerm and resets votedFor to -1
// - Stops heartbeat timer if running (was Leader)
// - Resets the election timer with a new randomized timeout
// - Clears votesReceived candidate tracking
func (s *Server) becomeFollower(term raft.Term) {
	oldRole := s.State
	s.State = raft.Follower

	if s.PersistentState != nil && term > s.PersistentState.CurrentTerm {
		s.PersistentState.CurrentTerm = term
		s.PersistentState.VotedFor = -1
	}

	if s.heartbeatTimer != nil {
		s.heartbeatTimer.Stop()
	}

	s.lastHeartbeat = time.Now()
	if s.ElectionTimer != nil {
		s.ElectionTimer.Reset()
	}

	s.votesReceived = make(map[raft.NodeID]bool)

	var curTerm uint64
	if s.PersistentState != nil {
		curTerm = uint64(s.PersistentState.CurrentTerm)
	}

	s.GetLogger().Info("transitioned to Follower",
		"node_id", int(s.ID),
		"previous_state", oldRole.String(),
		"term", curTerm,
	)
}

// becomeCandidate transitions the node to the Candidate role to initiate a new election.
// In Raft (§5.2):
// - Updates state to Candidate
// - Increments currentTerm
// - Votes for self (votedFor = s.ID)
// - Stops heartbeat timer if running
// - Resets the election timer with a new randomized timeout
// - Initializes vote tracking with self-vote
func (s *Server) becomeCandidate() {
	oldRole := s.State
	s.State = raft.Candidate

	if s.PersistentState != nil {
		s.PersistentState.CurrentTerm++
		s.PersistentState.VotedFor = s.ID
	}

	if s.heartbeatTimer != nil {
		s.heartbeatTimer.Stop()
	}

	if s.ElectionTimer != nil {
		s.ElectionTimer.Reset()
	}

	s.votesReceived = make(map[raft.NodeID]bool)
	s.votesReceived[s.ID] = true

	var curTerm uint64
	if s.PersistentState != nil {
		curTerm = uint64(s.PersistentState.CurrentTerm)
	}

	s.GetLogger().Info("transitioned to Candidate (started election; RequestVote RPC broadcast stubbed)",
		"node_id", int(s.ID),
		"previous_state", oldRole.String(),
		"term", curTerm,
		"voted_for", int(s.ID),
		"votes_count", 1,
	)
}

// becomeLeader transitions the node from Candidate to Leader upon achieving majority votes.
// In Raft (§5.2 & §5.3):
// - Updates state to Leader
// - Stops election timer (leaders do not time out on elections)
// - Initializes VolatileStateLeader (nextIndex and matchIndex for all peers)
// - Initializes and arms the heartbeat timer (50ms interval)
func (s *Server) becomeLeader() {
	oldRole := s.State
	s.State = raft.Leader

	if s.ElectionTimer != nil {
		s.ElectionTimer.Stop()
	}

	var lastLogIndex raft.LogIndex = 0
	if s.PersistentState != nil && len(s.PersistentState.Log) > 0 {
		lastLogIndex = s.PersistentState.Log[len(s.PersistentState.Log)-1].Index
	}

	s.VolatileStateLeader = &raft.VolatileStateLeader{
		NextIndex:  make(map[raft.NodeID]raft.LogIndex),
		MatchIndex: make(map[raft.NodeID]raft.LogIndex),
	}
	for _, p := range s.Peers {
		s.VolatileStateLeader.NextIndex[p.ID] = lastLogIndex + 1
		s.VolatileStateLeader.MatchIndex[p.ID] = 0
	}

	if s.heartbeatInterval <= 0 {
		s.heartbeatInterval = raft.DefaultHeartbeatInterval
	}
	if s.heartbeatTimer == nil {
		s.heartbeatTimer = time.NewTimer(s.heartbeatInterval)
	} else {
		s.heartbeatTimer.Reset(s.heartbeatInterval)
	}

	var curTerm uint64
	if s.PersistentState != nil {
		curTerm = uint64(s.PersistentState.CurrentTerm)
	}

	s.GetLogger().Info("transitioned to Leader (started leadership; AppendEntries heartbeat broadcast stubbed)",
		"node_id", int(s.ID),
		"previous_state", oldRole.String(),
		"term", curTerm,
		"peers_count", len(s.Peers),
	)
}

// handleElectionTimeout is called when the election timer fires.
// In Raft (§5.2):
// The node transitions to Candidate, increments currentTerm, votes for itself,
// and resets the election timer to start a new election cycle.
func (s *Server) handleElectionTimeout(t time.Time) {
	s.GetLogger().Info("election timeout fired", "node_id", int(s.ID), "state", s.State.String(), "time", t)

	if s.State == raft.Leader {
		return
	}

	s.becomeCandidate()
}

// handleHeartbeatTimeout is called when the leader's heartbeat timer fires.
func (s *Server) handleHeartbeatTimeout() {
	if s.State != raft.Leader {
		return
	}
	var curTerm uint64
	if s.PersistentState != nil {
		curTerm = uint64(s.PersistentState.CurrentTerm)
	}
	s.GetLogger().Info("leader heartbeat timer fired (AppendEntries heartbeat broadcast stubbed)",
		"node_id", int(s.ID),
		"term", curTerm,
	)
	if s.heartbeatTimer != nil {
		s.heartbeatTimer.Reset(s.heartbeatInterval)
	}
}

// run is the central event loop of the Raft node.
// It uses a select statement to multiplex between:
// 1. Timer channel (election timeout when follower/candidate)
// 2. Heartbeat channel (when leader)
// 3. Incoming RPC channels (requestVoteCh and appendEntriesCh)
// 4. State mutation actions (stateActionCh)
// 5. Stop channel (stopCh)
func (s *Server) run() {
	s.loopRunning.Store(true)
	s.GetLogger().Info("started main Raft event loop", "node_id", int(s.ID))

	defer func() {
		s.loopRunning.Store(false)
		s.GetLogger().Info("stopped main Raft event loop", "node_id", int(s.ID))
		close(s.loopDone)
	}()

	for {
		var timerCh <-chan time.Time
		if s.State != raft.Leader && s.ElectionTimer != nil {
			timerCh = s.ElectionTimer.C()
		}

		var heartbeatCh <-chan time.Time
		if s.State == raft.Leader && s.heartbeatTimer != nil {
			heartbeatCh = s.heartbeatTimer.C
		}

		select {
		case <-s.stopCh:
			return

		case t, ok := <-timerCh:
			if !ok {
				return
			}
			s.mu.Lock()
			s.handleElectionTimeout(t)
			s.mu.Unlock()

		case <-heartbeatCh:
			s.mu.Lock()
			s.handleHeartbeatTimeout()
			s.mu.Unlock()

		case ev := <-s.requestVoteCh:
			s.mu.Lock()
			reply := s.processRequestVote(ev.Args)
			s.mu.Unlock()
			if ev.ResCh != nil {
				ev.ResCh <- reply
			}

		case ev := <-s.appendEntriesCh:
			s.mu.Lock()
			reply := s.processAppendEntries(ev.Args)
			s.mu.Unlock()
			if ev.ResCh != nil {
				ev.ResCh <- reply
			}

		case act := <-s.stateActionCh:
			s.mu.Lock()
			act.fn()
			s.mu.Unlock()
			if act.resCh != nil {
				act.resCh <- struct{}{}
			}
		}
	}
}

// Run executes the main run() loop synchronously, blocking until stopCh is closed.
func (s *Server) Run() {
	s.run()
}

// StartEventLoop launches the main run() event-loop in a separate goroutine.
func (s *Server) StartEventLoop() {
	if !s.loopRunning.CompareAndSwap(false, true) {
		return
	}
	go s.run()
}

// IsLoopRunning returns true if the main run() event loop is actively running.
func (s *Server) IsLoopRunning() bool {
	return s.loopRunning.Load()
}

// StopChannel returns the read-only stop channel.
func (s *Server) StopChannel() <-chan struct{} {
	return s.stopCh
}

// RequestVoteChannel returns the channel for incoming RequestVote events.
func (s *Server) RequestVoteChannel() chan<- RequestVoteEvent {
	return s.requestVoteCh
}

// AppendEntriesChannel returns the channel for incoming AppendEntries events.
func (s *Server) AppendEntriesChannel() chan<- AppendEntriesEvent {
	return s.appendEntriesCh
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


