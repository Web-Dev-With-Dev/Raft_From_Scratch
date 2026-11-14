package server

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"os"
	"sync"
	"sync/atomic"
	"time"

	"raft/logger"
	raftpb "raft/proto"
	"raft/raft"
	"raft/rpc"
	"raft/store"

	"google.golang.org/grpc"
	"google.golang.org/grpc/keepalive"
)

// Type aliases for convenience
type Peer = raft.Peer
type ClientPool = rpc.ClientPool
type RPCClientPool = rpc.ClientPool
type LogOptions = logger.Options
type ElectionTimer = raft.ElectionTimer
type Storage = store.Storage
type MemoryStorage = store.MemoryStorage

// NodeStatus represents the runtime status of a Raft node exposed via the debug HTTP endpoint (/status).
type NodeStatus struct {
	ID          raft.NodeID   `json:"id"`
	Term        raft.Term     `json:"term"`
	State       string        `json:"state"`
	VotedFor    raft.NodeID   `json:"votedFor"`
	VotedForAlt raft.NodeID   `json:"voted_for"`
	CommitIndex raft.LogIndex `json:"commitIndex"`
	LastApplied raft.LogIndex `json:"lastApplied"`
	LogLength   int           `json:"logLength"`
}

const (
	DefaultMinElectionTimeout = raft.DefaultMinElectionTimeout
	DefaultMaxElectionTimeout = raft.DefaultMaxElectionTimeout
	DefaultHeartbeatInterval  = raft.DefaultHeartbeatInterval
)

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

// RequestVoteResult represents the outcome of an asynchronous RequestVote RPC sent to a peer.
type RequestVoteResult struct {
	PeerID       raft.NodeID
	ElectionTerm raft.Term
	Reply        *raft.RequestVoteReply
	Err          error
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

	// Storage provides persistence for Raft state (currentTerm, votedFor, log).
	// Currently initialized with an in-memory Storage implementation, to be replaced in Month 4.
	Storage store.Storage `json:"-"`

	// State represents the current consensus role (Follower, Candidate, Leader).
	State raft.RaftState `json:"state"`

	// Volatile consensus state
	VolatileState       *raft.VolatileState       `json:"volatile_state"`
	VolatileStateLeader *raft.VolatileStateLeader `json:"volatile_state_leader,omitempty"`

	// ElectionTimer manages the randomized election timeout backed by time.Timer.
	ElectionTimer *raft.ElectionTimer `json:"-"`

	// Leader-only heartbeat ticker (fires every heartbeatInterval, e.g. 50ms)
	heartbeatTicker   *time.Ticker
	heartbeatTimer    *time.Timer
	heartbeatInterval time.Duration
	heartbeatsSent    atomic.Uint64

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
	voteResultCh    chan RequestVoteResult
	stateActionCh   chan stateAction
	loopDone        chan struct{}
	loopRunning     atomic.Bool

	grpcServer *grpc.Server
	listener   net.Listener

	httpServer   *http.Server
	httpListener net.Listener

	// logUpToDateChecker is an optional testing hook for log up-to-date assertions.
	logUpToDateChecker func(lastLogIndex uint64, lastLogTerm uint64) bool
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
	voteResultCh := make(chan RequestVoteResult, 64)
	stateActionCh := make(chan stateAction, 64)
	loopDone := make(chan struct{})

	memStorage := store.NewMemoryStorage()
	if persistentState != nil {
		_ = memStorage.Persist(persistentState)
	}

	return &Server{
		ID:                id,
		Peers:             peers,
		PersistentState:   persistentState,
		Storage:           memStorage,
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
		voteResultCh:      voteResultCh,
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

// VotesReceived returns a copy of the candidate's recorded votes.
func (s *Server) VotesReceived() map[raft.NodeID]bool {
	s.mu.RLock()
	defer s.mu.RUnlock()
	res := make(map[raft.NodeID]bool, len(s.votesReceived))
	for k, v := range s.votesReceived {
		res[k] = v
	}
	return res
}

// VoteResultChannel returns the internal vote result channel used by the event loop.
func (s *Server) VoteResultChannel() chan RequestVoteResult {
	return s.voteResultCh
}

// GetStorage returns the current storage engine.
func (s *Server) GetStorage() store.Storage {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.Storage
}

// SetStorage configures a custom storage engine for the server.
func (s *Server) SetStorage(storage store.Storage) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.Storage = storage
}

// Persist writes the current PersistentState to the storage engine.
// In Raft (§5.2, §5.6), persistent state (currentTerm, votedFor, log) must be persisted
// before responding to RPCs or changing roles.
// When the event loop is active, persistence is serialized onto the single event loop.
func (s *Server) Persist() error {
	if s.loopRunning.Load() {
		var err error
		resCh := make(chan struct{}, 1)
		select {
		case s.stateActionCh <- stateAction{
			fn: func() {
				err = s.persistLocked()
			},
			resCh: resCh,
		}:
		case <-s.stopCh:
			return fmt.Errorf("server %d stopped", s.ID)
		}
		select {
		case <-resCh:
			return err
		case <-s.stopCh:
			return fmt.Errorf("server %d stopped", s.ID)
		}
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	return s.persistLocked()
}

// persistLocked persists the current persistent state while holding s.mu (or inside event loop).
func (s *Server) persistLocked() error {
	if s.Storage == nil || s.PersistentState == nil {
		return nil
	}
	if err := s.Storage.Persist(s.PersistentState); err != nil {
		s.GetLogger().Error("failed to persist Raft state", "error", err)
		return err
	}
	s.GetLogger().Debug("persisted Raft state",
		"term", uint64(s.PersistentState.CurrentTerm),
		"voted_for", int(s.PersistentState.VotedFor),
		"log_len", len(s.PersistentState.Log),
	)
	return nil
}

// Load restores the PersistentState from the storage engine.
// When the event loop is active, loading is serialized onto the single event loop.
func (s *Server) Load() error {
	if s.loopRunning.Load() {
		var err error
		resCh := make(chan struct{}, 1)
		select {
		case s.stateActionCh <- stateAction{
			fn: func() {
				err = s.loadLocked()
			},
			resCh: resCh,
		}:
		case <-s.stopCh:
			return fmt.Errorf("server %d stopped", s.ID)
		}
		select {
		case <-resCh:
			return err
		case <-s.stopCh:
			return fmt.Errorf("server %d stopped", s.ID)
		}
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	return s.loadLocked()
}

// loadLocked restores persistent state while holding s.mu (or inside event loop).
func (s *Server) loadLocked() error {
	if s.Storage == nil {
		return nil
	}
	state, err := s.Storage.Load()
	if err != nil {
		s.GetLogger().Error("failed to load Raft state from storage", "error", err)
		return err
	}
	if state != nil {
		s.PersistentState = state
		s.GetLogger().Info("loaded Raft state from storage",
			"term", uint64(state.CurrentTerm),
			"voted_for", int(state.VotedFor),
			"log_len", len(state.Log),
		)
	}
	return nil
}

// SetCurrentTerm sets the current term and persists state (§5.1).
// If term is higher than currentTerm, votedFor is reset to -1 and state is persisted.
// When the event loop is active, the mutation is serialized on the event loop.
func (s *Server) SetCurrentTerm(term raft.Term) error {
	if s.loopRunning.Load() {
		var err error
		resCh := make(chan struct{}, 1)
		select {
		case s.stateActionCh <- stateAction{
			fn: func() {
				err = s.setCurrentTermLocked(term)
			},
			resCh: resCh,
		}:
		case <-s.stopCh:
			return fmt.Errorf("server %d stopped", s.ID)
		}
		select {
		case <-resCh:
			return err
		case <-s.stopCh:
			return fmt.Errorf("server %d stopped", s.ID)
		}
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	return s.setCurrentTermLocked(term)
}

func (s *Server) setCurrentTermLocked(term raft.Term) error {
	if s.PersistentState == nil {
		s.PersistentState = &raft.PersistentState{
			CurrentTerm: term,
			VotedFor:    -1,
		}
		return s.persistLocked()
	}

	if term != s.PersistentState.CurrentTerm {
		if term > s.PersistentState.CurrentTerm {
			s.PersistentState.VotedFor = -1
		}
		s.PersistentState.CurrentTerm = term
		return s.persistLocked()
	}
	return nil
}

// SetVotedFor sets the candidate ID voted for in the current term and persists state (§5.2).
// When the event loop is active, the mutation is serialized on the event loop.
func (s *Server) SetVotedFor(votedFor raft.NodeID) error {
	if s.loopRunning.Load() {
		var err error
		resCh := make(chan struct{}, 1)
		select {
		case s.stateActionCh <- stateAction{
			fn: func() {
				err = s.setVotedForLocked(votedFor)
			},
			resCh: resCh,
		}:
		case <-s.stopCh:
			return fmt.Errorf("server %d stopped", s.ID)
		}
		select {
		case <-resCh:
			return err
		case <-s.stopCh:
			return fmt.Errorf("server %d stopped", s.ID)
		}
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	return s.setVotedForLocked(votedFor)
}

func (s *Server) setVotedForLocked(votedFor raft.NodeID) error {
	if s.PersistentState == nil {
		s.PersistentState = &raft.PersistentState{
			CurrentTerm: 0,
			VotedFor:    votedFor,
		}
		return s.persistLocked()
	}

	if s.PersistentState.VotedFor != votedFor {
		s.PersistentState.VotedFor = votedFor
		return s.persistLocked()
	}
	return nil
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
				if old == raft.Leader && state != raft.Leader {
					s.stopHeartbeatTickerLocked()
				}
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
	if old == raft.Leader && state != raft.Leader {
		s.stopHeartbeatTickerLocked()
	}
	s.GetLogger().Info("transitioned consensus state", "from", old.String(), "to", state.String())
}

// GetElectionTimer returns the server's election timer.
func (s *Server) GetElectionTimer() *raft.ElectionTimer {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.ElectionTimer
}

// GetHeartbeatTicker returns the leader's heartbeat ticker, or nil if not running.
func (s *Server) GetHeartbeatTicker() *time.Ticker {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.heartbeatTicker
}

// HeartbeatInterval returns the configured interval between leader heartbeats (default 50ms).
func (s *Server) HeartbeatInterval() time.Duration {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if s.heartbeatInterval <= 0 {
		return raft.DefaultHeartbeatInterval
	}
	return s.heartbeatInterval
}

// SetHeartbeatInterval configures the heartbeat ticker interval.
func (s *Server) SetHeartbeatInterval(interval time.Duration) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if interval <= 0 {
		interval = raft.DefaultHeartbeatInterval
	}
	s.heartbeatInterval = interval
	if s.State == raft.Leader && s.heartbeatTicker != nil {
		s.heartbeatTicker.Reset(interval)
	}
	if s.State == raft.Leader && s.heartbeatTimer != nil {
		s.heartbeatTimer.Reset(interval)
	}
}

// HeartbeatsSent returns the total number of leader heartbeat ticks dispatched.
func (s *Server) HeartbeatsSent() uint64 {
	return s.heartbeatsSent.Load()
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

// Status returns the current runtime status of the Raft node (term, state, votedFor, etc.).
func (s *Server) Status() NodeStatus {
	s.mu.RLock()
	defer s.mu.RUnlock()

	var term raft.Term
	votedFor := raft.NodeID(-1)
	logLen := 0
	if s.PersistentState != nil {
		term = s.PersistentState.CurrentTerm
		votedFor = s.PersistentState.VotedFor
		logLen = len(s.PersistentState.Log)
	}

	var commitIndex, lastApplied raft.LogIndex
	if s.VolatileState != nil {
		commitIndex = s.VolatileState.CommitIndex
		lastApplied = s.VolatileState.LastApplied
	}

	return NodeStatus{
		ID:          s.ID,
		Term:        term,
		State:       s.State.String(),
		VotedFor:    votedFor,
		VotedForAlt: votedFor,
		CommitIndex: commitIndex,
		LastApplied: lastApplied,
		LogLength:   logLen,
	}
}

// HandleStatus serves the /status debug endpoint as an http.HandlerFunc.
func (s *Server) HandleStatus(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet && r.Method != http.MethodHead {
		http.Error(w, "Method Not Allowed", http.StatusMethodNotAllowed)
		return
	}

	status := s.Status()
	w.Header().Set("Content-Type", "application/json")
	encoder := json.NewEncoder(w)
	encoder.SetIndent("", "  ")
	if err := encoder.Encode(status); err != nil {
		s.GetLogger().Error("failed to encode status response", "error", err)
	}
}

// HTTPHandler returns an http.Handler multiplexer configured with the /status endpoint.
func (s *Server) HTTPHandler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("/status", s.HandleStatus)
	return mux
}

// StartHTTPServer starts the debug HTTP server on the specified network address in the background.
func (s *Server) StartHTTPServer(address string) error {
	s.GetLogger().Info("starting debug HTTP server", "address", address)
	lis, err := net.Listen("tcp", address)
	if err != nil {
		s.GetLogger().Error("failed to listen on HTTP address", "address", address, "error", err)
		return fmt.Errorf("failed to listen on HTTP address %s: %w", address, err)
	}
	return s.ServeHTTPDebug(lis)
}

// ServeHTTPDebug starts the debug HTTP server on the provided listener in the background.
func (s *Server) ServeHTTPDebug(lis net.Listener) error {
	s.mu.Lock()
	if s.httpServer != nil {
		s.mu.Unlock()
		_ = lis.Close()
		return fmt.Errorf("http server is already running")
	}

	httpServer := &http.Server{
		Handler: s.HTTPHandler(),
	}
	s.httpServer = httpServer
	s.httpListener = lis
	addrStr := lis.Addr().String()
	s.mu.Unlock()

	s.GetLogger().Info("debug HTTP server listening", "address", addrStr)

	go func() {
		if err := httpServer.Serve(lis); err != nil && err != http.ErrServerClosed {
			s.GetLogger().Error("debug HTTP server error during serve", "error", err)
		}
	}()

	return nil
}

// HTTPAddr returns the network address the debug HTTP server is listening on, or nil if not running.
func (s *Server) HTTPAddr() net.Addr {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if s.httpListener != nil {
		return s.httpListener.Addr()
	}
	return nil
}

// HTTPServer returns the active debug *http.Server instance, or nil if not running.
func (s *Server) HTTPServer() *http.Server {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.httpServer
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

// stopHeartbeatTickerLocked stops and cleans up the leader's heartbeat ticker and timer.
// Must be called with s.mu held (or inside the event loop).
func (s *Server) stopHeartbeatTickerLocked() {
	if s.heartbeatTicker != nil {
		s.heartbeatTicker.Stop()
		s.heartbeatTicker = nil
	}
	if s.heartbeatTimer != nil {
		s.heartbeatTimer.Stop()
		s.heartbeatTimer = nil
	}
}

// Stop immediately stops the gRPC server, debug HTTP server, and main event loop.
func (s *Server) Stop() {
	s.stopOnce.Do(func() {
		close(s.stopCh)
	})

	if s.loopRunning.Load() {
		select {
		case <-s.loopDone:
		case <-time.After(1 * time.Second):
		}
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	if s.ElectionTimer != nil {
		s.ElectionTimer.Stop()
	}
	s.stopHeartbeatTickerLocked()

	if s.grpcServer != nil {
		s.grpcServer.Stop()
		s.grpcServer = nil
		s.GetLogger().Info("stopped gRPC server")
	}
	if s.listener != nil {
		_ = s.listener.Close()
		s.listener = nil
	}
	if s.httpServer != nil {
		_ = s.httpServer.Close()
		s.httpServer = nil
		s.GetLogger().Info("stopped debug HTTP server")
	}
	if s.httpListener != nil {
		_ = s.httpListener.Close()
		s.httpListener = nil
	}
}

// GracefulStop gracefully stops the gRPC server, debug HTTP server, and closes listeners.
func (s *Server) GracefulStop() {
	s.stopOnce.Do(func() {
		close(s.stopCh)
	})

	if s.loopRunning.Load() {
		select {
		case <-s.loopDone:
		case <-time.After(1 * time.Second):
		}
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	if s.ElectionTimer != nil {
		s.ElectionTimer.Stop()
	}
	s.stopHeartbeatTickerLocked()

	if s.grpcServer != nil {
		s.grpcServer.GracefulStop()
		s.grpcServer = nil
		s.GetLogger().Info("gracefully stopped gRPC server")
	}
	if s.listener != nil {
		_ = s.listener.Close()
		s.listener = nil
	}
	if s.httpServer != nil {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		_ = s.httpServer.Shutdown(ctx)
		cancel()
		s.httpServer = nil
		s.GetLogger().Info("gracefully stopped debug HTTP server")
	}
	if s.httpListener != nil {
		_ = s.httpListener.Close()
		s.httpListener = nil
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

// IsLogUpToDate checks whether a candidate's log is at least as up-to-date as this receiver's log (§5.4).
// Stub returning true for now; full comparison logic will be added in Day 17.
func (s *Server) IsLogUpToDate(lastLogIndex uint64, lastLogTerm uint64) bool {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.isLogUpToDateLocked(lastLogIndex, lastLogTerm)
}

// isLogUpToDateLocked checks log freshness with s.mu held (or within the single event loop).
func (s *Server) isLogUpToDateLocked(lastLogIndex uint64, lastLogTerm uint64) bool {
	if s.logUpToDateChecker != nil {
		return s.logUpToDateChecker(lastLogIndex, lastLogTerm)
	}
	return s.isLogUpToDate(lastLogIndex, lastLogTerm)
}

func (s *Server) isLogUpToDate(lastLogIndex uint64, lastLogTerm uint64) bool {
	// Stub: always returns true for now per Day 16 specification.
	// Day 17 will implement §5.4 term and index comparison.
	return true
}

// SetLogUpToDateChecker sets an optional hook for testing log up-to-date evaluations.
func (s *Server) SetLogUpToDateChecker(fn func(lastLogIndex uint64, lastLogTerm uint64) bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.logUpToDateChecker = fn
}

// processRequestVote evaluates an incoming RequestVote RPC exactly per Raft paper Figure 2 rules 1-2.
func (s *Server) processRequestVote(req *raftpb.RequestVoteArgs) *raftpb.RequestVoteReply {
	s.GetLogger().Info("received RequestVote",
		"candidate_id", req.CandidateId,
		"term", req.Term,
		"last_log_index", req.LastLogIndex,
		"last_log_term", req.LastLogTerm,
	)

	var currentTerm uint64
	var votedFor raft.NodeID = -1
	if s.PersistentState != nil {
		currentTerm = uint64(s.PersistentState.CurrentTerm)
		votedFor = s.PersistentState.VotedFor
	}

	// Figure 2 RequestVote Rule 1: Reply false if term < currentTerm (§5.1)
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

	// Rules for Servers (§5.1):
	// If RPC request or response contains term T > currentTerm:
	// set currentTerm = T, convert to follower (§5.1).
	steppedDown := false
	if req.Term > currentTerm {
		oldRole := s.State
		s.State = raft.Follower
		s.stopHeartbeatTickerLocked()
		s.votesReceived = make(map[raft.NodeID]bool)

		if s.PersistentState != nil {
			s.PersistentState.CurrentTerm = raft.Term(req.Term)
			s.PersistentState.VotedFor = -1
			currentTerm = uint64(s.PersistentState.CurrentTerm)
			votedFor = -1
		}
		steppedDown = true

		s.GetLogger().Info("transitioned to Follower on discovering higher term",
			"node_id", int(s.ID),
			"previous_state", oldRole.String(),
			"term", currentTerm,
		)
	}

	// Figure 2 RequestVote Rule 2:
	// If votedFor is null or candidateId, and candidate’s log is at least as up-to-date as receiver’s log, grant vote (§5.2, §5.4)
	canVote := (votedFor == -1 || votedFor == raft.NodeID(req.CandidateId))
	logOk := s.isLogUpToDateLocked(req.LastLogIndex, req.LastLogTerm)

	voteGranted := false
	if canVote && logOk {
		voteGranted = true
		if s.PersistentState != nil {
			s.PersistentState.VotedFor = raft.NodeID(req.CandidateId)
		}

		// Persist state changes (currentTerm and votedFor)
		_ = s.persistLocked()

		// Reset election timer when granting vote (§5.2)
		s.lastHeartbeat = time.Now()
		if s.ElectionTimer != nil {
			newTimeout := s.ElectionTimer.Reset()
			s.GetLogger().Info("reset election timer on granting vote",
				"candidate_id", req.CandidateId,
				"term", req.Term,
				"timeout_ms", newTimeout.Milliseconds(),
			)
		}

		s.GetLogger().Info("granted vote to candidate",
			"candidate_id", req.CandidateId,
			"term", currentTerm,
		)
	} else {
		// If stepped down due to higher term but vote was not granted, persist the higher term with votedFor = -1
		if steppedDown {
			_ = s.persistLocked()
			s.lastHeartbeat = time.Now()
			if s.ElectionTimer != nil {
				s.ElectionTimer.Reset()
			}
		}

		s.GetLogger().Info("denied vote to candidate",
			"candidate_id", req.CandidateId,
			"term", currentTerm,
			"voted_for", int(votedFor),
			"can_vote", canVote,
			"log_ok", logOk,
		)
	}

	return &raftpb.RequestVoteReply{
		Term:        currentTerm,
		VoteGranted: voteGranted,
	}
}

// processAppendEntries evaluates an incoming AppendEntries RPC exactly per Raft paper Figure 2 rules 1-2.
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

	// Figure 2 AppendEntries Rule 1: Reply false if term < currentTerm (§5.1)
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

	// Rules for Servers (§5.1):
	// If RPC request or response contains term T > currentTerm:
	// set currentTerm = T, convert to follower (§5.1).
	if req.Term > currentTerm {
		s.becomeFollowerLocked(raft.Term(req.Term))
		if s.PersistentState != nil {
			currentTerm = uint64(s.PersistentState.CurrentTerm)
		}
	} else if s.State == raft.Candidate {
		// §5.2: If candidate receives AppendEntries from current leader (req.Term == currentTerm), revert to follower
		s.becomeFollowerLocked(raft.Term(req.Term))
	} else {
		// Valid heartbeat from current leader for follower/leader: reset election timer (§5.2)
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

	// Figure 2 AppendEntries Rule 2:
	// Reply false if log doesn't contain an entry at prevLogIndex whose term matches prevLogTerm (§5.3)
	success := true
	if req.PrevLogIndex > 0 {
		var logLen uint64
		if s.PersistentState != nil {
			logLen = uint64(len(s.PersistentState.Log))
		}
		if req.PrevLogIndex > logLen {
			success = false
		} else if s.PersistentState.Log[req.PrevLogIndex-1].Term != raft.Term(req.PrevLogTerm) {
			success = false
		}
	}

	return &raftpb.AppendEntriesReply{
		Term:    currentTerm,
		Success: success,
	}
}

// becomeFollower transitions the node to the Follower role for the specified term.
// When the event loop is running, the mutation is executed on the single event loop.
func (s *Server) becomeFollower(term raft.Term) {
	if s.loopRunning.Load() {
		resCh := make(chan struct{}, 1)
		select {
		case s.stateActionCh <- stateAction{
			fn: func() {
				s.becomeFollowerLocked(term)
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
	s.becomeFollowerLocked(term)
}

// becomeFollowerLocked executes the follower state transition.
// Must be called with s.mu held (or inside the event loop).
// In Raft (§5.1 & §5.2):
// - Updates state to Follower
// - If term > currentTerm, updates currentTerm and resets votedFor to -1
// - Stops leader heartbeat ticker if running (was Leader)
// - Resets the election timer with a new randomized timeout
// - Clears votesReceived candidate tracking
func (s *Server) becomeFollowerLocked(term raft.Term) {
	oldRole := s.State
	s.State = raft.Follower

	if s.PersistentState != nil && term > s.PersistentState.CurrentTerm {
		s.PersistentState.CurrentTerm = term
		s.PersistentState.VotedFor = -1
		_ = s.persistLocked()
	}

	s.stopHeartbeatTickerLocked()

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
// When the event loop is running, the mutation is executed on the single event loop.
func (s *Server) becomeCandidate() {
	if s.loopRunning.Load() {
		resCh := make(chan struct{}, 1)
		select {
		case s.stateActionCh <- stateAction{
			fn: func() {
				s.becomeCandidateLocked()
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
	s.becomeCandidateLocked()
}

// becomeCandidateLocked executes the candidate transition.
// Must be called with s.mu held (or inside the event loop).
// In Raft (§5.2):
// - Updates state to Candidate
// - Increments currentTerm
// - Votes for self (votedFor = s.ID)
// - Stops leader heartbeat ticker if running
// - Resets the election timer with a new randomized timeout
// - Initializes vote tracking with self-vote
func (s *Server) becomeCandidateLocked() {
	oldRole := s.State
	s.State = raft.Candidate

	if s.PersistentState != nil {
		s.PersistentState.CurrentTerm++
		s.PersistentState.VotedFor = s.ID
		_ = s.persistLocked()
	}

	s.stopHeartbeatTickerLocked()

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
// When the event loop is running, the mutation is executed on the single event loop.
func (s *Server) becomeLeader() {
	if s.loopRunning.Load() {
		resCh := make(chan struct{}, 1)
		select {
		case s.stateActionCh <- stateAction{
			fn: func() {
				s.becomeLeaderLocked()
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
	s.becomeLeaderLocked()
}

// becomeLeaderLocked executes the leader transition.
// Must be called with s.mu held (or inside the event loop).
// In Raft (§5.2 & §5.3):
// - Updates state to Leader
// - Stops election timer (leaders do not time out on elections)
// - Initializes VolatileStateLeader (nextIndex and matchIndex for all peers)
// - Initializes and arms the leader-only heartbeat ticker (50ms interval)
func (s *Server) becomeLeaderLocked() {
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

	// Arm leader-only heartbeat ticker (every 50ms)
	if s.heartbeatTicker != nil {
		s.heartbeatTicker.Stop()
	}
	s.heartbeatTicker = time.NewTicker(s.heartbeatInterval)

	if s.heartbeatTimer != nil {
		s.heartbeatTimer.Stop()
	}
	s.heartbeatTimer = time.NewTimer(s.heartbeatInterval)

	var curTerm uint64
	if s.PersistentState != nil {
		curTerm = uint64(s.PersistentState.CurrentTerm)
	}

	s.GetLogger().Info("transitioned to Leader (started leadership; heartbeat ticker armed; will call AppendEntries)",
		"node_id", int(s.ID),
		"previous_state", oldRole.String(),
		"term", curTerm,
		"peers_count", len(s.Peers),
		"heartbeat_interval_ms", s.heartbeatInterval.Milliseconds(),
	)
}

// handleElectionTimeout is called when the election timer fires.
// In Raft (§5.2):
// The node transitions to Candidate, increments currentTerm, votes for itself,
// and broadcasts RequestVote RPCs in parallel to all peers using goroutines + results channel.
func (s *Server) handleElectionTimeout(t time.Time) {
	s.GetLogger().Info("election timeout fired", "node_id", int(s.ID), "state", s.State.String(), "time", t)

	if s.State == raft.Leader {
		return
	}

	s.startElectionLocked()
}

// StartElection initiates an election cycle on the server.
// If the event loop is running, the election is queued onto the event loop.
func (s *Server) StartElection() {
	if s.loopRunning.Load() {
		resCh := make(chan struct{}, 1)
		select {
		case s.stateActionCh <- stateAction{
			fn: func() {
				s.startElectionLocked()
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
	s.startElectionLocked()
}

// startElectionLocked begins an election cycle:
// 1. Transitions to Candidate, increments term, votes for self, persists state, resets timer (§5.2)
// 2. Sends RequestVote RPCs in parallel to all peers using goroutines + results channel
func (s *Server) startElectionLocked() {
	s.becomeCandidateLocked()
	s.broadcastRequestVotesLocked()
}

// broadcastRequestVotesLocked sends RequestVote RPCs in parallel to all peers using goroutines,
// streaming results back onto s.voteResultCh.
func (s *Server) broadcastRequestVotesLocked() {
	if s.State != raft.Candidate || len(s.Peers) == 0 {
		return
	}

	term := s.PersistentState.CurrentTerm
	var lastLogIndex raft.LogIndex
	var lastLogTerm raft.Term
	if s.PersistentState != nil && len(s.PersistentState.Log) > 0 {
		lastEntry := s.PersistentState.Log[len(s.PersistentState.Log)-1]
		lastLogIndex = lastEntry.Index
		lastLogTerm = lastEntry.Term
	}

	timeout := s.RPCTimeout()
	id := s.ID

	s.GetLogger().Info("broadcasting RequestVote RPCs in parallel to all peers",
		"node_id", int(id),
		"term", uint64(term),
		"peers_count", len(s.Peers),
		"timeout_ms", timeout.Milliseconds(),
	)

	for _, peer := range s.Peers {
		if peer.ID == id {
			continue
		}

		go func(p Peer) {
			ctx, cancel := context.WithTimeout(context.Background(), timeout)
			defer cancel()

			stopDone := make(chan struct{})
			go func() {
				select {
				case <-s.stopCh:
					cancel()
				case <-stopDone:
				}
			}()
			defer close(stopDone)

			reply, err := s.SendRequestVote(ctx, p, &raft.RequestVoteArgs{
				Term:         term,
				CandidateID:  id,
				LastLogIndex: lastLogIndex,
				LastLogTerm:  lastLogTerm,
			})

			select {
			case s.voteResultCh <- RequestVoteResult{
				PeerID:       p.ID,
				ElectionTerm: term,
				Reply:        reply,
				Err:          err,
			}:
			case <-s.stopCh:
			}
		}(peer)
	}
}

// SendRequestVotesParallel sends RequestVote RPCs in parallel to all provided peers using goroutines,
// streaming results back onto the provided results channel.
func (s *Server) SendRequestVotesParallel(ctx context.Context, term raft.Term, peers []Peer, resCh chan<- RequestVoteResult) {
	var lastLogIndex raft.LogIndex
	var lastLogTerm raft.Term
	s.mu.RLock()
	id := s.ID
	if s.PersistentState != nil && len(s.PersistentState.Log) > 0 {
		lastEntry := s.PersistentState.Log[len(s.PersistentState.Log)-1]
		lastLogIndex = lastEntry.Index
		lastLogTerm = lastEntry.Term
	}
	timeout := s.RPCTimeout()
	s.mu.RUnlock()

	for _, peer := range peers {
		if peer.ID == id {
			continue
		}

		go func(p Peer) {
			rpcCtx, cancel := context.WithTimeout(ctx, timeout)
			defer cancel()

			reply, err := s.SendRequestVote(rpcCtx, p, &raft.RequestVoteArgs{
				Term:         term,
				CandidateID:  id,
				LastLogIndex: lastLogIndex,
				LastLogTerm:  lastLogTerm,
			})

			select {
			case resCh <- RequestVoteResult{
				PeerID:       p.ID,
				ElectionTerm: term,
				Reply:        reply,
				Err:          err,
			}:
			case <-ctx.Done():
			case <-s.stopCh:
			}
		}(peer)
	}
}

// HandleVoteResult processes a vote result (steps down on higher term, or transitions to Leader on majority).
func (s *Server) HandleVoteResult(res RequestVoteResult) {
	if s.loopRunning.Load() {
		select {
		case s.voteResultCh <- res:
		case <-s.stopCh:
		}
		return
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	s.handleVoteResultLocked(res)
}

// handleVoteResultLocked processes an incoming vote result from a peer.
// In Raft (§5.1 & §5.2):
// - If reply contains higher term: step down to Follower immediately
// - If vote granted: count vote; on majority, transition to Leader
func (s *Server) handleVoteResultLocked(res RequestVoteResult) {
	if res.Err != nil {
		s.GetLogger().Debug("RequestVote RPC to peer failed or timed out",
			"peer_id", int(res.PeerID),
			"error", res.Err,
		)
		return
	}
	if res.Reply == nil {
		return
	}

	var currentTerm uint64
	if s.PersistentState != nil {
		currentTerm = uint64(s.PersistentState.CurrentTerm)
	}

	// 1. If RPC response contains term T > currentTerm:
	// set currentTerm = T, convert to follower (§5.1)
	if uint64(res.Reply.Term) > currentTerm {
		s.GetLogger().Info("discovered higher term in RequestVote reply, stepping down to follower",
			"node_id", int(s.ID),
			"reply_term", uint64(res.Reply.Term),
			"current_term", currentTerm,
			"peer_id", int(res.PeerID),
		)
		s.becomeFollowerLocked(res.Reply.Term)
		return
	}

	// 2. Ignore votes if no longer Candidate or if reply is from an older election term
	if s.State != raft.Candidate || res.ElectionTerm != raft.Term(currentTerm) {
		return
	}

	// 3. Count vote if granted
	if res.Reply.VoteGranted {
		if s.votesReceived == nil {
			s.votesReceived = make(map[raft.NodeID]bool)
			s.votesReceived[s.ID] = true
		}
		s.votesReceived[res.PeerID] = true
		votesCount := len(s.votesReceived)
		totalNodes := len(s.Peers) + 1
		majority := (totalNodes / 2) + 1

		s.GetLogger().Info("received granted vote from peer",
			"node_id", int(s.ID),
			"peer_id", int(res.PeerID),
			"term", currentTerm,
			"votes_count", votesCount,
			"majority_required", majority,
		)

		// 4. On majority, transition to Leader (§5.2)
		if votesCount >= majority {
			s.GetLogger().Info("achieved majority votes, transitioning to Leader",
				"node_id", int(s.ID),
				"term", currentTerm,
				"votes_count", votesCount,
				"total_nodes", totalNodes,
				"majority", majority,
			)
			s.becomeLeaderLocked()

			// Immediately broadcast heartbeats to establish authority (§5.2)
			s.broadcastHeartbeats()
		}
	} else {
		s.GetLogger().Info("vote denied by peer",
			"node_id", int(s.ID),
			"peer_id", int(res.PeerID),
			"term", currentTerm,
			"reply_term", res.Reply.Term,
		)
	}
}

// handleHeartbeatTick is called on each tick of the leader-only heartbeat ticker (every 50ms).
// In Raft (§5.2), leaders periodically send heartbeats (AppendEntries with no log entries)
// to all peers to maintain authority and prevent election timeouts.
func (s *Server) handleHeartbeatTick() {
	if s.State != raft.Leader {
		return
	}
	s.heartbeatsSent.Add(1)

	var curTerm uint64
	if s.PersistentState != nil {
		curTerm = uint64(s.PersistentState.CurrentTerm)
	}
	s.GetLogger().Info("leader heartbeat ticker fired (will call AppendEntries)",
		"node_id", int(s.ID),
		"term", curTerm,
		"peers_count", len(s.Peers),
		"heartbeat_interval_ms", s.heartbeatInterval.Milliseconds(),
		"heartbeats_sent", s.heartbeatsSent.Load(),
	)

	if s.heartbeatTimer != nil {
		s.heartbeatTimer.Reset(s.heartbeatInterval)
	}

	// Stub: will dispatch AppendEntries RPC to all cluster peers
	s.broadcastHeartbeats()
}

// broadcastHeartbeats sends AppendEntries heartbeats to all cluster peers.
// Currently stubbed to log 'would send heartbeat to peer X' until log replication and peer RPC dispatch are wired.
func (s *Server) broadcastHeartbeats() {
	if s.State != raft.Leader {
		return
	}
	for _, peer := range s.Peers {
		if peer.ID == s.ID {
			continue
		}
		s.GetLogger().Info(fmt.Sprintf("would send heartbeat to peer %d", peer.ID),
			"leader_id", int(s.ID),
			"peer_id", int(peer.ID),
			"peer_addr", peer.Address,
		)
	}
}

// handleHeartbeatTimeout is an alias for handleHeartbeatTick for backward compatibility.
func (s *Server) handleHeartbeatTimeout() {
	s.handleHeartbeatTick()
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
		if s.State == raft.Leader {
			if s.heartbeatTicker != nil {
				heartbeatCh = s.heartbeatTicker.C
			} else if s.heartbeatTimer != nil {
				heartbeatCh = s.heartbeatTimer.C
			}
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

		case res := <-s.voteResultCh:
			s.mu.Lock()
			s.handleVoteResultLocked(res)
			s.mu.Unlock()

		case <-heartbeatCh:
			s.mu.Lock()
			s.handleHeartbeatTick()
			s.mu.Unlock()

		case ev := <-s.requestVoteCh:
			s.mu.Lock()
			reply := s.processRequestVote(ev.Args)
			s.mu.Unlock()
			if ev.ResCh != nil {
				select {
				case ev.ResCh <- reply:
				case <-s.stopCh:
					return
				}
			}

		case ev := <-s.appendEntriesCh:
			s.mu.Lock()
			reply := s.processAppendEntries(ev.Args)
			s.mu.Unlock()
			if ev.ResCh != nil {
				select {
				case ev.ResCh <- reply:
				case <-s.stopCh:
					return
				}
			}

		case act := <-s.stateActionCh:
			s.mu.Lock()
			act.fn()
			s.mu.Unlock()
			if act.resCh != nil {
				select {
				case act.resCh <- struct{}{}:
				case <-s.stopCh:
					return
				}
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

// checkReplyTerm checks if an RPC response contains term T > currentTerm.
// In Raft (§5.1, All Servers): set currentTerm = T, convert to follower, reset votedFor to -1, and persist.
func (s *Server) checkReplyTerm(replyTerm raft.Term) {
	var curTerm raft.Term
	s.mu.RLock()
	if s.PersistentState != nil {
		curTerm = s.PersistentState.CurrentTerm
	}
	s.mu.RUnlock()

	if replyTerm > curTerm {
		s.becomeFollower(replyTerm)
	}
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


