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

// ServerCounters holds runtime consensus event counters for a Raft node.
type ServerCounters struct {
	ElectionsStarted uint64 `json:"electionsStarted"`
	VotesGranted     uint64 `json:"votesGranted"`
	VotesDenied      uint64 `json:"votesDenied"`
	HeartbeatsSent   uint64 `json:"heartbeatsSent"`
}

// NodeStatus represents the runtime status of a Raft node exposed via the debug HTTP endpoint (/status).
type NodeStatus struct {
	ID                  raft.NodeID    `json:"id"`
	Term                raft.Term      `json:"term"`
	State               string         `json:"state"`
	VotedFor            raft.NodeID    `json:"votedFor"`
	VotedForAlt         raft.NodeID    `json:"voted_for"`
	CommitIndex         raft.LogIndex  `json:"commitIndex"`
	LastApplied         raft.LogIndex  `json:"lastApplied"`
	LogLength           int            `json:"logLength"`
	ElectionsStarted    uint64         `json:"electionsStarted"`
	ElectionsStartedAlt uint64         `json:"elections_started"`
	VotesGranted        uint64         `json:"votesGranted"`
	VotesGrantedAlt     uint64         `json:"votes_granted"`
	VotesDenied         uint64         `json:"votesDenied"`
	VotesDeniedAlt      uint64         `json:"votes_denied"`
	HeartbeatsSent      uint64         `json:"heartbeatsSent"`
	HeartbeatsSentAlt   uint64         `json:"heartbeats_sent"`
	Counters            ServerCounters `json:"counters"`
	RecentEvents        []Event        `json:"recentEvents,omitempty"`
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

// TimeoutNowEvent packages an incoming TimeoutNow RPC with a response channel.
type TimeoutNowEvent struct {
	Args  *raftpb.TimeoutNowArgs
	ResCh chan<- *raftpb.TimeoutNowReply
}

// RequestVoteResult represents the outcome of an asynchronous RequestVote RPC sent to a peer.
type RequestVoteResult struct {
	PeerID       raft.NodeID
	ElectionTerm raft.Term
	PreVote      bool
	Reply        *raft.RequestVoteReply
	Err          error
}

// HeartbeatResult represents the outcome of an asynchronous AppendEntries heartbeat RPC sent to a peer.
type HeartbeatResult struct {
	PeerID raft.NodeID
	Term   raft.Term
	Reply  *raft.AppendEntriesReply
	Err    error
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

	// Log abstracts the replicated consensus log (1-based indexing, sentinel at index 0).
	Log raft.Log `json:"-"`

	// Storage provides persistence for Raft state (currentTerm, votedFor, log).
	// Currently initialized with an in-memory Storage implementation, to be replaced in Month 4.
	Storage store.Storage `json:"-"`

	// State represents the current consensus role (Follower, Candidate, Leader).
	State raft.RaftState `json:"state"`

	// Volatile consensus state
	VolatileState       *raft.VolatileState       `json:"volatile_state"`
	VolatileStateLeader *raft.VolatileStateLeader `json:"volatile_state_leader,omitempty"`

	// ElectionTimer manages the randomized election timeout backed by Clock and Timer interfaces.
	ElectionTimer *raft.ElectionTimer `json:"-"`

	// Clock abstracts time operations for the server and timers.
	clock raft.Clock

	// Leader-only heartbeat ticker (fires every heartbeatInterval, e.g. 50ms)
	heartbeatTicker   raft.Ticker
	heartbeatTimer    raft.Timer
	heartbeatInterval time.Duration
	heartbeatsSent    atomic.Uint64

	// Candidate vote tracking
	votesReceived map[raft.NodeID]bool

	// RPCClientPool manages gRPC client connections to peers.
	RPCClientPool *rpc.ClientPool `json:"-"`

	// Logger is the structured logger for this node with [Node <ID>] prefix on every line.
	Logger *slog.Logger `json:"-"`

	lastHeartbeat       time.Time
	lastLeaderHeartbeat time.Time

	// Concurrency channels for the single event loop goroutine:
	stopCh            chan struct{}
	stopOnce          sync.Once
	requestVoteCh     chan RequestVoteEvent
	appendEntriesCh   chan AppendEntriesEvent
	timeoutNowCh      chan TimeoutNowEvent
	voteResultCh      chan RequestVoteResult
	heartbeatResultCh chan HeartbeatResult
	stateActionCh     chan stateAction
	loopDone          chan struct{}
	loopRunning       atomic.Bool

	grpcServer *grpc.Server
	listener   net.Listener

	httpServer   *http.Server
	httpListener net.Listener

	// logUpToDateChecker is an optional testing hook for log up-to-date assertions.
	logUpToDateChecker func(lastLogIndex uint64, lastLogTerm uint64) bool

	// requestVoteFilter is an optional testing hook to intercept/filter RequestVote RPCs.
	requestVoteFilter func(req *raftpb.RequestVoteArgs) bool

	// electionsStarted counts the total number of elections initiated by this node.
	electionsStarted atomic.Uint64

	// votesGranted counts the total number of votes granted by this node.
	votesGranted atomic.Uint64

	// votesDenied counts the total number of votes denied by this node.
	votesDenied atomic.Uint64

	// eventLog records consensus events into an in-memory ring buffer for post-mortem debugging.
	eventLog *EventLog

	// heartbeatsReceived counts the total number of valid heartbeats received by this node.
	heartbeatsReceived uint64

	// pause controls for mid-test process / goroutine pausing
	paused        atomic.Bool
	resumeCh      chan struct{}
	pauseNotifyCh chan struct{}
	pauseMu       sync.Mutex

	// checkQuorum controls (§6.2): leader tracks last-heard-from time per follower
	checkQuorum         bool
	checkQuorumInterval time.Duration
	checkQuorumTimer    raft.Timer
	heartbeatAcks       map[raft.NodeID]bool
	lastHeardFrom       map[raft.NodeID]time.Time

	// preVote controls (§9.6): candidates gather majority pre-votes before incrementing term
	preVote          bool
	preVotesReceived map[raft.NodeID]bool

	// stickiness controls (§9.6): followers reject (pre)votes if heard from current leader within minTimeout
	stickiness bool
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
	timeoutNowCh := make(chan TimeoutNowEvent, 64)
	voteResultCh := make(chan RequestVoteResult, 64)
	heartbeatResultCh := make(chan HeartbeatResult, 64)
	stateActionCh := make(chan stateAction, 64)
	loopDone := make(chan struct{})
	pauseNotifyCh := make(chan struct{}, 1)

	memStorage := store.NewMemoryStorage()
	if persistentState != nil {
		_ = memStorage.Persist(persistentState)
	}

	raftLog := raft.NewMemoryLog()
	if persistentState != nil && len(persistentState.Log) > 0 {
		_ = raftLog.Append(persistentState.Log...)
	}

	clk := raft.NewRealClock()
	srv := &Server{
		ID:                id,
		clock:             clk,
		Peers:             peers,
		PersistentState:   persistentState,
		Log:               raftLog,
		Storage:           memStorage,
		State:             raft.Follower,
		VolatileState:     &raft.VolatileState{CommitIndex: 0, LastApplied: 0},
		ElectionTimer:     raft.NewElectionTimerWithClock(clk, raft.DefaultMinElectionTimeout, raft.DefaultMaxElectionTimeout),
		RPCClientPool:     rpcClientPool,
		Logger:            nodeLogger,
		lastHeartbeat:     clk.Now(),
		heartbeatInterval: raft.DefaultHeartbeatInterval,
		stickiness:        true,
		votesReceived:     make(map[raft.NodeID]bool),
		preVotesReceived:  make(map[raft.NodeID]bool),
		stopCh:            stopCh,
		requestVoteCh:     requestVoteCh,
		appendEntriesCh:   appendEntriesCh,
		timeoutNowCh:      timeoutNowCh,
		voteResultCh:      voteResultCh,
		heartbeatResultCh: heartbeatResultCh,
		stateActionCh:     stateActionCh,
		loopDone:          loopDone,
		pauseNotifyCh:     pauseNotifyCh,
		heartbeatAcks:     make(map[raft.NodeID]bool),
		lastHeardFrom:     make(map[raft.NodeID]time.Time),
		eventLog:          NewEventLog(DefaultEventLogCapacity),
	}
	srv.recordEventLocked(EventServerStarted, fmt.Sprintf("server node %d initialized", id), map[string]interface{}{
		"node_id": int(id),
	})
	return srv
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

// LeaderTerm returns (term, true) if the node is currently in Leader state,
// captured atomically under a single s.mu.RLock(). Returns (0, false) otherwise.
func (s *Server) LeaderTerm() (raft.Term, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if s.State == raft.Leader && s.PersistentState != nil {
		return s.PersistentState.CurrentTerm, true
	}
	return 0, false
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

// HeartbeatResultChannel returns the internal heartbeat result channel used by the event loop.
func (s *Server) HeartbeatResultChannel() chan HeartbeatResult {
	return s.heartbeatResultCh
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

func (s *Server) getClockLocked() raft.Clock {
	if s.clock == nil {
		s.clock = raft.NewRealClock()
	}
	return s.clock
}

// Clock returns the Clock used by the server.
func (s *Server) Clock() raft.Clock {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.getClockLocked()
}

// SetClock configures a custom Clock interface for the server and election timer.
func (s *Server) SetClock(clk raft.Clock) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if clk == nil {
		clk = raft.NewRealClock()
	}
	s.clock = clk
	s.lastHeartbeat = clk.Now()
	if s.ElectionTimer != nil {
		s.ElectionTimer.SetClock(clk)
	}
}

// SetRandFn configures a custom random duration generator on the election timer.
func (s *Server) SetRandFn(fn func(min, max time.Duration) time.Duration) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.ElectionTimer != nil {
		s.ElectionTimer.SetRandFn(fn)
	}
}

// GetElectionTimer returns the server's election timer.
func (s *Server) GetElectionTimer() *raft.ElectionTimer {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.ElectionTimer
}

// GetHeartbeatTicker returns the leader's heartbeat ticker, or nil if not running.
func (s *Server) GetHeartbeatTicker() raft.Ticker {
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

// SetCheckQuorum enables or disables the check-quorum mechanism (§6.2).
// When enabled, a leader that cannot contact a majority of cluster nodes
// within checkQuorumInterval steps down to Follower.
func (s *Server) SetCheckQuorum(enabled bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.checkQuorum = enabled
	if enabled && s.State == raft.Leader {
		s.resetCheckQuorumTimerLocked()
	} else if !enabled && s.checkQuorumTimer != nil {
		s.checkQuorumTimer.Stop()
		s.checkQuorumTimer = nil
	}
}

// CheckQuorum returns whether check-quorum is enabled.
func (s *Server) CheckQuorum() bool {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.checkQuorum
}

// SetCheckQuorumInterval configures the timeout interval for quorum verification.
func (s *Server) SetCheckQuorumInterval(interval time.Duration) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.checkQuorumInterval = interval
	if s.checkQuorum && s.State == raft.Leader && s.checkQuorumTimer != nil {
		s.checkQuorumTimer.Reset(interval)
	}
}

func (s *Server) resetCheckQuorumTimerLocked() {
	if s.checkQuorumTimer != nil {
		s.checkQuorumTimer.Stop()
	}
	interval := s.checkQuorumInterval
	if interval <= 0 {
		if s.ElectionTimer != nil {
			minT, _ := s.ElectionTimer.Range()
			interval = minT
		} else {
			interval = raft.DefaultMinElectionTimeout
		}
	}
	s.checkQuorumTimer = s.getClockLocked().NewTimer(interval)
}

// LastHeardFrom returns a copy of the map tracking the leader's last contact time for each follower.
func (s *Server) LastHeardFrom() map[raft.NodeID]time.Time {
	s.mu.RLock()
	defer s.mu.RUnlock()
	res := make(map[raft.NodeID]time.Time, len(s.lastHeardFrom))
	for k, v := range s.lastHeardFrom {
		res[k] = v
	}
	return res
}

func (s *Server) handleCheckQuorumTimeoutLocked() {
	if s.State != raft.Leader || !s.checkQuorum {
		return
	}

	totalNodes := len(s.Peers) + 1
	majority := (totalNodes / 2) + 1

	electionTimeout := s.checkQuorumInterval
	if electionTimeout <= 0 {
		if s.ElectionTimer != nil {
			minT, _ := s.ElectionTimer.Range()
			electionTimeout = minT
		} else {
			electionTimeout = raft.DefaultMinElectionTimeout
		}
	}

	now := s.getClockLocked().Now()
	activeCount := 1 // Self counts as 1
	for _, peer := range s.Peers {
		if lastHeard, ok := s.lastHeardFrom[peer.ID]; ok {
			if now.Sub(lastHeard) <= electionTimeout {
				activeCount++
			}
		}
	}

	curTerm := uint64(0)
	if s.PersistentState != nil {
		curTerm = uint64(s.PersistentState.CurrentTerm)
	}

	if activeCount < majority {
		s.recordEventLocked(EventQuorumLoss, fmt.Sprintf("leader lost quorum contact (%d/%d active, majority %d within %v); stepping down to Follower", activeCount, totalNodes, majority, electionTimeout), map[string]interface{}{
			"active_nodes":      activeCount,
			"majority_required": majority,
			"total_nodes":       totalNodes,
			"term":              curTerm,
		})
		s.GetLogger().Info("leader lost contact with majority within election timeout, stepping down to Follower (§6.2)",
			"node_id", int(s.ID),
			"term", curTerm,
			"active_nodes", activeCount,
			"majority_required", majority,
			"total_nodes", totalNodes,
			"election_timeout", electionTimeout,
		)
		s.becomeFollowerLocked(raft.Term(curTerm))
		return
	}

	s.heartbeatAcks = make(map[raft.NodeID]bool)
	s.resetCheckQuorumTimerLocked()
}

// HeartbeatsSent returns the total number of leader heartbeat ticks dispatched.
func (s *Server) HeartbeatsSent() uint64 {
	return s.heartbeatsSent.Load()
}

// SetPreVote enables or disables the Pre-Vote algorithm (§9.6).
func (s *Server) SetPreVote(enabled bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.preVote = enabled
}

// PreVote returns whether the Pre-Vote algorithm is enabled.
func (s *Server) PreVote() bool {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.preVote
}

// SetStickiness enables or disables leader stickiness / disruptive server protection (§9.6).
func (s *Server) SetStickiness(enabled bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.stickiness = enabled
}

// Stickiness returns whether leader stickiness (§9.6) is enabled.
func (s *Server) Stickiness() bool {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.stickiness
}

// PreVotesReceived returns a copy of recorded pre-votes.
func (s *Server) PreVotesReceived() map[raft.NodeID]bool {
	s.mu.RLock()
	defer s.mu.RUnlock()
	res := make(map[raft.NodeID]bool, len(s.preVotesReceived))
	for k, v := range s.preVotesReceived {
		res[k] = v
	}
	return res
}

func (s *Server) hasActiveLeaderLocked() bool {
	if !s.stickiness {
		return false
	}
	if s.State != raft.Follower {
		return false
	}
	if s.heartbeatsReceived == 0 || s.ElectionTimer == nil || s.lastLeaderHeartbeat.IsZero() {
		return false
	}
	minTimeout, _ := s.ElectionTimer.Range()
	return s.getClockLocked().Now().Sub(s.lastLeaderHeartbeat) < minTimeout
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
					s.GetLogger().Debug("reset election timeout", "timeout_ms", d.Milliseconds())
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
	s.GetLogger().Debug("reset election timeout", "timeout_ms", d.Milliseconds())
	return d
}

func (s *Server) handleHeartbeatViaLoop(leaderID raft.NodeID, term raft.Term) (bool, time.Duration) {
	resCh := make(chan *raftpb.AppendEntriesReply, 1)
	args := &raftpb.AppendEntriesArgs{LeaderId: int64(leaderID), Term: uint64(term)}
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

func (s *Server) handleHeartbeatDirectLocked(leaderID raft.NodeID, term raft.Term) (bool, time.Duration) {
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
		s.votesReceived = make(map[raft.NodeID]bool)
		s.GetLogger().Info("reverted from Candidate to Follower upon valid heartbeat",
			"leader_id", int(leaderID),
			"term", uint64(term),
		)
	}

	s.heartbeatsReceived++
	s.lastHeartbeat = s.getClockLocked().Now()
	s.lastLeaderHeartbeat = s.lastHeartbeat
	var newTimeout time.Duration
	if s.ElectionTimer != nil {
		newTimeout = s.ElectionTimer.Reset()
		s.GetLogger().Debug("reset election timer on valid heartbeat",
			"leader_id", int(leaderID),
			"term", uint64(term),
			"timeout_ms", newTimeout.Milliseconds(),
		)
	}
	return true, newTimeout
}

// HandleHeartbeat processes a heartbeat event from a leader.
// When the event loop is active, the heartbeat is routed through the single event loop.
func (s *Server) HandleHeartbeat(leaderID raft.NodeID, term raft.Term) (bool, time.Duration) {
	if s.loopRunning.Load() {
		return s.handleHeartbeatViaLoop(leaderID, term)
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.handleHeartbeatDirectLocked(leaderID, term)
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

func (s *Server) getPeerLocked(id raft.NodeID) (Peer, bool) {
	for _, p := range s.Peers {
		if p.ID == id {
			return p, true
		}
	}
	return Peer{}, false
}

// GetPeer retrieves peer information for a specific node ID.
func (s *Server) GetPeer(id raft.NodeID) (Peer, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.getPeerLocked(id)
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

func (s *Server) stateSnapshotLocked() (raft.Term, raft.NodeID, int, raft.LogIndex, raft.LogIndex) {
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
	return term, votedFor, logLen, commitIndex, lastApplied
}

// Status returns the current runtime status of the Raft node (term, state, votedFor, counters, etc.).
func (s *Server) Status() NodeStatus {
	s.mu.RLock()
	defer s.mu.RUnlock()

	term, votedFor, logLen, commitIndex, lastApplied := s.stateSnapshotLocked()
	counters := s.Counters()
	var recentEvents []Event
	if s.eventLog != nil {
		recentEvents = s.eventLog.Recent(20)
	}

	return NodeStatus{
		ID:                  s.ID,
		Term:                term,
		State:               s.State.String(),
		VotedFor:            votedFor,
		VotedForAlt:         votedFor,
		CommitIndex:         commitIndex,
		LastApplied:         lastApplied,
		LogLength:           logLen,
		ElectionsStarted:    counters.ElectionsStarted,
		ElectionsStartedAlt: counters.ElectionsStarted,
		VotesGranted:        counters.VotesGranted,
		VotesGrantedAlt:     counters.VotesGranted,
		VotesDenied:         counters.VotesDenied,
		VotesDeniedAlt:      counters.VotesDenied,
		HeartbeatsSent:      counters.HeartbeatsSent,
		HeartbeatsSentAlt:   counters.HeartbeatsSent,
		Counters:            counters,
		RecentEvents:        recentEvents,
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

// HandleEvents serves the /events debug endpoint returning all recorded consensus events.
func (s *Server) HandleEvents(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet && r.Method != http.MethodHead {
		http.Error(w, "Method Not Allowed", http.StatusMethodNotAllowed)
		return
	}

	var events []Event
	if s.eventLog != nil {
		events = s.eventLog.Entries()
	}
	if events == nil {
		events = []Event{}
	}

	w.Header().Set("Content-Type", "application/json")
	encoder := json.NewEncoder(w)
	encoder.SetIndent("", "  ")
	if err := encoder.Encode(events); err != nil {
		s.GetLogger().Error("failed to encode events response", "error", err)
	}
}

// HTTPHandler returns an http.Handler multiplexer configured with the /status and /events endpoints.
func (s *Server) HTTPHandler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("/status", s.HandleStatus)
	mux.HandleFunc("/events", s.HandleEvents)
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

func (s *Server) buildGRPCServer(opts ...grpc.ServerOption) *grpc.Server {
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
	return grpcServer
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

	grpcServer := s.buildGRPCServer(opts...)
	s.grpcServer = grpcServer
	s.listener = lis
	addrStr := lis.Addr().String()
	s.mu.Unlock()

	s.GetLogger().Info("gRPC server listening", "address", addrStr)
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
	if s.checkQuorumTimer != nil {
		s.checkQuorumTimer.Stop()
		s.checkQuorumTimer = nil
	}
	s.heartbeatAcks = make(map[raft.NodeID]bool)
	s.lastHeardFrom = make(map[raft.NodeID]time.Time)
}

func (s *Server) shutdownEventLoop() {
	s.stopOnce.Do(func() {
		close(s.stopCh)
	})
	if s.loopRunning.Load() {
		select {
		case <-s.loopDone:
		case <-time.After(1 * time.Second):
		}
	}
}

func (s *Server) cleanupServerLocked(graceful bool) {
	s.recordEventLocked(EventServerStopped, fmt.Sprintf("server node %d stopped", s.ID), map[string]interface{}{
		"node_id": int(s.ID),
	})
	if s.ElectionTimer != nil {
		s.ElectionTimer.Stop()
	}
	s.stopHeartbeatTickerLocked()

	if s.grpcServer != nil {
		if graceful {
			s.grpcServer.GracefulStop()
			s.GetLogger().Info("gracefully stopped gRPC server")
		} else {
			s.grpcServer.Stop()
			s.GetLogger().Info("stopped gRPC server")
		}
		s.grpcServer = nil
	}
	if s.listener != nil {
		_ = s.listener.Close()
		s.listener = nil
	}
	if s.httpServer != nil {
		if graceful {
			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			_ = s.httpServer.Shutdown(ctx)
			cancel()
			s.GetLogger().Info("gracefully stopped debug HTTP server")
		} else {
			_ = s.httpServer.Close()
			s.GetLogger().Info("stopped debug HTTP server")
		}
		s.httpServer = nil
	}
	if s.httpListener != nil {
		_ = s.httpListener.Close()
		s.httpListener = nil
	}
}

// Stop immediately stops the gRPC server, debug HTTP server, and main event loop.
func (s *Server) Stop() {
	s.shutdownEventLoop()
	s.mu.Lock()
	defer s.mu.Unlock()
	s.cleanupServerLocked(false)
}

// GracefulStop gracefully stops the gRPC server, debug HTTP server, and closes listeners.
func (s *Server) GracefulStop() {
	s.shutdownEventLoop()
	s.mu.Lock()
	defer s.mu.Unlock()
	s.cleanupServerLocked(true)
}

// Kill simulates a hard crash of the node process mid-test.
// It abruptly closes the server, terminates its listeners and event loop,
// and shuts down client connections without graceful cleanup.
func (s *Server) Kill() {
	s.GetLogger().Info("killed node process mid-test (simulating crash)", "node_id", int(s.ID))
	_ = s.Close()
}

// IsKilled reports whether the node has been killed or stopped.
func (s *Server) IsKilled() bool {
	select {
	case <-s.stopCh:
		return true
	default:
		return false
	}
}

// Pause pauses the node's event loop and message processing, simulating a process freeze,
// SIGSTOP, or long GC pause mid-test.
func (s *Server) Pause() {
	s.pauseMu.Lock()
	defer s.pauseMu.Unlock()
	if s.paused.Load() {
		return
	}
	s.resumeCh = make(chan struct{})
	s.paused.Store(true)

	// Wake up event loop if blocked in select so it enters paused state immediately
	select {
	case s.pauseNotifyCh <- struct{}{}:
	default:
	}

	s.GetLogger().Info("paused node event loop mid-test", "node_id", int(s.ID))
}

// Resume resumes a previously paused node's event loop and message processing.
func (s *Server) Resume() {
	s.pauseMu.Lock()
	defer s.pauseMu.Unlock()
	if !s.paused.Load() {
		return
	}
	s.paused.Store(false)
	if s.resumeCh != nil {
		close(s.resumeCh)
	}

	s.GetLogger().Info("resumed node event loop mid-test", "node_id", int(s.ID))
}

// IsPaused returns true if the node is currently paused.
func (s *Server) IsPaused() bool {
	return s.paused.Load()
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

// TimeoutNow handles an incoming TimeoutNow RPC from the current leader (§3.10).
// It instructs the server to start an election immediately, bypassing leader stickiness.
func (s *Server) TimeoutNow(ctx context.Context, req *raftpb.TimeoutNowArgs) (*raftpb.TimeoutNowReply, error) {
	if s.loopRunning.Load() {
		resCh := make(chan *raftpb.TimeoutNowReply, 1)
		select {
		case s.timeoutNowCh <- TimeoutNowEvent{Args: req, ResCh: resCh}:
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
	return s.processTimeoutNow(req), nil
}

// LastLogInfo returns the (index, term) of the receiver's last log entry.
// If the log is empty or state is uninitialized, it returns (0, 0).
func (s *Server) LastLogInfo() (raft.LogIndex, raft.Term) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.lastLogInfoLocked()
}

func (s *Server) lastLogInfoLocked() (raft.LogIndex, raft.Term) {
	if s.Log != nil {
		if s.PersistentState != nil && len(s.PersistentState.Log) > 0 && s.Log.LastIndex() == 0 {
			_ = s.Log.Append(s.PersistentState.Log...)
		}
		return s.Log.LastIndex(), s.Log.LastTerm()
	}
	if s.PersistentState != nil {
		return s.PersistentState.LastLogInfo()
	}
	return 0, 0
}

// GetLog returns the server's consensus log.
func (s *Server) GetLog() raft.Log {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.Log
}

// SetLog configures a custom consensus log for the server.
func (s *Server) SetLog(l raft.Log) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.Log = l
}

// IsLogUpToDate checks whether a candidate's log is at least as up-to-date as this receiver's log (§5.4).
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
	myLastIndex, myLastTerm := s.lastLogInfoLocked()
	var upToDate bool
	if s.Log != nil {
		upToDate = s.Log.IsUpToDate(raft.LogIndex(lastLogIndex), raft.Term(lastLogTerm))
	} else {
		upToDate = raft.IsLogUpToDate(
			raft.LogIndex(lastLogIndex),
			raft.Term(lastLogTerm),
			myLastIndex,
			myLastTerm,
		)
	}
	s.GetLogger().Debug("evaluated candidate log up-to-date rule",
		"candidate_last_index", lastLogIndex,
		"candidate_last_term", lastLogTerm,
		"receiver_last_index", uint64(myLastIndex),
		"receiver_last_term", uint64(myLastTerm),
		"up_to_date", upToDate,
	)
	return upToDate
}

// SetLogUpToDateChecker sets an optional hook for testing log up-to-date evaluations.
func (s *Server) SetLogUpToDateChecker(fn func(lastLogIndex uint64, lastLogTerm uint64) bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.logUpToDateChecker = fn
}

// SetRequestVoteFilter sets an optional testing hook to filter or intercept incoming RequestVote RPCs.
func (s *Server) SetRequestVoteFilter(fn func(req *raftpb.RequestVoteArgs) bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.requestVoteFilter = fn
}

// ElectionsStarted returns the total number of elections initiated by this node.
func (s *Server) ElectionsStarted() uint64 {
	return s.electionsStarted.Load()
}

// VotesGranted returns the number of times this node has granted its vote to any candidate.
func (s *Server) VotesGranted() uint64 {
	return s.votesGranted.Load()
}

// VotesDenied returns the number of times this node has denied its vote to any candidate.
func (s *Server) VotesDenied() uint64 {
	return s.votesDenied.Load()
}

// Counters returns a snapshot of all Raft consensus event counters.
func (s *Server) Counters() ServerCounters {
	return ServerCounters{
		ElectionsStarted: s.ElectionsStarted(),
		VotesGranted:     s.VotesGranted(),
		VotesDenied:      s.VotesDenied(),
		HeartbeatsSent:   s.HeartbeatsSent(),
	}
}

// EventLog returns the node's in-memory event log ring buffer for post-mortem debugging.
func (s *Server) EventLog() *EventLog {
	return s.eventLog
}

// RecordEvent records a custom event into the node's event log ring buffer.
func (s *Server) RecordEvent(eventType EventType, message string, details map[string]interface{}) {
	s.mu.RLock()
	var term raft.Term
	if s.PersistentState != nil {
		term = s.PersistentState.CurrentTerm
	}
	s.mu.RUnlock()

	if s.eventLog != nil {
		s.eventLog.Append(eventType, term, message, details)
	}
}

func (s *Server) recordEventLocked(eventType EventType, message string, details map[string]interface{}) {
	if s.eventLog == nil {
		return
	}
	var term raft.Term
	if s.PersistentState != nil {
		term = s.PersistentState.CurrentTerm
	}
	s.eventLog.Append(eventType, term, message, details)
}

// HeartbeatsReceived returns the number of valid empty AppendEntries (heartbeats) received.
func (s *Server) HeartbeatsReceived() uint64 {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.heartbeatsReceived
}

// resetElectionTimerOnGrantingVoteLocked resets the follower's election timer
// upon granting a vote to a candidate, as required by Raft paper §5.2 / Figure 2:
// "If election timeout elapses without receiving AppendEntries RPC from current leader
// or granting vote to candidate: convert to candidate".
//
// Resetting the election timer when granting a vote gives the candidate a full election
// timeout window to collect majority votes and establish leadership (by sending heartbeats),
// significantly reducing the likelihood of split votes.
func (s *Server) resetElectionTimerOnGrantingVoteLocked(candidateID int64, term uint64) time.Duration {
	s.lastHeartbeat = s.getClockLocked().Now()
	var newTimeout time.Duration
	if s.ElectionTimer != nil {
		newTimeout = s.ElectionTimer.Reset()
		s.GetLogger().Info("reset election timer on granting vote (§5.2)",
			"candidate_id", candidateID,
			"term", term,
			"timeout_ms", newTimeout.Milliseconds(),
		)
	}
	return newTimeout
}

func (s *Server) validateRequestVote(req *raftpb.RequestVoteArgs) (uint64, raft.NodeID, bool, *raftpb.RequestVoteReply) {
	var currentTerm uint64
	var votedFor raft.NodeID = -1
	if s.PersistentState != nil {
		currentTerm = uint64(s.PersistentState.CurrentTerm)
		votedFor = s.PersistentState.VotedFor
	}

	if s.requestVoteFilter != nil && !s.requestVoteFilter(req) {
		s.votesDenied.Add(1)
		s.recordEventLocked(EventVoteDenied, fmt.Sprintf("denied vote to candidate %d for term %d (filtered)", req.CandidateId, req.Term), map[string]interface{}{
			"candidate_id": req.CandidateId,
			"term":         req.Term,
			"reason":       "filter",
		})
		s.GetLogger().Warn("dropped/rejected RequestVote via test filter hook", "candidate_id", req.CandidateId, "term", req.Term)
		return currentTerm, votedFor, false, &raftpb.RequestVoteReply{Term: currentTerm, VoteGranted: false}
	}

	if req.Term < currentTerm {
		s.votesDenied.Add(1)
		s.recordEventLocked(EventVoteDenied, fmt.Sprintf("denied vote to candidate %d for term %d (stale term < %d)", req.CandidateId, req.Term, currentTerm), map[string]interface{}{
			"candidate_id": req.CandidateId,
			"req_term":     req.Term,
			"current_term": currentTerm,
			"reason":       "stale_term",
		})
		s.GetLogger().Warn("rejected RequestVote from candidate with stale term", "candidate_id", req.CandidateId, "req_term", req.Term, "current_term", currentTerm)
		return currentTerm, votedFor, false, &raftpb.RequestVoteReply{Term: currentTerm, VoteGranted: false}
	}
	return currentTerm, votedFor, true, nil
}

func (s *Server) stepDownOnHigherTermLocked(term uint64) (uint64, raft.NodeID, bool) {
	oldRole := s.State
	s.State = raft.Follower
	s.stopHeartbeatTickerLocked()
	s.votesReceived = make(map[raft.NodeID]bool)

	var currentTerm uint64
	var votedFor raft.NodeID = -1
	if s.PersistentState != nil {
		s.PersistentState.CurrentTerm = raft.Term(term)
		s.PersistentState.VotedFor = -1
		currentTerm = uint64(s.PersistentState.CurrentTerm)
	}

	s.recordEventLocked(EventStepDown, fmt.Sprintf("discovered higher term %d; transitioned to Follower", term), map[string]interface{}{
		"previous_state": oldRole.String(),
		"term":           currentTerm,
	})
	s.GetLogger().Info("transitioned to Follower on discovering higher term",
		"node_id", int(s.ID),
		"previous_state", oldRole.String(),
		"term", currentTerm,
	)
	return currentTerm, votedFor, true
}

func (s *Server) recordVoteGrantOrDenyLocked(req *raftpb.RequestVoteArgs, currentTerm uint64, votedFor raft.NodeID, steppedDown bool) bool {
	// Invariant: never grant votes to two different candidates in the same term (§5.2).
	if votedFor != -1 && votedFor != raft.NodeID(req.CandidateId) {
		s.votesDenied.Add(1)
		s.recordEventLocked(EventVoteDenied, fmt.Sprintf("denied vote to candidate %d for term %d (already voted for %d)", req.CandidateId, currentTerm, votedFor), map[string]interface{}{
			"candidate_id": req.CandidateId,
			"term":         currentTerm,
			"voted_for":    int(votedFor),
			"reason":       "already_voted_other",
		})
		s.GetLogger().Info("denied vote: already voted for another candidate in this term",
			"candidate_id", req.CandidateId,
			"term", currentTerm,
			"voted_for", int(votedFor),
		)
		return false
	}

	// Per Raft Figure 2 Rule 2: candidate's log must be at least as up-to-date as receiver's log.
	if !s.isLogUpToDateLocked(req.LastLogIndex, req.LastLogTerm) {
		s.votesDenied.Add(1)
		s.recordEventLocked(EventVoteDenied, fmt.Sprintf("denied vote to candidate %d for term %d (stale log)", req.CandidateId, currentTerm), map[string]interface{}{
			"candidate_id": req.CandidateId,
			"term":         currentTerm,
			"reason":       "stale_log",
		})
		if steppedDown {
			_ = s.persistLocked()
			if s.ElectionTimer != nil {
				s.ElectionTimer.Reset()
			}
		}
		s.GetLogger().Info("denied vote to candidate (stale log)", "candidate_id", req.CandidateId, "term", currentTerm)
		return false
	}

	// Idempotent retry: candidate already holds this node's vote for this term.
	if votedFor == raft.NodeID(req.CandidateId) {
		s.resetElectionTimerOnGrantingVoteLocked(req.CandidateId, req.Term)
		s.GetLogger().Debug("re-granted vote to candidate (idempotent duplicate request)",
			"candidate_id", req.CandidateId,
			"term", currentTerm,
		)
		return true
	}

	// Fresh vote grant for this term (votedFor == -1):
	s.votesGranted.Add(1)
	s.recordEventLocked(EventVoteGranted, fmt.Sprintf("granted vote to candidate %d for term %d", req.CandidateId, currentTerm), map[string]interface{}{
		"candidate_id": req.CandidateId,
		"term":         currentTerm,
	})
	if s.PersistentState != nil {
		s.PersistentState.VotedFor = raft.NodeID(req.CandidateId)
	}
	_ = s.persistLocked()
	s.resetElectionTimerOnGrantingVoteLocked(req.CandidateId, req.Term)
	s.GetLogger().Info("granted vote to candidate", "candidate_id", req.CandidateId, "term", currentTerm)
	return true
}

// processPreVoteLocked evaluates an incoming PreVote RequestVote RPC per Ongaro §9.6.
// Receivers answer based on what they would do, without changing term or votedFor.
func (s *Server) processPreVoteLocked(req *raftpb.RequestVoteArgs) *raftpb.RequestVoteReply {
	var currentTerm uint64
	if s.PersistentState != nil {
		currentTerm = uint64(s.PersistentState.CurrentTerm)
	}

	s.GetLogger().Debug("received PreVote request",
		"candidate_id", req.CandidateId,
		"proposed_term", req.Term,
		"current_term", currentTerm,
		"last_log_index", req.LastLogIndex,
		"last_log_term", req.LastLogTerm,
	)

	if s.requestVoteFilter != nil && !s.requestVoteFilter(req) {
		s.votesDenied.Add(1)
		return &raftpb.RequestVoteReply{Term: currentTerm, VoteGranted: false}
	}

	if req.Term < currentTerm || (!req.GetLeadershipTransfer() && (s.State == raft.Leader || s.hasActiveLeaderLocked())) {
		s.votesDenied.Add(1)
		return &raftpb.RequestVoteReply{Term: currentTerm, VoteGranted: false}
	}

	if !s.isLogUpToDateLocked(req.LastLogIndex, req.LastLogTerm) {
		s.votesDenied.Add(1)
		return &raftpb.RequestVoteReply{Term: currentTerm, VoteGranted: false}
	}

	if req.Term == currentTerm && s.PersistentState != nil {
		if s.PersistentState.VotedFor != -1 && s.PersistentState.VotedFor != raft.NodeID(req.CandidateId) {
			s.votesDenied.Add(1)
			return &raftpb.RequestVoteReply{Term: currentTerm, VoteGranted: false}
		}
	}

	s.votesGranted.Add(1)
	return &raftpb.RequestVoteReply{Term: currentTerm, VoteGranted: true}
}

func (s *Server) denyVoteActiveLeaderLeaseLocked(req *raftpb.RequestVoteArgs, currentTerm uint64) *raftpb.RequestVoteReply {
	s.votesDenied.Add(1)
	s.recordEventLocked(EventVoteDenied, fmt.Sprintf("denied vote to candidate %d for term %d (active leader lease)", req.CandidateId, req.Term), map[string]interface{}{
		"candidate_id": req.CandidateId,
		"term":         req.Term,
		"current_term": currentTerm,
		"reason":       "active_leader_lease",
	})
	s.GetLogger().Info("denied vote: receiver has an active leader within minimum election timeout (§9.6)",
		"candidate_id", req.CandidateId,
		"req_term", req.Term,
		"current_term", currentTerm,
	)
	return &raftpb.RequestVoteReply{Term: currentTerm, VoteGranted: false}
}

// processRequestVote evaluates an incoming RequestVote RPC exactly per Raft paper Figure 2 rules 1-2.
func (s *Server) processRequestVote(req *raftpb.RequestVoteArgs) *raftpb.RequestVoteReply {
	if req.GetPreVote() {
		return s.processPreVoteLocked(req)
	}

	s.GetLogger().Debug("received RequestVote",
		"candidate_id", req.CandidateId,
		"term", req.Term,
		"last_log_index", req.LastLogIndex,
		"last_log_term", req.LastLogTerm,
	)

	currentTerm, votedFor, ok, earlyReply := s.validateRequestVote(req)
	if !ok {
		return earlyReply
	}

	// Followers reject votes if they heard from a current leader within the minimum election
	// timeout (§9.6), unless this is an explicit leadership-transfer request (§6.4).
	if !req.GetLeadershipTransfer() && s.hasActiveLeaderLocked() {
		return s.denyVoteActiveLeaderLeaseLocked(req, currentTerm)
	}

	steppedDown := false
	if req.Term > currentTerm {
		currentTerm, votedFor, steppedDown = s.stepDownOnHigherTermLocked(req.Term)
	}

	voteGranted := s.recordVoteGrantOrDenyLocked(req, currentTerm, votedFor, steppedDown)
	return &raftpb.RequestVoteReply{
		Term:        currentTerm,
		VoteGranted: voteGranted,
	}
}

func (s *Server) applyAppendEntriesRoleAndTimer(req *raftpb.AppendEntriesArgs, currentTerm uint64) uint64 {
	if req.Term > currentTerm {
		s.becomeFollowerLocked(raft.Term(req.Term))
		if s.PersistentState != nil {
			currentTerm = uint64(s.PersistentState.CurrentTerm)
		}
	} else if s.State == raft.Candidate || s.State == raft.PreCandidate {
		s.becomeFollowerLocked(raft.Term(req.Term))
	}

	now := s.getClockLocked().Now()
	s.lastHeartbeat = now
	s.lastLeaderHeartbeat = now
	if s.ElectionTimer != nil {
		newTimeout := s.ElectionTimer.Reset()
		s.GetLogger().Debug("reset election timer on valid heartbeat/AppendEntries",
			"leader_id", req.LeaderId,
			"term", req.Term,
			"timeout_ms", newTimeout.Milliseconds(),
		)
	}

	if len(req.Entries) == 0 {
		s.heartbeatsReceived++
		s.recordEventLocked(EventHeartbeatReceived, fmt.Sprintf("received heartbeat from leader %d for term %d", req.LeaderId, req.Term), map[string]interface{}{
			"leader_id": req.LeaderId,
			"term":      req.Term,
		})
	}
	return currentTerm
}

func (s *Server) checkPrevLogInLog(prevLogIndex uint64, prevLogTerm uint64) bool {
	if prevLogIndex == 0 {
		return true
	}
	var logLen uint64
	if s.PersistentState != nil {
		logLen = uint64(len(s.PersistentState.Log))
	}
	if prevLogIndex > logLen {
		return false
	}
	return s.PersistentState.Log[prevLogIndex-1].Term == raft.Term(prevLogTerm)
}

// processAppendEntries evaluates an incoming AppendEntries RPC exactly per Raft paper Figure 2 rules 1-2.
func (s *Server) processAppendEntries(req *raftpb.AppendEntriesArgs) *raftpb.AppendEntriesReply {
	s.GetLogger().Debug("received AppendEntries",
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

	if req.Term < currentTerm {
		s.GetLogger().Warn("rejected AppendEntries from stale leader",
			"leader_id", req.LeaderId,
			"req_term", req.Term,
			"current_term", currentTerm,
		)
		return &raftpb.AppendEntriesReply{Term: currentTerm, Success: false}
	}

	currentTerm = s.applyAppendEntriesRoleAndTimer(req, currentTerm)
	success := s.checkPrevLogInLog(req.PrevLogIndex, req.PrevLogTerm)

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

	if s.ElectionTimer != nil {
		s.ElectionTimer.Reset()
	}

	s.votesReceived = make(map[raft.NodeID]bool)

	var curTerm uint64
	if s.PersistentState != nil {
		curTerm = uint64(s.PersistentState.CurrentTerm)
	}

	if oldRole != raft.Follower {
		s.recordEventLocked(EventStateTransition, fmt.Sprintf("transitioned to Follower for term %d", curTerm), map[string]interface{}{
			"previous_state": oldRole.String(),
			"term":           curTerm,
		})
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
	s.electionsStarted.Add(1)

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

	s.recordEventLocked(EventElectionStarted, fmt.Sprintf("started election for term %d", curTerm), map[string]interface{}{
		"term":              curTerm,
		"previous_state":    oldRole.String(),
		"elections_started": s.electionsStarted.Load(),
	})

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

func (s *Server) initVolatileStateLeaderLocked(lastLogIndex raft.LogIndex) {
	s.VolatileStateLeader = &raft.VolatileStateLeader{
		NextIndex:  make(map[raft.NodeID]raft.LogIndex, len(s.Peers)),
		MatchIndex: make(map[raft.NodeID]raft.LogIndex, len(s.Peers)),
	}
	for _, p := range s.Peers {
		s.VolatileStateLeader.NextIndex[p.ID] = lastLogIndex + 1
		s.VolatileStateLeader.MatchIndex[p.ID] = 0
	}
}

func (s *Server) startLeaderTimersLocked() {
	if s.heartbeatInterval <= 0 {
		s.heartbeatInterval = raft.DefaultHeartbeatInterval
	}
	if s.heartbeatTicker != nil {
		s.heartbeatTicker.Stop()
	}
	clk := s.getClockLocked()
	s.heartbeatTicker = clk.NewTicker(s.heartbeatInterval)

	if s.heartbeatTimer != nil {
		s.heartbeatTimer.Stop()
	}
	s.heartbeatTimer = clk.NewTimer(s.heartbeatInterval)

	s.heartbeatAcks = make(map[raft.NodeID]bool)
	s.lastHeardFrom = make(map[raft.NodeID]time.Time)
	now := clk.Now()
	if len(s.votesReceived) > 0 {
		for peerID, granted := range s.votesReceived {
			if granted && peerID != s.ID {
				s.lastHeardFrom[peerID] = now
			}
		}
	} else {
		for _, peer := range s.Peers {
			s.lastHeardFrom[peer.ID] = now
		}
	}
	if s.checkQuorum {
		s.resetCheckQuorumTimerLocked()
	}
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

	lastLogIndex, _ := s.lastLogInfoLocked()
	s.initVolatileStateLeaderLocked(lastLogIndex)
	s.startLeaderTimersLocked()

	var curTerm uint64
	if s.PersistentState != nil {
		curTerm = uint64(s.PersistentState.CurrentTerm)
	}

	s.recordEventLocked(EventStateTransition, fmt.Sprintf("transitioned to Leader for term %d", curTerm), map[string]interface{}{
		"previous_state": oldRole.String(),
		"term":           curTerm,
	})

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
// With Pre-Vote (§9.6), it transitions to PreCandidate without incrementing term.
func (s *Server) handleElectionTimeout(t time.Time) {
	s.GetLogger().Info("election timeout fired", "node_id", int(s.ID), "state", s.State.String(), "time", t)

	if s.State == raft.Leader {
		return
	}

	if s.preVote {
		s.startPreVoteLocked()
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
				s.startElectionOrPreVoteLocked()
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
	s.startElectionOrPreVoteLocked()
}

func (s *Server) startElectionOrPreVoteLocked() {
	if s.preVote {
		s.startPreVoteLocked()
		return
	}
	s.startElectionLocked()
}

// StartPreVote explicitly initiates a pre-vote round (§9.6).
func (s *Server) StartPreVote() {
	if s.loopRunning.Load() {
		resCh := make(chan struct{}, 1)
		select {
		case s.stateActionCh <- stateAction{
			fn: func() {
				s.startPreVoteLocked()
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
	s.startPreVoteLocked()
}

// startPreVoteLocked begins a speculative Pre-Vote round (§9.6):
// 1. Transitions to PreCandidate without incrementing term or changing votedFor
// 2. Broadcasts PreVote RPCs to all peers for proposed term currentTerm + 1
func (s *Server) startPreVoteLocked() {
	s.becomePreCandidateLocked()
	s.broadcastPreVotesLocked()
}

func (s *Server) becomePreCandidateLocked() {
	oldRole := s.State
	s.State = raft.PreCandidate
	s.stopHeartbeatTickerLocked()

	if s.ElectionTimer != nil {
		s.ElectionTimer.Reset()
	}

	s.preVotesReceived = make(map[raft.NodeID]bool)
	s.preVotesReceived[s.ID] = true

	var curTerm uint64
	if s.PersistentState != nil {
		curTerm = uint64(s.PersistentState.CurrentTerm)
	}

	s.recordEventLocked(EventStateTransition, fmt.Sprintf("transitioned to PreCandidate for speculative term %d", curTerm+1), map[string]interface{}{
		"previous_state":   oldRole.String(),
		"current_term":     curTerm,
		"speculative_term": curTerm + 1,
	})

	s.GetLogger().Info("transitioned to PreCandidate (started pre-vote phase)",
		"node_id", int(s.ID),
		"previous_state", oldRole.String(),
		"current_term", curTerm,
		"speculative_term", curTerm + 1,
		"pre_votes_count", 1,
	)

	totalNodes := len(s.Peers) + 1
	majority := (totalNodes / 2) + 1
	if len(s.preVotesReceived) >= majority {
		s.startElectionLocked()
	}
}

func (s *Server) broadcastPreVotesLocked() {
	if s.State != raft.PreCandidate || len(s.Peers) == 0 {
		return
	}

	var currentTerm uint64
	if s.PersistentState != nil {
		currentTerm = uint64(s.PersistentState.CurrentTerm)
	}
	proposedTerm := raft.Term(currentTerm + 1)
	lastLogIndex, lastLogTerm := s.lastLogInfoLocked()
	timeout := s.RPCTimeout()
	id := s.ID

	s.GetLogger().Info("broadcasting PreVote RPCs in parallel to all peers",
		"node_id", int(id),
		"proposed_term", uint64(proposedTerm),
		"peers_count", len(s.Peers),
		"timeout_ms", timeout.Milliseconds(),
	)

	for _, peer := range s.Peers {
		if peer.ID != id {
			go s.sendPreVoteRequestToPeer(peer, proposedTerm, lastLogIndex, lastLogTerm, timeout)
		}
	}
}

func (s *Server) sendPreVoteRequestToPeer(peer Peer, proposedTerm raft.Term, lastLogIndex raft.LogIndex, lastLogTerm raft.Term, timeout time.Duration) {
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

	reply, err := s.SendRequestVote(ctx, peer, &raft.RequestVoteArgs{
		Term:         proposedTerm,
		CandidateID:  s.ID,
		LastLogIndex: lastLogIndex,
		LastLogTerm:  lastLogTerm,
		PreVote:      true,
	})

	select {
	case s.voteResultCh <- RequestVoteResult{
		PeerID:       peer.ID,
		ElectionTerm: proposedTerm,
		PreVote:      true,
		Reply:        reply,
		Err:          err,
	}:
	case <-s.stopCh:
	}
}

// StartElectionInTerm forces the server to begin an election in a specific term (test hook).
// When the event loop is active, the mutation is executed on the single event loop.
func (s *Server) StartElectionInTerm(term raft.Term) {
	if s.loopRunning.Load() {
		resCh := make(chan struct{}, 1)
		select {
		case s.stateActionCh <- stateAction{
			fn: func() {
				s.startElectionInTermLocked(term)
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
	s.startElectionInTermLocked(term)
}

func (s *Server) startElectionInTermLocked(term raft.Term) {
	oldRole := s.State
	s.State = raft.Candidate

	if s.PersistentState != nil {
		s.PersistentState.CurrentTerm = term
		s.PersistentState.VotedFor = s.ID
		_ = s.persistLocked()
	}

	s.stopHeartbeatTickerLocked()

	if s.ElectionTimer != nil {
		s.ElectionTimer.Reset()
	}

	s.votesReceived = make(map[raft.NodeID]bool)
	s.votesReceived[s.ID] = true

	s.GetLogger().Info("forced candidate election in term (test hook)",
		"node_id", int(s.ID),
		"previous_state", oldRole.String(),
		"term", uint64(term),
		"voted_for", int(s.ID),
		"votes_count", 1,
	)

	s.broadcastRequestVotesLocked()
}

// processTimeoutNow handles a TimeoutNow RPC for leadership transfer (§3.10).
// The target follower starts an election immediately, bypassing leader stickiness.
func (s *Server) processTimeoutNow(req *raftpb.TimeoutNowArgs) *raftpb.TimeoutNowReply {
	s.GetLogger().Info("received TimeoutNow RPC",
		"leader_id", req.LeaderId,
		"term", req.Term,
	)

	var currentTerm uint64
	if s.PersistentState != nil {
		currentTerm = uint64(s.PersistentState.CurrentTerm)
	}

	if req.Term < currentTerm {
		s.GetLogger().Warn("rejected TimeoutNow from stale leader",
			"leader_id", req.LeaderId,
			"req_term", req.Term,
			"current_term", currentTerm,
		)
		return &raftpb.TimeoutNowReply{Term: currentTerm, Success: false}
	}

	if req.Term > currentTerm {
		s.becomeFollowerLocked(raft.Term(req.Term))
	}

	// Target starts an election immediately, bypassing stickiness (§3.10)
	s.startElectionWithTransferLocked(true)

	var newTerm uint64
	if s.PersistentState != nil {
		newTerm = uint64(s.PersistentState.CurrentTerm)
	}
	return &raftpb.TimeoutNowReply{
		Term:    newTerm,
		Success: true,
	}
}

// startElectionLocked begins an election cycle:
// 1. Transitions to Candidate, increments term, votes for self, persists state, resets timer (§5.2)
// 2. Sends RequestVote RPCs in parallel to all peers using goroutines + results channel
func (s *Server) startElectionLocked() {
	s.startElectionWithTransferLocked(false)
}

func (s *Server) startElectionWithTransferLocked(isTransfer bool) {
	s.becomeCandidateLocked()
	s.broadcastRequestVotesWithTransferLocked(isTransfer)
}

func (s *Server) sendVoteRequestToPeer(peer Peer, term raft.Term, lastLogIndex raft.LogIndex, lastLogTerm raft.Term, timeout time.Duration, isTransfer bool) {
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

	reply, err := s.SendRequestVote(ctx, peer, &raft.RequestVoteArgs{
		Term:               term,
		CandidateID:        s.ID,
		LastLogIndex:       lastLogIndex,
		LastLogTerm:        lastLogTerm,
		LeadershipTransfer: isTransfer,
	})

	select {
	case s.voteResultCh <- RequestVoteResult{
		PeerID:       peer.ID,
		ElectionTerm: term,
		Reply:        reply,
		Err:          err,
	}:
	case <-s.stopCh:
	}
}

// broadcastRequestVotesLocked sends RequestVote RPCs in parallel to all peers using goroutines,
// streaming results back onto s.voteResultCh.
func (s *Server) broadcastRequestVotesLocked() {
	s.broadcastRequestVotesWithTransferLocked(false)
}

func (s *Server) broadcastRequestVotesWithTransferLocked(isTransfer bool) {
	if s.State != raft.Candidate || len(s.Peers) == 0 {
		return
	}

	term := s.PersistentState.CurrentTerm
	lastLogIndex, lastLogTerm := s.lastLogInfoLocked()
	timeout := s.RPCTimeout()
	id := s.ID

	s.GetLogger().Info("broadcasting RequestVote RPCs in parallel to all peers",
		"node_id", int(id),
		"term", uint64(term),
		"peers_count", len(s.Peers),
		"timeout_ms", timeout.Milliseconds(),
		"leadership_transfer", isTransfer,
	)

	for _, peer := range s.Peers {
		if peer.ID != id {
			go s.sendVoteRequestToPeer(peer, term, lastLogIndex, lastLogTerm, timeout, isTransfer)
		}
	}
}

// SendRequestVotesParallel sends RequestVote RPCs in parallel to all provided peers using goroutines,
// streaming results back onto the provided results channel.
func (s *Server) SendRequestVotesParallel(ctx context.Context, term raft.Term, peers []Peer, resCh chan<- RequestVoteResult) {
	s.mu.RLock()
	id := s.ID
	lastLogIndex, lastLogTerm := s.lastLogInfoLocked()
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

func (s *Server) checkHigherTermInVoteReply(replyTerm raft.Term, peerID raft.NodeID, currentTerm uint64) bool {
	if uint64(replyTerm) > currentTerm {
		s.GetLogger().Info("discovered higher term in RequestVote reply, stepping down to follower",
			"node_id", int(s.ID),
			"reply_term", uint64(replyTerm),
			"current_term", currentTerm,
			"peer_id", int(peerID),
		)
		s.becomeFollowerLocked(replyTerm)
		return true
	}
	return false
}

func (s *Server) recordVoteAndCheckMajorityLocked(peerID raft.NodeID, currentTerm uint64) {
	if s.State != raft.Candidate {
		return
	}
	if s.votesReceived == nil {
		s.votesReceived = make(map[raft.NodeID]bool)
		s.votesReceived[s.ID] = true
	}
	s.votesReceived[peerID] = true
	votesCount := len(s.votesReceived)
	totalNodes := len(s.Peers) + 1
	majority := (totalNodes / 2) + 1

	s.GetLogger().Info("received granted vote from peer",
		"node_id", int(s.ID),
		"peer_id", int(peerID),
		"term", currentTerm,
		"votes_count", votesCount,
		"majority_required", majority,
	)

	if votesCount >= majority {
		s.GetLogger().Info("achieved majority votes, transitioning to Leader",
			"node_id", int(s.ID),
			"term", currentTerm,
			"votes_count", votesCount,
			"total_nodes", totalNodes,
			"majority", majority,
		)
		s.becomeLeaderLocked()
		s.broadcastHeartbeats()
	}
}

// handleVoteResultLocked processes an incoming vote result from a peer.
// In Raft (§5.1 & §5.2):
//   - If reply contains higher term: step down to Follower immediately
//   - Ignore reply if its term differs from currentTerm, if ElectionTerm != currentTerm,
//     or if the server is no longer in Candidate state
//   - If vote granted: count vote; on majority, transition to Leader
func (s *Server) handleVoteResultLocked(res RequestVoteResult) {
	if res.Err != nil || res.Reply == nil {
		if res.Err != nil {
			s.GetLogger().Debug("RequestVote RPC to peer failed or timed out", "peer_id", int(res.PeerID), "error", res.Err)
		}
		return
	}

	var currentTerm uint64
	if s.PersistentState != nil {
		currentTerm = uint64(s.PersistentState.CurrentTerm)
	}

	if s.checkHigherTermInVoteReply(res.Reply.Term, res.PeerID, currentTerm) {
		return
	}

	if res.PreVote {
		s.handlePreVoteResultLocked(res, currentTerm)
		return
	}

	if res.Reply.Term != raft.Term(currentTerm) || res.ElectionTerm != raft.Term(currentTerm) || s.State != raft.Candidate {
		s.GetLogger().Debug("ignored stale or mismatched RequestVote reply",
			"node_id", int(s.ID),
			"peer_id", int(res.PeerID),
			"state", s.State.String(),
			"current_term", currentTerm,
			"election_term", res.ElectionTerm,
			"reply_term", res.Reply.Term,
		)
		return
	}

	if res.Reply.VoteGranted {
		s.recordVoteAndCheckMajorityLocked(res.PeerID, currentTerm)
	} else {
		s.GetLogger().Info("vote denied by peer",
			"node_id", int(s.ID),
			"peer_id", int(res.PeerID),
			"term", currentTerm,
			"reply_term", res.Reply.Term,
		)
	}
}

func (s *Server) handlePreVoteResultLocked(res RequestVoteResult, currentTerm uint64) {
	if s.State != raft.PreCandidate {
		return
	}
	expectedTerm := raft.Term(currentTerm + 1)
	if res.ElectionTerm != expectedTerm || !res.Reply.VoteGranted {
		return
	}

	if s.preVotesReceived == nil {
		s.preVotesReceived = make(map[raft.NodeID]bool)
		s.preVotesReceived[s.ID] = true
	}
	s.preVotesReceived[res.PeerID] = true

	totalNodes := len(s.Peers) + 1
	majority := (totalNodes / 2) + 1
	if len(s.preVotesReceived) >= majority {
		s.GetLogger().Info("achieved majority pre-votes; initiating real election (§9.6)",
			"node_id", int(s.ID),
			"speculative_term", currentTerm+1,
			"pre_votes_count", len(s.preVotesReceived),
			"majority", majority,
		)
		s.startElectionLocked()
	}
}

// handleHeartbeatTick is called on each tick of the leader-only heartbeat ticker (every 50ms).
// In Raft (§5.2), leaders periodically send heartbeats (AppendEntries with no log entries)
// to all peers to maintain authority and prevent election timeouts.
func (s *Server) handleHeartbeatTick() {
	if s.State != raft.Leader {
		return
	}
	if s.checkQuorum {
		s.handleCheckQuorumTimeoutLocked()
		if s.State != raft.Leader {
			return
		}
	}
	s.heartbeatsSent.Add(1)

	var curTerm uint64
	if s.PersistentState != nil {
		curTerm = uint64(s.PersistentState.CurrentTerm)
	}

	s.recordEventLocked(EventHeartbeatSent, fmt.Sprintf("leader heartbeat dispatched for term %d", curTerm), map[string]interface{}{
		"term":            curTerm,
		"peers_count":     len(s.Peers),
		"heartbeats_sent": s.heartbeatsSent.Load(),
	})

	s.GetLogger().Debug("leader heartbeat ticker fired (will call AppendEntries)",
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

func (s *Server) sendHeartbeatToPeer(peer Peer, args *raft.AppendEntriesArgs, timeout time.Duration) {
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

	reply, err := s.SendAppendEntries(ctx, peer, args)

	select {
	case s.heartbeatResultCh <- HeartbeatResult{
		PeerID: peer.ID,
		Term:   args.Term,
		Reply:  reply,
		Err:    err,
	}:
	case <-s.stopCh:
	}
}

// broadcastHeartbeats sends real empty AppendEntries RPCs (heartbeats) to all cluster peers (§5.2).
// In Raft (§5.2), leaders periodically send heartbeats (AppendEntries with no log entries)
// to maintain authority and prevent election timeouts on followers.
func (s *Server) broadcastHeartbeats() {
	if s.State != raft.Leader {
		return
	}

	term := s.PersistentState.CurrentTerm
	id := s.ID
	lastLogIndex, lastLogTerm := s.lastLogInfoLocked()

	var commitIndex raft.LogIndex
	if s.VolatileState != nil {
		commitIndex = s.VolatileState.CommitIndex
	}
	timeout := s.RPCTimeout()

	for _, peer := range s.Peers {
		if peer.ID == id {
			continue
		}
		s.GetLogger().Debug(fmt.Sprintf("would send heartbeat to peer %d", peer.ID),
			"leader_id", int(id),
			"peer_id", int(peer.ID),
			"peer_addr", peer.Address,
		)

		if s.loopRunning.Load() && s.RPCClientPool != nil && peer.Address != "" {
			args := &raft.AppendEntriesArgs{
				Term:         term,
				LeaderID:     id,
				PrevLogIndex: lastLogIndex,
				PrevLogTerm:  lastLogTerm,
				Entries:      nil,
				LeaderCommit: commitIndex,
			}
			go s.sendHeartbeatToPeer(peer, args, timeout)
		}
	}
}

// handleHeartbeatResultLocked processes an AppendEntries heartbeat response received from a peer.
// Must be called with s.mu held (or inside the event loop).
func (s *Server) handleHeartbeatResultLocked(res HeartbeatResult) {
	if res.Err == nil && res.Reply != nil {
		if s.lastHeardFrom == nil {
			s.lastHeardFrom = make(map[raft.NodeID]time.Time)
		}
		s.lastHeardFrom[res.PeerID] = s.getClockLocked().Now()
		if res.Reply.Success {
			if s.heartbeatAcks == nil {
				s.heartbeatAcks = make(map[raft.NodeID]bool)
			}
			s.heartbeatAcks[res.PeerID] = true
		}
	}

	if res.Err != nil || res.Reply == nil {
		return
	}

	var currentTerm uint64
	if s.PersistentState != nil {
		currentTerm = uint64(s.PersistentState.CurrentTerm)
	}

	// Figure 2: If RPC response contains term T > currentTerm, convert to follower (§5.1)
	if uint64(res.Reply.Term) > currentTerm {
		s.GetLogger().Info("discovered higher term in AppendEntries heartbeat reply, stepping down to follower",
			"node_id", int(s.ID),
			"reply_term", uint64(res.Reply.Term),
			"current_term", currentTerm,
			"peer_id", int(res.PeerID),
		)
		s.becomeFollowerLocked(res.Reply.Term)
		return
	}
}

// handleHeartbeatTimeout is an alias for handleHeartbeatTick for backward compatibility.
func (s *Server) handleHeartbeatTimeout() {
	s.handleHeartbeatTick()
}

func (s *Server) waitIfPaused() bool {
	if !s.paused.Load() {
		return false
	}
	s.pauseMu.Lock()
	rCh := s.resumeCh
	s.pauseMu.Unlock()
	if rCh != nil {
		select {
		case <-rCh:
		case <-s.stopCh:
			return true
		}
	}
	return false
}

func (s *Server) selectActiveTimerChannels() (<-chan time.Time, <-chan time.Time, <-chan time.Time) {
	var timerCh <-chan time.Time
	if s.State != raft.Leader && s.ElectionTimer != nil {
		timerCh = s.ElectionTimer.C()
	}

	var heartbeatCh <-chan time.Time
	if s.State == raft.Leader {
		if s.heartbeatTicker != nil {
			heartbeatCh = s.heartbeatTicker.C()
		} else if s.heartbeatTimer != nil {
			heartbeatCh = s.heartbeatTimer.C()
		}
	}

	var checkQuorumCh <-chan time.Time
	if s.State == raft.Leader && s.checkQuorum && s.checkQuorumTimer != nil {
		checkQuorumCh = s.checkQuorumTimer.C()
	}
	return timerCh, heartbeatCh, checkQuorumCh
}

func (s *Server) handleRequestVoteEvent(ev RequestVoteEvent) bool {
	s.mu.Lock()
	reply := s.processRequestVote(ev.Args)
	s.mu.Unlock()
	if ev.ResCh != nil {
		select {
		case ev.ResCh <- reply:
		case <-s.stopCh:
			return true
		}
	}
	return false
}

func (s *Server) handleAppendEntriesEvent(ev AppendEntriesEvent) bool {
	s.mu.Lock()
	reply := s.processAppendEntries(ev.Args)
	s.mu.Unlock()
	if ev.ResCh != nil {
		select {
		case ev.ResCh <- reply:
		case <-s.stopCh:
			return true
		}
	}
	return false
}

func (s *Server) handleTimeoutNowEvent(ev TimeoutNowEvent) bool {
	s.mu.Lock()
	reply := s.processTimeoutNow(ev.Args)
	s.mu.Unlock()
	if ev.ResCh != nil {
		select {
		case ev.ResCh <- reply:
		case <-s.stopCh:
			return true
		}
	}
	return false
}

func (s *Server) handleStateActionEvent(act stateAction) bool {
	s.mu.Lock()
	act.fn()
	s.mu.Unlock()
	if act.resCh != nil {
		select {
		case act.resCh <- struct{}{}:
		case <-s.stopCh:
			return true
		}
	}
	return false
}

func (s *Server) processEventLoopCycle(timerCh, heartbeatCh, checkQuorumCh <-chan time.Time) bool {
	select {
	case <-s.stopCh:
		return true
	case <-s.pauseNotifyCh:
		return false
	case <-checkQuorumCh:
		s.mu.Lock()
		s.handleCheckQuorumTimeoutLocked()
		s.mu.Unlock()
	case t, ok := <-timerCh:
		if !ok {
			return true
		}
		s.mu.Lock()
		s.handleElectionTimeout(t)
		s.mu.Unlock()
	case res := <-s.voteResultCh:
		s.mu.Lock()
		s.handleVoteResultLocked(res)
		s.mu.Unlock()
	case res := <-s.heartbeatResultCh:
		s.mu.Lock()
		s.handleHeartbeatResultLocked(res)
		s.mu.Unlock()
	case <-heartbeatCh:
		s.mu.Lock()
		s.handleHeartbeatTick()
		s.mu.Unlock()
	case ev := <-s.requestVoteCh:
		return s.handleRequestVoteEvent(ev)
	case ev := <-s.appendEntriesCh:
		return s.handleAppendEntriesEvent(ev)
	case ev := <-s.timeoutNowCh:
		return s.handleTimeoutNowEvent(ev)
	case act := <-s.stateActionCh:
		return s.handleStateActionEvent(act)
	}
	return false
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
		if s.waitIfPaused() {
			return
		}
		timerCh, heartbeatCh, checkQuorumCh := s.selectActiveTimerChannels()
		if s.processEventLoopCycle(timerCh, heartbeatCh, checkQuorumCh) {
			return
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

	s.GetLogger().Debug("sending RequestVote", "peer_id", int(peer.ID), "term", uint64(args.Term))
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

	s.GetLogger().Debug("sending AppendEntries", "peer_id", int(peer.ID), "term", uint64(args.Term), "entries_count", len(args.Entries))
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

// SendTimeoutNow sends a TimeoutNow RPC to a peer using the server's RPC client pool.
func (s *Server) SendTimeoutNow(ctx context.Context, peer Peer, args *raft.TimeoutNowArgs) (*raft.TimeoutNowReply, error) {
	if s.RPCClientPool == nil {
		return nil, fmt.Errorf("server %d RPCClientPool is nil", s.ID)
	}
	if peer.Address == "" {
		if p, ok := s.GetPeer(peer.ID); ok {
			peer = p
		}
	}

	s.GetLogger().Debug("sending TimeoutNow", "peer_id", int(peer.ID), "term", uint64(args.Term))
	reply, err := s.RPCClientPool.SendTimeoutNow(ctx, peer, args)
	if err != nil {
		s.GetLogger().Warn("failed to send TimeoutNow", "peer_id", int(peer.ID), "error", err, "is_timeout", rpc.IsTimeout(err))
		return nil, err
	}
	return reply, nil
}

// SendTimeoutNowToNode sends a TimeoutNow RPC to a peer identified by NodeID.
func (s *Server) SendTimeoutNowToNode(ctx context.Context, id raft.NodeID, args *raft.TimeoutNowArgs) (*raft.TimeoutNowReply, error) {
	peer, ok := s.GetPeer(id)
	if !ok {
		return nil, fmt.Errorf("peer %d not found in server peer list", id)
	}
	return s.SendTimeoutNow(ctx, peer, args)
}

// Transfer initiates leadership transfer to the target node per Raft §3.10.
// Note: full transfer needs log catch-up, which is scheduled for Month 3; this triggers the immediate election.
func (s *Server) Transfer(target raft.NodeID) error {
	return s.TransferWithContext(context.Background(), target)
}

// TransferWithContext initiates leadership transfer to the target node with a context.
func (s *Server) TransferWithContext(ctx context.Context, target raft.NodeID) error {
	s.mu.RLock()
	if s.State != raft.Leader {
		s.mu.RUnlock()
		return fmt.Errorf("cannot transfer leadership: server %d is not the leader", s.ID)
	}
	if target == s.ID {
		s.mu.RUnlock()
		return nil
	}
	peer, ok := s.getPeerLocked(target)
	var curTerm raft.Term
	if s.PersistentState != nil {
		curTerm = s.PersistentState.CurrentTerm
	}
	s.mu.RUnlock()

	if !ok {
		return fmt.Errorf("target node %d not found in peer list", target)
	}

	args := &raft.TimeoutNowArgs{
		Term:     curTerm,
		LeaderID: s.ID,
	}

	reply, err := s.SendTimeoutNow(ctx, peer, args)
	if err != nil {
		return fmt.Errorf("failed to send TimeoutNow to target %d: %w", target, err)
	}

	if !reply.Success {
		return fmt.Errorf("target %d rejected TimeoutNow (term %d)", target, reply.Term)
	}

	s.checkReplyTerm(reply.Term)
	return nil
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
