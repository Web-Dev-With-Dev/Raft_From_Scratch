package server

import (
	"context"
	"fmt"
	"io"
	"math/rand"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"raft/logger"
	raftpb "raft/proto"
	"raft/raft"
	"raft/store"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

// RPCMessageType identifies the category of Raft RPC message.
type RPCMessageType string

const (
	RPCRequestVote   RPCMessageType = "RequestVote"
	RPCAppendEntries RPCMessageType = "AppendEntries"
)

// MessageFilter is a hook function that can inspect, modify, or drop messages.
// Returns true if the message should be dropped.
type MessageFilter func(src, dst raft.NodeID, msgType RPCMessageType, msg any) bool

// Network simulates an in-memory, zero-socket Raft network transport.
// It manages virtual message routing between nodes, allowing tests to simulate
// dropped packets, artificial network latency/delays, and multi-way partitions.
type Network struct {
	mu sync.RWMutex

	// nodes registered in the network
	nodes map[raft.NodeID]*Server

	// partitions: if partitions[src][dst] == true, communication from src to dst is blocked
	partitions map[raft.NodeID]map[raft.NodeID]bool

	// drops: if drops[src][dst] == true, messages from src to dst are dropped
	drops map[raft.NodeID]map[raft.NodeID]bool

	// delays: injected latency for communication between src and dst
	delays map[raft.NodeID]map[raft.NodeID]time.Duration

	// globalDelay across all links in the network
	globalDelay time.Duration

	// filters: custom dynamic drop hooks
	filters []MessageFilter

	// statistics
	totalSent    atomic.Uint64
	totalDropped atomic.Uint64
	totalDelayed atomic.Uint64
}

// NewNetwork creates and initializes a new in-memory fake Network.
func NewNetwork() *Network {
	return &Network{
		nodes:      make(map[raft.NodeID]*Server),
		partitions: make(map[raft.NodeID]map[raft.NodeID]bool),
		drops:      make(map[raft.NodeID]map[raft.NodeID]bool),
		delays:     make(map[raft.NodeID]map[raft.NodeID]time.Duration),
	}
}

// RegisterNode adds a server instance to the network.
func (n *Network) RegisterNode(id raft.NodeID, srv *Server) {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.nodes[id] = srv
}

// Partition isolates all nodes in set1 from all nodes in set2 bidirectionally.
// Nodes within set1 can still communicate with each other, and nodes within set2
// can still communicate with each other.
func (n *Network) Partition(set1, set2 []raft.NodeID) {
	n.mu.Lock()
	defer n.mu.Unlock()

	for _, n1 := range set1 {
		if n.partitions[n1] == nil {
			n.partitions[n1] = make(map[raft.NodeID]bool)
		}
		for _, n2 := range set2 {
			n.partitions[n1][n2] = true
			if n.partitions[n2] == nil {
				n.partitions[n2] = make(map[raft.NodeID]bool)
			}
			n.partitions[n2][n1] = true
		}
	}
}

// Heal removes all active network partitions across the entire network.
func (n *Network) Heal() {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.partitions = make(map[raft.NodeID]map[raft.NodeID]bool)
}

// Drop drops all messages sent from src to dst.
func (n *Network) Drop(src, dst raft.NodeID) {
	n.mu.Lock()
	defer n.mu.Unlock()
	if n.drops[src] == nil {
		n.drops[src] = make(map[raft.NodeID]bool)
	}
	n.drops[src][dst] = true
}

// DropBidirectional drops all messages between n1 and n2 in both directions.
func (n *Network) DropBidirectional(n1, n2 raft.NodeID) {
	n.Drop(n1, n2)
	n.Drop(n2, n1)
}

// DropFrom drops all outbound messages from src to any destination node.
func (n *Network) DropFrom(src raft.NodeID) {
	n.mu.Lock()
	defer n.mu.Unlock()
	if n.drops[src] == nil {
		n.drops[src] = make(map[raft.NodeID]bool)
	}
	for id := range n.nodes {
		if id != src {
			n.drops[src][id] = true
		}
	}
}

// DropTo drops all inbound messages to dst from any source node.
func (n *Network) DropTo(dst raft.NodeID) {
	n.mu.Lock()
	defer n.mu.Unlock()
	for id := range n.nodes {
		if id != dst {
			if n.drops[id] == nil {
				n.drops[id] = make(map[raft.NodeID]bool)
			}
			n.drops[id][dst] = true
		}
	}
}

// DropAll drops all messages network-wide.
func (n *Network) DropAll() {
	n.mu.Lock()
	defer n.mu.Unlock()
	for src := range n.nodes {
		if n.drops[src] == nil {
			n.drops[src] = make(map[raft.NodeID]bool)
		}
		for dst := range n.nodes {
			if src != dst {
				n.drops[src][dst] = true
			}
		}
	}
}

// Undrop restores message delivery from src to dst.
func (n *Network) Undrop(src, dst raft.NodeID) {
	n.mu.Lock()
	defer n.mu.Unlock()
	if n.drops[src] != nil {
		delete(n.drops[src], dst)
	}
}

// ClearDrops clears all dropped message configurations.
func (n *Network) ClearDrops() {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.drops = make(map[raft.NodeID]map[raft.NodeID]bool)
}

// SetDelay configures an artificial latency delay for messages sent from src to dst.
func (n *Network) SetDelay(src, dst raft.NodeID, delay time.Duration) {
	n.mu.Lock()
	defer n.mu.Unlock()
	if n.delays[src] == nil {
		n.delays[src] = make(map[raft.NodeID]time.Duration)
	}
	n.delays[src][dst] = delay
}

// SetDelayBidirectional configures delay between n1 and n2 in both directions.
func (n *Network) SetDelayBidirectional(n1, n2 raft.NodeID, delay time.Duration) {
	n.SetDelay(n1, n2, delay)
	n.SetDelay(n2, n1, delay)
}

// SetGlobalDelay configures delay across all links in the network.
func (n *Network) SetGlobalDelay(delay time.Duration) {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.globalDelay = delay
}

// ClearDelays clears all configured artificial delays.
func (n *Network) ClearDelays() {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.delays = make(map[raft.NodeID]map[raft.NodeID]time.Duration)
	n.globalDelay = 0
}

// AddFilter registers a custom message filter predicate.
func (n *Network) AddFilter(f MessageFilter) {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.filters = append(n.filters, f)
}

// ClearFilters removes all custom message filters.
func (n *Network) ClearFilters() {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.filters = nil
}

// IsPartitioned returns true if communication from src to dst is blocked by a partition.
func (n *Network) IsPartitioned(src, dst raft.NodeID) bool {
	n.mu.RLock()
	defer n.mu.RUnlock()
	return n.isBlockedLocked(src, dst)
}

// TotalSent returns total number of RPC messages dispatched through this network.
func (n *Network) TotalSent() uint64 {
	return n.totalSent.Load()
}

// TotalDropped returns total number of RPC messages dropped by partitions or filters.
func (n *Network) TotalDropped() uint64 {
	return n.totalDropped.Load()
}

// TotalDelayed returns total number of RPC messages delayed by artificial latency.
func (n *Network) TotalDelayed() uint64 {
	return n.totalDelayed.Load()
}

func (n *Network) isBlockedLocked(src, dst raft.NodeID) bool {
	if n.partitions[src] != nil && n.partitions[src][dst] {
		return true
	}
	if n.drops[src] != nil && n.drops[src][dst] {
		return true
	}
	return false
}

func (n *Network) getDelayLocked(src, dst raft.NodeID) time.Duration {
	d := n.globalDelay
	if n.delays[src] != nil && n.delays[src][dst] > 0 {
		d += n.delays[src][dst]
	}
	return d
}

func (n *Network) prepareOutboundRPC(ctx context.Context, src, dst raft.NodeID, msgType RPCMessageType, msg any) (*Server, error) {
	n.totalSent.Add(1)

	n.mu.RLock()
	blocked := n.isBlockedLocked(src, dst)
	delay := n.getDelayLocked(src, dst)
	filters := n.filters
	targetNode := n.nodes[dst]
	n.mu.RUnlock()

	for _, f := range filters {
		if f(src, dst, msgType, msg) {
			blocked = true
			break
		}
	}

	if blocked {
		n.totalDropped.Add(1)
		return nil, status.Errorf(codes.Unavailable, "network: message dropped (node %d to %d)", src, dst)
	}

	if delay > 0 {
		n.totalDelayed.Add(1)
		select {
		case <-time.After(delay):
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}

	if targetNode == nil || targetNode.IsKilled() {
		return nil, status.Errorf(codes.Unavailable, "network: target node %d is unavailable or killed", dst)
	}

	return targetNode, nil
}

func (n *Network) prepareInboundReply(ctx context.Context, dst, src raft.NodeID) error {
	n.mu.RLock()
	returnBlocked := n.partitions[dst] != nil && n.partitions[dst][src]
	returnDelay := n.getDelayLocked(dst, src)
	n.mu.RUnlock()

	if returnBlocked {
		n.totalDropped.Add(1)
		return status.Errorf(codes.Unavailable, "network: reply dropped by partition (node %d to %d)", dst, src)
	}

	if returnDelay > 0 {
		select {
		case <-time.After(returnDelay):
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	return nil
}

// sendRequestVote routes a RequestVote RPC from src to dst in-memory.
func (n *Network) sendRequestVote(ctx context.Context, src, dst raft.NodeID, req *raftpb.RequestVoteArgs) (*raftpb.RequestVoteReply, error) {
	targetNode, err := n.prepareOutboundRPC(ctx, src, dst, RPCRequestVote, req)
	if err != nil {
		return nil, err
	}

	clonedReq := proto.Clone(req).(*raftpb.RequestVoteArgs)
	reply, err := targetNode.RequestVote(ctx, clonedReq)
	if err != nil {
		return nil, err
	}

	if err := n.prepareInboundReply(ctx, dst, src); err != nil {
		return nil, err
	}

	return proto.Clone(reply).(*raftpb.RequestVoteReply), nil
}

// sendAppendEntries routes an AppendEntries RPC from src to dst in-memory.
func (n *Network) sendAppendEntries(ctx context.Context, src, dst raft.NodeID, req *raftpb.AppendEntriesArgs) (*raftpb.AppendEntriesReply, error) {
	targetNode, err := n.prepareOutboundRPC(ctx, src, dst, RPCAppendEntries, req)
	if err != nil {
		return nil, err
	}

	clonedReq := proto.Clone(req).(*raftpb.AppendEntriesArgs)
	reply, err := targetNode.AppendEntries(ctx, clonedReq)
	if err != nil {
		return nil, err
	}

	if err := n.prepareInboundReply(ctx, dst, src); err != nil {
		return nil, err
	}

	return proto.Clone(reply).(*raftpb.AppendEntriesReply), nil
}

// inMemoryPeerClient implements raftpb.RaftServiceClient routing calls through the Network.
type inMemoryPeerClient struct {
	net   *Network
	srcID raft.NodeID
	dstID raft.NodeID
}

func (c *inMemoryPeerClient) RequestVote(ctx context.Context, in *raftpb.RequestVoteArgs, opts ...grpc.CallOption) (*raftpb.RequestVoteReply, error) {
	return c.net.sendRequestVote(ctx, c.srcID, c.dstID, in)
}

func (c *inMemoryPeerClient) AppendEntries(ctx context.Context, in *raftpb.AppendEntriesArgs, opts ...grpc.CallOption) (*raftpb.AppendEntriesReply, error) {
	return c.net.sendAppendEntries(ctx, c.srcID, c.dstID, in)
}

// Cluster manages a group of N in-process Raft nodes interconnected by an in-memory Network.
type Cluster struct {
	t          testing.TB
	Network    *Network
	SimNetwork *SimNetwork
	servers    map[raft.NodeID]*Server
	nodeIDs    []raft.NodeID
	options    clusterOptions
	mu         sync.RWMutex
}

type clusterOptions struct {
	minTimeout          time.Duration
	maxTimeout          time.Duration
	heartbeatInterval   time.Duration
	rpcTimeout          time.Duration
	checkQuorum         bool
	checkQuorumInterval time.Duration
	autoStart           bool
	clock               raft.Clock
	simNetwork          *SimNetwork
	randSeed            int64
	quiet               bool
}

// ClusterOption configures test cluster parameters.
type ClusterOption func(*clusterOptions)

// WithQuiet silences node terminal logging while preserving in-memory EventLog.
func WithQuiet(quiet bool) ClusterOption {
	return func(o *clusterOptions) {
		o.quiet = quiet
	}
}

// WithRandSeed configures a deterministic pseudo-random seed for election timeouts across nodes.
func WithRandSeed(seed int64) ClusterOption {
	return func(o *clusterOptions) {
		o.randSeed = seed
	}
}

// WithSimNetwork configures a SimNetwork instance as the cluster transport.
func WithSimNetwork(simNet *SimNetwork) ClusterOption {
	return func(o *clusterOptions) {
		o.simNetwork = simNet
	}
}

// WithClock sets a custom Clock interface for cluster nodes.
func WithClock(clk raft.Clock) ClusterOption {
	return func(o *clusterOptions) {
		o.clock = clk
	}
}

// WithElectionTimeout configures the election timer range for all nodes.
func WithElectionTimeout(min, max time.Duration) ClusterOption {
	return func(o *clusterOptions) {
		o.minTimeout = min
		o.maxTimeout = max
	}
}

// WithHeartbeatInterval configures the leader heartbeat interval for all nodes.
func WithHeartbeatInterval(interval time.Duration) ClusterOption {
	return func(o *clusterOptions) {
		o.heartbeatInterval = interval
	}
}

// WithRPCTimeout configures the peer RPC timeout for all nodes.
func WithRPCTimeout(timeout time.Duration) ClusterOption {
	return func(o *clusterOptions) {
		o.rpcTimeout = timeout
	}
}

// WithCheckQuorum enables or disables check-quorum mechanism for all nodes in the cluster (§6.2).
func WithCheckQuorum(enabled bool) ClusterOption {
	return func(o *clusterOptions) {
		o.checkQuorum = enabled
	}
}

// WithCheckQuorumInterval configures the check-quorum timeout interval for all nodes.
func WithCheckQuorumInterval(interval time.Duration) ClusterOption {
	return func(o *clusterOptions) {
		o.checkQuorumInterval = interval
	}
}

// WithAutoStart controls whether event loops are automatically launched.
func WithAutoStart(autoStart bool) ClusterOption {
	return func(o *clusterOptions) {
		o.autoStart = autoStart
	}
}

func buildPeerList(nodeIDs []raft.NodeID, selfID raft.NodeID) []Peer {
	peers := make([]Peer, 0, len(nodeIDs)-1)
	for _, peerID := range nodeIDs {
		if peerID != selfID {
			peers = append(peers, Peer{
				ID:      peerID,
				Address: fmt.Sprintf("in-memory://node-%d", peerID),
			})
		}
	}
	return peers
}

func configureServerOptions(srv *Server, options clusterOptions) {
	if options.quiet {
		srv.SetLogger(logger.NewNodeLogger(srv.ID, io.Discard, nil))
	}
	if options.clock != nil {
		srv.SetClock(options.clock)
	}
	if options.randSeed != 0 {
		nodeRng := rand.New(rand.NewSource(options.randSeed + int64(srv.ID)*1000))
		srv.SetRandFn(func(min, max time.Duration) time.Duration {
			if min >= max {
				return min
			}
			delta := int64(max - min)
			return min + time.Duration(nodeRng.Int63n(delta+1))
		})
	}
	srv.SetHeartbeatInterval(options.heartbeatInterval)
	srv.SetRPCTimeout(options.rpcTimeout)
	if srv.GetElectionTimer() != nil {
		srv.GetElectionTimer().SetRange(options.minTimeout, options.maxTimeout)
		srv.ResetElectionTimeout()
	}
	if options.checkQuorum {
		srv.SetCheckQuorum(true)
		if options.checkQuorumInterval > 0 {
			srv.SetCheckQuorumInterval(options.checkQuorumInterval)
		}
	}
}

func registerPeerClients(net *Network, srv *Server, srcID raft.NodeID, peers []Peer) {
	for _, peer := range peers {
		client := &inMemoryPeerClient{
			net:   net,
			srcID: srcID,
			dstID: peer.ID,
		}
		srv.ClientPool().Register(peer.ID, client)
	}
}

func restoreStorageState(oldSrv *Server, id raft.NodeID, t testing.TB) (*raft.PersistentState, store.Storage) {
	if oldSrv == nil {
		memStorage := store.NewMemoryStorage()
		state := &raft.PersistentState{CurrentTerm: 0, VotedFor: -1}
		_ = memStorage.Persist(state)
		return state, memStorage
	}

	memStorage := oldSrv.Storage
	var loadedState *raft.PersistentState
	if memStorage != nil {
		var err error
		loadedState, err = memStorage.Load()
		if err != nil && t != nil {
			t.Logf("warning: failed to load storage for node %d: %v", id, err)
		}
	}
	if loadedState == nil && oldSrv.PersistentState != nil {
		loadedState = &raft.PersistentState{
			CurrentTerm: oldSrv.PersistentState.CurrentTerm,
			VotedFor:    oldSrv.PersistentState.VotedFor,
			Log:         append([]raft.LogEntry(nil), oldSrv.PersistentState.Log...),
		}
	}
	if loadedState == nil {
		loadedState = &raft.PersistentState{CurrentTerm: 0, VotedFor: -1}
	}
	if memStorage == nil {
		memStorage = store.NewMemoryStorage()
		_ = memStorage.Persist(loadedState)
	}
	return loadedState, memStorage
}

func resolveClusterOptions(opts ...ClusterOption) clusterOptions {
	options := clusterOptions{
		minTimeout:        100 * time.Millisecond,
		maxTimeout:        200 * time.Millisecond,
		heartbeatInterval: 20 * time.Millisecond,
		rpcTimeout:        50 * time.Millisecond,
		autoStart:         true,
	}
	for _, opt := range opts {
		opt(&options)
	}
	return options
}

func registerSimPeerClients(simNet *SimNetwork, srv *Server, srcID raft.NodeID, peers []Peer) {
	for _, peer := range peers {
		srv.ClientPool().Register(peer.ID, simNet.Client(srcID, peer.ID))
	}
}

func initClusterNodes(net *Network, simNet *SimNetwork, nodeIDs []raft.NodeID, options clusterOptions) map[raft.NodeID]*Server {
	servers := make(map[raft.NodeID]*Server, len(nodeIDs))
	for _, id := range nodeIDs {
		peers := buildPeerList(nodeIDs, id)
		srv := NewServer(id, peers, &raft.PersistentState{CurrentTerm: 0, VotedFor: -1}, nil)
		configureServerOptions(srv, options)
		servers[id] = srv
		if simNet != nil {
			simNet.RegisterNode(id, srv)
			registerSimPeerClients(simNet, srv, id, peers)
		} else {
			net.RegisterNode(id, srv)
			registerPeerClients(net, srv, id, peers)
		}
	}
	return servers
}

// NewCluster builds an in-process Raft cluster of n nodes with an in-memory fake network.
func NewCluster(t testing.TB, n int, opts ...ClusterOption) *Cluster {
	options := resolveClusterOptions(opts...)
	net := NewNetwork()
	nodeIDs := make([]raft.NodeID, n)
	for i := 1; i <= n; i++ {
		nodeIDs[i-1] = raft.NodeID(i)
	}
	servers := initClusterNodes(net, options.simNetwork, nodeIDs, options)

	c := &Cluster{
		t:          t,
		Network:    net,
		SimNetwork: options.simNetwork,
		servers:    servers,
		nodeIDs:    nodeIDs,
		options:    options,
	}

	if t != nil {
		t.Cleanup(func() {
			c.Stop()
		})
	}
	if options.autoStart {
		c.Start()
	}
	return c
}

// Start starts the event loop on all non-killed nodes.
func (c *Cluster) Start() {
	c.mu.Lock()
	defer c.mu.Unlock()
	for _, srv := range c.servers {
		if !srv.IsKilled() && !srv.loopRunning.Load() {
			srv.StartEventLoop()
		}
	}
}

// Stop stops all servers in the cluster and terminates their event loops.
func (c *Cluster) Stop() {
	c.mu.Lock()
	defer c.mu.Unlock()
	for _, srv := range c.servers {
		_ = srv.Close()
	}
}

// GetNode returns the Server instance for the specified node ID, or nil if not found.
func (c *Cluster) GetNode(id raft.NodeID) *Server {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.servers[id]
}

// Nodes returns a slice of all servers in the cluster in ID order.
func (c *Cluster) Nodes() []*Server {
	c.mu.RLock()
	defer c.mu.RUnlock()
	list := make([]*Server, 0, len(c.nodeIDs))
	for _, id := range c.nodeIDs {
		list = append(list, c.servers[id])
	}
	return list
}

// NodeIDs returns a copy of all node IDs in the cluster.
func (c *Cluster) NodeIDs() []raft.NodeID {
	c.mu.RLock()
	defer c.mu.RUnlock()
	res := make([]raft.NodeID, len(c.nodeIDs))
	copy(res, c.nodeIDs)
	return res
}

// Kill simulates a hard crash of the specified node process.
func (c *Cluster) Kill(id raft.NodeID) {
	node := c.GetNode(id)
	if node != nil {
		node.Kill()
	}
}

// Restart restarts a previously killed node process, restoring its persistent state from storage.
func (c *Cluster) Restart(id raft.NodeID) *Server {
	c.mu.Lock()
	defer c.mu.Unlock()

	oldSrv := c.servers[id]
	if oldSrv != nil && !oldSrv.IsKilled() {
		oldSrv.Kill()
	}

	loadedState, memStorage := restoreStorageState(oldSrv, id, c.t)

	var peers []Peer
	if oldSrv != nil {
		peers = oldSrv.Peers
	} else {
		peers = buildPeerList(c.nodeIDs, id)
	}

	newSrv := NewServer(id, peers, loadedState, nil)
	newSrv.Storage = memStorage
	configureServerOptions(newSrv, c.options)
	if c.SimNetwork != nil {
		c.SimNetwork.RegisterNode(id, newSrv)
		registerSimPeerClients(c.SimNetwork, newSrv, id, peers)
	} else {
		c.Network.RegisterNode(id, newSrv)
		registerPeerClients(c.Network, newSrv, id, peers)
	}

	c.servers[id] = newSrv

	newSrv.StartEventLoop()
	return newSrv
}

// LiveNodes returns a slice of all currently running (non-killed) nodes in the cluster.
func (c *Cluster) LiveNodes() []*Server {
	c.mu.RLock()
	defer c.mu.RUnlock()

	var live []*Server
	for _, id := range c.nodeIDs {
		srv := c.servers[id]
		if srv != nil && !srv.IsKilled() {
			live = append(live, srv)
		}
	}
	return live
}

// KilledNodeIDs returns a slice of all currently killed node IDs in the cluster.
func (c *Cluster) KilledNodeIDs() []raft.NodeID {
	c.mu.RLock()
	defer c.mu.RUnlock()

	var killed []raft.NodeID
	for _, id := range c.nodeIDs {
		srv := c.servers[id]
		if srv == nil || srv.IsKilled() {
			killed = append(killed, id)
		}
	}
	return killed
}

// LeaderInfo describes a currently elected leader node and its term.
type LeaderInfo struct {
	NodeID raft.NodeID
	Term   raft.Term
}

// Leaders returns a slice of all live nodes currently claiming leadership along with their terms.
func (c *Cluster) Leaders() []LeaderInfo {
	c.mu.RLock()
	defer c.mu.RUnlock()

	var leaders []LeaderInfo
	for _, id := range c.nodeIDs {
		srv := c.servers[id]
		if srv != nil && !srv.IsKilled() {
			if term, isLeader := srv.LeaderTerm(); isLeader {
				leaders = append(leaders, LeaderInfo{
					NodeID: id,
					Term:   term,
				})
			}
		}
	}
	return leaders
}

// Pause pauses the node's event loop (simulating SIGSTOP / freeze).
func (c *Cluster) Pause(id raft.NodeID) {
	node := c.GetNode(id)
	if node != nil {
		node.Pause()
	}
}

// Resume resumes a previously paused node's event loop.
func (c *Cluster) Resume(id raft.NodeID) {
	node := c.GetNode(id)
	if node != nil {
		node.Resume()
	}
}

// Leader returns the current unique leader node and term, or nil if none or multiple.
func (c *Cluster) Leader() (*Server, raft.Term) {
	c.mu.RLock()
	defer c.mu.RUnlock()

	var leader *Server
	var leaderTerm raft.Term
	for _, srv := range c.servers {
		if !srv.IsKilled() {
			if term, isLeader := srv.LeaderTerm(); isLeader {
				if leader != nil {
					return nil, 0 // split-brain or transient multiple leaders
				}
				leader = srv
				leaderTerm = term
			}
		}
	}
	if leader != nil {
		return leader, leaderTerm
	}
	return nil, 0
}

// FakeClock returns the cluster's FakeClock if one was configured, or nil.
func (c *Cluster) FakeClock() *raft.FakeClock {
	c.mu.RLock()
	defer c.mu.RUnlock()
	if fc, ok := c.options.clock.(*raft.FakeClock); ok {
		return fc
	}
	return nil
}

// Advance advances the cluster's fake clock by duration d and yields to event loops.
func (c *Cluster) Advance(d time.Duration) time.Time {
	fc := c.FakeClock()
	if fc == nil {
		return time.Now()
	}
	now := fc.Advance(d)
	for i := 0; i < 5; i++ {
		runtime.Gosched()
	}
	return now
}

// AdvanceUntil advances the cluster's fake clock until condition returns true or budget is exhausted.
func (c *Cluster) AdvanceUntil(budget time.Duration, condition func() bool) bool {
	if condition() {
		return true
	}
	fc := c.FakeClock()
	if fc == nil {
		return false
	}

	step := 5 * time.Millisecond
	elapsed := time.Duration(0)
	for elapsed < budget {
		fc.Advance(step)
		elapsed += step
		for i := 0; i < 5; i++ {
			if condition() {
				return true
			}
			runtime.Gosched()
		}
	}
	return condition()
}

// WaitLeader polls until exactly one node in the cluster is in Leader state.
func (c *Cluster) WaitLeader(timeout time.Duration) (*Server, raft.Term, error) {
	if c.FakeClock() != nil {
		var leader *Server
		var leaderTerm raft.Term
		success := c.AdvanceUntil(timeout, func() bool {
			l, term := c.Leader()
			if l != nil {
				leader = l
				leaderTerm = term
				return true
			}
			return false
		})
		if success && leader != nil {
			return leader, leaderTerm, nil
		}
		return nil, 0, fmt.Errorf("no unique leader elected within simulated %v", timeout)
	}

	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if leader, term := c.Leader(); leader != nil {
			return leader, term, nil
		}
		time.Sleep(5 * time.Millisecond)
	}
	return nil, 0, fmt.Errorf("no unique leader elected within %v", timeout)
}

// Partition disconnects all nodes in set1 from set2 bidirectionally.
func (c *Cluster) Partition(set1, set2 []raft.NodeID) {
	if c.SimNetwork != nil {
		c.SimNetwork.Partition(set1, set2)
	}
	if c.Network != nil {
		c.Network.Partition(set1, set2)
	}
}

// Heal removes all network partitions in the cluster.
func (c *Cluster) Heal() {
	if c.SimNetwork != nil {
		c.SimNetwork.Heal()
	}
	if c.Network != nil {
		c.Network.Heal()
	}
}

// Drop drops all messages from src to dst.
func (c *Cluster) Drop(src, dst raft.NodeID) {
	if c.SimNetwork != nil {
		c.SimNetwork.Drop(src, dst)
	}
	if c.Network != nil {
		c.Network.Drop(src, dst)
	}
}

// SetDelay configures latency delay between src and dst.
func (c *Cluster) SetDelay(src, dst raft.NodeID, delay time.Duration) {
	c.Network.SetDelay(src, dst, delay)
}

// SetCheckQuorum enables or disables the check-quorum mechanism for all nodes in the cluster.
func (c *Cluster) SetCheckQuorum(enabled bool) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	for _, srv := range c.servers {
		srv.SetCheckQuorum(enabled)
	}
}

// SetCheckQuorumInterval updates the check-quorum interval for all nodes in the cluster.
func (c *Cluster) SetCheckQuorumInterval(interval time.Duration) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	for _, srv := range c.servers {
		srv.SetCheckQuorumInterval(interval)
	}
}
