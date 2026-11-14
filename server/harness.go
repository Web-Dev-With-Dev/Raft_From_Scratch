package server

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	raftpb "raft/proto"
	"raft/raft"

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

// sendRequestVote routes a RequestVote RPC from src to dst in-memory.
func (n *Network) sendRequestVote(ctx context.Context, src, dst raft.NodeID, req *raftpb.RequestVoteArgs) (*raftpb.RequestVoteReply, error) {
	n.totalSent.Add(1)

	n.mu.RLock()
	blocked := n.isBlockedLocked(src, dst)
	delay := n.getDelayLocked(src, dst)
	filters := n.filters
	targetNode := n.nodes[dst]
	n.mu.RUnlock()

	// Evaluate custom filters
	for _, f := range filters {
		if f(src, dst, RPCRequestVote, req) {
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

	// Deep-copy request to ensure true wire memory isolation
	clonedReq := proto.Clone(req).(*raftpb.RequestVoteArgs)

	reply, err := targetNode.RequestVote(ctx, clonedReq)
	if err != nil {
		return nil, err
	}

	// Check if return path is partitioned
	n.mu.RLock()
	returnBlocked := n.partitions[dst] != nil && n.partitions[dst][src]
	returnDelay := n.getDelayLocked(dst, src)
	n.mu.RUnlock()

	if returnBlocked {
		n.totalDropped.Add(1)
		return nil, status.Errorf(codes.Unavailable, "network: reply dropped by partition (node %d to %d)", dst, src)
	}

	if returnDelay > 0 {
		select {
		case <-time.After(returnDelay):
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}

	// Deep-copy reply
	clonedReply := proto.Clone(reply).(*raftpb.RequestVoteReply)
	return clonedReply, nil
}

// sendAppendEntries routes an AppendEntries RPC from src to dst in-memory.
func (n *Network) sendAppendEntries(ctx context.Context, src, dst raft.NodeID, req *raftpb.AppendEntriesArgs) (*raftpb.AppendEntriesReply, error) {
	n.totalSent.Add(1)

	n.mu.RLock()
	blocked := n.isBlockedLocked(src, dst)
	delay := n.getDelayLocked(src, dst)
	filters := n.filters
	targetNode := n.nodes[dst]
	n.mu.RUnlock()

	for _, f := range filters {
		if f(src, dst, RPCAppendEntries, req) {
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

	// Deep-copy request
	clonedReq := proto.Clone(req).(*raftpb.AppendEntriesArgs)

	reply, err := targetNode.AppendEntries(ctx, clonedReq)
	if err != nil {
		return nil, err
	}

	// Check if return path is partitioned
	n.mu.RLock()
	returnBlocked := n.partitions[dst] != nil && n.partitions[dst][src]
	returnDelay := n.getDelayLocked(dst, src)
	n.mu.RUnlock()

	if returnBlocked {
		n.totalDropped.Add(1)
		return nil, status.Errorf(codes.Unavailable, "network: reply dropped by partition (node %d to %d)", dst, src)
	}

	if returnDelay > 0 {
		select {
		case <-time.After(returnDelay):
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}

	clonedReply := proto.Clone(reply).(*raftpb.AppendEntriesReply)
	return clonedReply, nil
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
	t       testing.TB
	Network *Network
	servers map[raft.NodeID]*Server
	nodeIDs []raft.NodeID
	options clusterOptions
	mu      sync.RWMutex
}

type clusterOptions struct {
	minTimeout        time.Duration
	maxTimeout        time.Duration
	heartbeatInterval time.Duration
	rpcTimeout        time.Duration
	autoStart         bool
}

// ClusterOption configures test cluster parameters.
type ClusterOption func(*clusterOptions)

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

// WithAutoStart controls whether event loops are automatically launched.
func WithAutoStart(autoStart bool) ClusterOption {
	return func(o *clusterOptions) {
		o.autoStart = autoStart
	}
}

// NewCluster builds an in-process Raft cluster of n nodes with an in-memory fake network.
func NewCluster(t testing.TB, n int, opts ...ClusterOption) *Cluster {
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

	net := NewNetwork()
	nodeIDs := make([]raft.NodeID, n)
	servers := make(map[raft.NodeID]*Server, n)

	// Step 1: Create all server instances with full mesh peer metadata
	for i := 1; i <= n; i++ {
		id := raft.NodeID(i)
		nodeIDs[i-1] = id

		peers := make([]Peer, 0, n-1)
		for j := 1; j <= n; j++ {
			if i != j {
				peers = append(peers, Peer{
					ID:      raft.NodeID(j),
					Address: fmt.Sprintf("in-memory://node-%d", j),
				})
			}
		}

		srv := NewServer(id, peers, &raft.PersistentState{CurrentTerm: 0, VotedFor: -1}, nil)
		srv.SetHeartbeatInterval(options.heartbeatInterval)
		srv.SetRPCTimeout(options.rpcTimeout)
		if srv.GetElectionTimer() != nil {
			srv.GetElectionTimer().SetRange(options.minTimeout, options.maxTimeout)
			srv.ResetElectionTimeout()
		}

		servers[id] = srv
		net.RegisterNode(id, srv)
	}

	// Step 2: Register in-memory fake clients in each node's RPCClientPool
	for i := 1; i <= n; i++ {
		srcID := raft.NodeID(i)
		srcServer := servers[srcID]
		for j := 1; j <= n; j++ {
			if i != j {
				dstID := raft.NodeID(j)
				client := &inMemoryPeerClient{
					net:   net,
					srcID: srcID,
					dstID: dstID,
				}
				srcServer.ClientPool().Register(dstID, client)
			}
		}
	}

	c := &Cluster{
		t:       t,
		Network: net,
		servers: servers,
		nodeIDs: nodeIDs,
		options: options,
	}

	if t != nil {
		t.Cleanup(func() {
			c.Stop()
		})
	}

	// Step 3: Start event loops if autoStart is true
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
	for _, srv := range c.servers {
		if !srv.IsKilled() && srv.GetState() == raft.Leader {
			if leader != nil {
				return nil, 0 // split-brain or transient multiple leaders
			}
			leader = srv
		}
	}
	if leader != nil {
		return leader, leader.Status().Term
	}
	return nil, 0
}

// WaitLeader polls until exactly one node in the cluster is in Leader state.
func (c *Cluster) WaitLeader(timeout time.Duration) (*Server, raft.Term, error) {
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
	c.Network.Partition(set1, set2)
}

// Heal removes all network partitions in the cluster.
func (c *Cluster) Heal() {
	c.Network.Heal()
}

// Drop drops all messages from src to dst.
func (c *Cluster) Drop(src, dst raft.NodeID) {
	c.Network.Drop(src, dst)
}

// SetDelay configures latency delay between src and dst.
func (c *Cluster) SetDelay(src, dst raft.NodeID, delay time.Duration) {
	c.Network.SetDelay(src, dst, delay)
}
