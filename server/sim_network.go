package server

import (
	"context"
	"fmt"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	raftpb "raft/proto"
	"raft/raft"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

type linkKey struct {
	src raft.NodeID
	dst raft.NodeID
}

type delayRange struct {
	min time.Duration
	max time.Duration
}

func (dr delayRange) sample(rng *rand.Rand) time.Duration {
	if dr.max <= 0 || dr.min < 0 {
		return 0
	}
	if dr.max <= dr.min {
		return dr.min
	}
	delta := int64(dr.max - dr.min)
	return dr.min + time.Duration(rng.Int63n(delta+1))
}

// Packet represents an arbitrary network packet routed through SimNetwork.
type Packet struct {
	ID          uint64
	Src         raft.NodeID
	Dst         raft.NodeID
	Type        string
	Payload     any
	SentAt      time.Time
	DeliveredAt time.Time
	IsDuplicate bool
}

// SimNetwork simulates a realistic, lossy network environment driven by a seeded math/rand source.
// It supports per-message delay ranges, probabilistic drops, packet duplication, and reordering,
// along with multi-group network partitions and healing.
type SimNetwork struct {
	mu sync.RWMutex

	rng   *rand.Rand
	seed  int64
	clock raft.Clock

	nodes    map[raft.NodeID]*Server
	channels map[raft.NodeID]chan *Packet

	partitions map[raft.NodeID]map[raft.NodeID]bool
	drops      map[raft.NodeID]map[raft.NodeID]bool

	globalDelay delayRange
	linkDelays  map[linkKey]delayRange

	globalDropProb float64
	linkDropProbs  map[linkKey]float64

	globalDupProb float64
	linkDupProbs  map[linkKey]float64

	globalReorderProb float64
	linkReorderProbs  map[linkKey]float64
	reorderDelayRange delayRange

	totalSent       atomic.Uint64
	totalDelivered  atomic.Uint64
	totalDropped    atomic.Uint64
	totalDuplicated atomic.Uint64
	totalReordered  atomic.Uint64
}

// NewSimNetwork creates a new SimNetwork driven deterministically by the provided seed.
func NewSimNetwork(seed int64) *SimNetwork {
	if seed == 0 {
		seed = 1
	}
	return &SimNetwork{
		rng:               rand.New(rand.NewSource(seed)),
		seed:              seed,
		clock:             raft.NewRealClock(),
		nodes:             make(map[raft.NodeID]*Server),
		channels:          make(map[raft.NodeID]chan *Packet),
		partitions:        make(map[raft.NodeID]map[raft.NodeID]bool),
		drops:             make(map[raft.NodeID]map[raft.NodeID]bool),
		linkDelays:        make(map[linkKey]delayRange),
		linkDropProbs:     make(map[linkKey]float64),
		linkDupProbs:      make(map[linkKey]float64),
		linkReorderProbs:  make(map[linkKey]float64),
		reorderDelayRange: delayRange{min: 10 * time.Millisecond, max: 30 * time.Millisecond},
	}
}

// Reseed resets the pseudo-random generator with a new seed.
func (n *SimNetwork) Reseed(seed int64) {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.seed = seed
	n.rng = rand.New(rand.NewSource(seed))
}

// SetClock configures the Clock abstraction used for scheduling network delays.
func (n *SimNetwork) SetClock(clk raft.Clock) {
	n.mu.Lock()
	defer n.mu.Unlock()
	if clk == nil {
		clk = raft.NewRealClock()
	}
	n.clock = clk
}

func (n *SimNetwork) getClock() raft.Clock {
	n.mu.RLock()
	defer n.mu.RUnlock()
	if n.clock == nil {
		return raft.NewRealClock()
	}
	return n.clock
}

// SetDelayRange sets the global min/max latency delay range across all links.
func (n *SimNetwork) SetDelayRange(min, max time.Duration) {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.globalDelay = delayRange{min: min, max: max}
}

// SetLinkDelayRange sets a custom latency delay range for communication from src to dst.
func (n *SimNetwork) SetLinkDelayRange(src, dst raft.NodeID, min, max time.Duration) {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.linkDelays[linkKey{src, dst}] = delayRange{min: min, max: max}
}

// ClearDelayRanges clears all global and per-link delay ranges.
func (n *SimNetwork) ClearDelayRanges() {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.globalDelay = delayRange{}
	n.linkDelays = make(map[linkKey]delayRange)
}

// SetDropProbability sets the network-wide probability of dropping a message (0.0 <= p <= 1.0).
func (n *SimNetwork) SetDropProbability(prob float64) {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.globalDropProb = clampProb(prob)
}

// SetLinkDropProbability sets the drop probability for messages sent from src to dst.
func (n *SimNetwork) SetLinkDropProbability(src, dst raft.NodeID, prob float64) {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.linkDropProbs[linkKey{src, dst}] = clampProb(prob)
}

// ClearDropProbabilities resets all drop probabilities to zero.
func (n *SimNetwork) ClearDropProbabilities() {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.globalDropProb = 0
	n.linkDropProbs = make(map[linkKey]float64)
}

// SetDuplicationProbability sets the network-wide probability of duplicating a message.
func (n *SimNetwork) SetDuplicationProbability(prob float64) {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.globalDupProb = clampProb(prob)
}

// SetLinkDuplicationProbability sets duplication probability for messages sent from src to dst.
func (n *SimNetwork) SetLinkDuplicationProbability(src, dst raft.NodeID, prob float64) {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.linkDupProbs[linkKey{src, dst}] = clampProb(prob)
}

// ClearDuplicationProbabilities resets all duplication probabilities to zero.
func (n *SimNetwork) ClearDuplicationProbabilities() {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.globalDupProb = 0
	n.linkDupProbs = make(map[linkKey]float64)
}

// SetReorderProbability sets the probability of delaying a packet to induce out-of-order delivery.
func (n *SimNetwork) SetReorderProbability(prob float64) {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.globalReorderProb = clampProb(prob)
}

// SetReorderDelayRange configures the extra delay range injected when a packet is reordered.
func (n *SimNetwork) SetReorderDelayRange(min, max time.Duration) {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.reorderDelayRange = delayRange{min: min, max: max}
}

// Partition partitions the network by isolating groupA from groupB bidirectionally.
func (n *SimNetwork) Partition(groupA, groupB []raft.NodeID) {
	n.mu.Lock()
	defer n.mu.Unlock()
	for _, a := range groupA {
		if n.partitions[a] == nil {
			n.partitions[a] = make(map[raft.NodeID]bool)
		}
		for _, b := range groupB {
			n.partitions[a][b] = true
			if n.partitions[b] == nil {
				n.partitions[b] = make(map[raft.NodeID]bool)
			}
			n.partitions[b][a] = true
		}
	}
}

// Heal removes all active partitions, restoring full network connectivity.
func (n *SimNetwork) Heal() {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.partitions = make(map[raft.NodeID]map[raft.NodeID]bool)
}

// IsPartitioned returns true if communication from src to dst is partitioned.
func (n *SimNetwork) IsPartitioned(src, dst raft.NodeID) bool {
	n.mu.RLock()
	defer n.mu.RUnlock()
	return n.isBlockedLocked(src, dst)
}

// Drop drops all messages sent from src to dst unconditionally.
func (n *SimNetwork) Drop(src, dst raft.NodeID) {
	n.mu.Lock()
	defer n.mu.Unlock()
	if n.drops[src] == nil {
		n.drops[src] = make(map[raft.NodeID]bool)
	}
	n.drops[src][dst] = true
}

// DropBidirectional drops communication between n1 and n2 in both directions.
func (n *SimNetwork) DropBidirectional(n1, n2 raft.NodeID) {
	n.Drop(n1, n2)
	n.Drop(n2, n1)
}

// Undrop restores unconditional delivery from src to dst.
func (n *SimNetwork) Undrop(src, dst raft.NodeID) {
	n.mu.Lock()
	defer n.mu.Unlock()
	if n.drops[src] != nil {
		delete(n.drops[src], dst)
	}
}

// ClearDrops removes all unconditional drop rules.
func (n *SimNetwork) ClearDrops() {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.drops = make(map[raft.NodeID]map[raft.NodeID]bool)
}

func (n *SimNetwork) isBlockedLocked(src, dst raft.NodeID) bool {
	if n.partitions[src] != nil && n.partitions[src][dst] {
		return true
	}
	if n.drops[src] != nil && n.drops[src][dst] {
		return true
	}
	return false
}

func (n *SimNetwork) computeDeliveryDelay(src, dst raft.NodeID) (time.Duration, bool) {
	n.mu.Lock()
	defer n.mu.Unlock()

	dr, hasLink := n.linkDelays[linkKey{src, dst}]
	if !hasLink {
		dr = n.globalDelay
	}
	delay := dr.sample(n.rng)

	prob, hasReorder := n.linkReorderProbs[linkKey{src, dst}]
	if !hasReorder {
		prob = n.globalReorderProb
	}

	reordered := false
	if prob > 0 && n.rng.Float64() < prob {
		reordered = true
		extra := n.reorderDelayRange.sample(n.rng)
		if extra <= 0 {
			extra = 15 * time.Millisecond
		}
		delay += extra
	}
	return delay, reordered
}

func (n *SimNetwork) checkDrop(src, dst raft.NodeID) bool {
	n.mu.Lock()
	defer n.mu.Unlock()

	if n.isBlockedLocked(src, dst) {
		return true
	}
	prob, hasProb := n.linkDropProbs[linkKey{src, dst}]
	if !hasProb {
		prob = n.globalDropProb
	}
	return prob > 0 && n.rng.Float64() < prob
}

func (n *SimNetwork) checkDuplicate(src, dst raft.NodeID) bool {
	n.mu.Lock()
	defer n.mu.Unlock()

	prob, hasProb := n.linkDupProbs[linkKey{src, dst}]
	if !hasProb {
		prob = n.globalDupProb
	}
	return prob > 0 && n.rng.Float64() < prob
}

func (n *SimNetwork) waitDelay(ctx context.Context, delay time.Duration) error {
	if delay <= 0 {
		return nil
	}
	select {
	case <-n.getClock().After(delay):
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// RegisterChannel creates an inbound message buffer for packet passing to node id.
func (n *SimNetwork) RegisterChannel(id raft.NodeID, bufferSize int) {
	n.mu.Lock()
	defer n.mu.Unlock()
	if bufferSize <= 0 {
		bufferSize = 64
	}
	n.channels[id] = make(chan *Packet, bufferSize)
}

// ReceiveChan returns the packet channel for node id.
func (n *SimNetwork) ReceiveChan(id raft.NodeID) <-chan *Packet {
	n.mu.RLock()
	defer n.mu.RUnlock()
	return n.channels[id]
}

// Send sends a packet through SimNetwork, simulating latency, drops, duplicates, and reordering.
func (n *SimNetwork) Send(pkt *Packet) error {
	return n.SendWithContext(context.Background(), pkt)
}

// SendAsync dispatches a packet asynchronously in a background goroutine.
func (n *SimNetwork) SendAsync(pkt *Packet) <-chan error {
	errCh := make(chan error, 1)
	go func() {
		errCh <- n.Send(pkt)
	}()
	return errCh
}

// SendWithContext sends a packet observing context cancellation.
func (n *SimNetwork) SendWithContext(ctx context.Context, pkt *Packet) error {
	n.totalSent.Add(1)
	if n.checkDrop(pkt.Src, pkt.Dst) {
		n.totalDropped.Add(1)
		return fmt.Errorf("packet dropped from %d to %d", pkt.Src, pkt.Dst)
	}

	delay, reordered := n.computeDeliveryDelay(pkt.Src, pkt.Dst)
	if reordered {
		n.totalReordered.Add(1)
	}

	if n.checkDuplicate(pkt.Src, pkt.Dst) {
		n.totalDuplicated.Add(1)
		dupPkt := *pkt
		dupPkt.IsDuplicate = true
		go func() { _ = n.dispatchPacket(context.Background(), &dupPkt, delay+5*time.Millisecond) }()
	}

	return n.dispatchPacket(ctx, pkt, delay)
}

func (n *SimNetwork) dispatchPacket(ctx context.Context, pkt *Packet, delay time.Duration) error {
	if err := n.waitDelay(ctx, delay); err != nil {
		return err
	}

	n.mu.RLock()
	ch := n.channels[pkt.Dst]
	n.mu.RUnlock()

	if ch == nil {
		return fmt.Errorf("channel not registered for node %d", pkt.Dst)
	}

	pkt.DeliveredAt = n.getClock().Now()
	select {
	case ch <- pkt:
		n.totalDelivered.Add(1)
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// RegisterNode registers a Raft Server to handle inbound RPCs.
func (n *SimNetwork) RegisterNode(id raft.NodeID, srv *Server) {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.nodes[id] = srv
}

func (n *SimNetwork) getTargetNode(dst raft.NodeID) (*Server, error) {
	n.mu.RLock()
	targetNode := n.nodes[dst]
	n.mu.RUnlock()

	if targetNode == nil || targetNode.IsKilled() {
		return nil, status.Errorf(codes.Unavailable, "simnet: target node %d is unavailable or killed", dst)
	}
	return targetNode, nil
}

func (n *SimNetwork) prepareOutbound(ctx context.Context, src, dst raft.NodeID, req proto.Message) (*Server, error) {
	n.totalSent.Add(1)
	if n.checkDrop(src, dst) {
		n.totalDropped.Add(1)
		return nil, status.Errorf(codes.Unavailable, "simnet: request dropped from %d to %d", src, dst)
	}

	delay, reordered := n.computeDeliveryDelay(src, dst)
	if reordered {
		n.totalReordered.Add(1)
	}
	if err := n.waitDelay(ctx, delay); err != nil {
		return nil, err
	}

	targetNode, err := n.getTargetNode(dst)
	if err != nil {
		return nil, err
	}

	if n.checkDuplicate(src, dst) {
		n.totalDuplicated.Add(1)
		n.dispatchRPCDuplicate(targetNode, req)
	}
	return targetNode, nil
}

func (n *SimNetwork) prepareInbound(ctx context.Context, dst, src raft.NodeID) error {
	if n.checkDrop(dst, src) {
		n.totalDropped.Add(1)
		return status.Errorf(codes.Unavailable, "simnet: reply dropped from %d to %d", dst, src)
	}
	returnDelay, _ := n.computeDeliveryDelay(dst, src)
	return n.waitDelay(ctx, returnDelay)
}

func (n *SimNetwork) dispatchRPCDuplicate(target *Server, req proto.Message) {
	switch r := req.(type) {
	case *raftpb.RequestVoteArgs:
		cloned := proto.Clone(r).(*raftpb.RequestVoteArgs)
		go func() { _, _ = target.RequestVote(context.Background(), cloned) }()
	case *raftpb.AppendEntriesArgs:
		cloned := proto.Clone(r).(*raftpb.AppendEntriesArgs)
		go func() { _, _ = target.AppendEntries(context.Background(), cloned) }()
	}
}

// SendRequestVote routes a RequestVote RPC through the simulated lossy network.
func (n *SimNetwork) SendRequestVote(ctx context.Context, src, dst raft.NodeID, req *raftpb.RequestVoteArgs) (*raftpb.RequestVoteReply, error) {
	targetNode, err := n.prepareOutbound(ctx, src, dst, req)
	if err != nil {
		return nil, err
	}

	clonedReq := proto.Clone(req).(*raftpb.RequestVoteArgs)
	clonedReq.PreVote = req.PreVote
	clonedReq.LeadershipTransfer = req.LeadershipTransfer
	reply, err := targetNode.RequestVote(ctx, clonedReq)
	if err != nil {
		return nil, err
	}

	if err := n.prepareInbound(ctx, dst, src); err != nil {
		return nil, err
	}

	n.totalDelivered.Add(1)
	return proto.Clone(reply).(*raftpb.RequestVoteReply), nil
}

// SendAppendEntries routes an AppendEntries RPC through the simulated lossy network.
func (n *SimNetwork) SendAppendEntries(ctx context.Context, src, dst raft.NodeID, req *raftpb.AppendEntriesArgs) (*raftpb.AppendEntriesReply, error) {
	targetNode, err := n.prepareOutbound(ctx, src, dst, req)
	if err != nil {
		return nil, err
	}

	clonedReq := proto.Clone(req).(*raftpb.AppendEntriesArgs)
	reply, err := targetNode.AppendEntries(ctx, clonedReq)
	if err != nil {
		return nil, err
	}

	if err := n.prepareInbound(ctx, dst, src); err != nil {
		return nil, err
	}

	n.totalDelivered.Add(1)
	return proto.Clone(reply).(*raftpb.AppendEntriesReply), nil
}

// Client returns a gRPC client stub for RPCs between src and dst.
func (n *SimNetwork) Client(src, dst raft.NodeID) raftpb.RaftServiceClient {
	return &simPeerClient{net: n, srcID: src, dstID: dst}
}

type simPeerClient struct {
	net   *SimNetwork
	srcID raft.NodeID
	dstID raft.NodeID
}

func (c *simPeerClient) RequestVote(ctx context.Context, in *raftpb.RequestVoteArgs, opts ...grpc.CallOption) (*raftpb.RequestVoteReply, error) {
	return c.net.SendRequestVote(ctx, c.srcID, c.dstID, in)
}

func (c *simPeerClient) AppendEntries(ctx context.Context, in *raftpb.AppendEntriesArgs, opts ...grpc.CallOption) (*raftpb.AppendEntriesReply, error) {
	return c.net.SendAppendEntries(ctx, c.srcID, c.dstID, in)
}

// TotalSent returns total number of messages sent into SimNetwork.
func (n *SimNetwork) TotalSent() uint64 {
	return n.totalSent.Load()
}

// TotalDelivered returns total number of messages successfully delivered.
func (n *SimNetwork) TotalDelivered() uint64 {
	return n.totalDelivered.Load()
}

// TotalDropped returns total number of messages dropped.
func (n *SimNetwork) TotalDropped() uint64 {
	return n.totalDropped.Load()
}

// TotalDuplicated returns total number of duplicated messages dispatched.
func (n *SimNetwork) TotalDuplicated() uint64 {
	return n.totalDuplicated.Load()
}

// TotalReordered returns total number of messages subjected to reorder delay.
func (n *SimNetwork) TotalReordered() uint64 {
	return n.totalReordered.Load()
}

// ResetMetrics resets all internal message counters.
func (n *SimNetwork) ResetMetrics() {
	n.totalSent.Store(0)
	n.totalDelivered.Store(0)
	n.totalDropped.Store(0)
	n.totalDuplicated.Store(0)
	n.totalReordered.Store(0)
}

func clampProb(prob float64) float64 {
	if prob < 0 {
		return 0
	}
	if prob > 1 {
		return 1
	}
	return prob
}
