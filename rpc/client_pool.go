package rpc

import (
	"context"
	"fmt"
	"math"
	"net"
	"sync"
	"time"

	raftpb "raft/proto"

	"google.golang.org/grpc"
	"google.golang.org/grpc/backoff"
	"google.golang.org/grpc/connectivity"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/keepalive"
)

// RetryPolicy defines exponential backoff parameters for dialing peers.
type RetryPolicy struct {
	MaxRetries     int           `json:"max_retries"`     // Maximum number of retry attempts after initial failure
	InitialBackoff time.Duration `json:"initial_backoff"` // Initial delay before the first retry
	MaxBackoff     time.Duration `json:"max_backoff"`     // Upper bound on exponential delay
	Multiplier     float64       `json:"multiplier"`      // Exponential multiplier for each subsequent retry
	DialTimeout    time.Duration `json:"dial_timeout"`    // Per-attempt dial timeout
	ProbeTCP       bool          `json:"probe_tcp"`       // If true, probes TCP connectivity before establishing gRPC conn
}

// DefaultRetryPolicy returns standard sensible defaults for cluster peer dialing.
func DefaultRetryPolicy() RetryPolicy {
	return RetryPolicy{
		MaxRetries:     3,
		InitialBackoff: 20 * time.Millisecond,
		MaxBackoff:     500 * time.Millisecond,
		Multiplier:     2.0,
		DialTimeout:    200 * time.Millisecond,
		ProbeTCP:       true,
	}
}

// BackoffForAttempt calculates the backoff duration for a given attempt index (0-indexed).
func (r RetryPolicy) BackoffForAttempt(attempt int) time.Duration {
	if attempt <= 0 {
		return r.InitialBackoff
	}
	backoffVal := float64(r.InitialBackoff) * math.Pow(r.Multiplier, float64(attempt))
	if backoffVal > float64(r.MaxBackoff) {
		return r.MaxBackoff
	}
	return time.Duration(backoffVal)
}

// PeerConn tracks persistent connection metadata and status for a single cluster peer.
type PeerConn struct {
	ID        NodeID
	Address   string
	Conn      *grpc.ClientConn
	Client    raftpb.RaftServiceClient
	CreatedAt time.Time
}

// State returns the current gRPC connectivity state of the peer connection.
func (pc *PeerConn) State() connectivity.State {
	if pc.Conn == nil {
		return connectivity.Shutdown
	}
	return pc.Conn.GetState()
}

// IsReady returns true if the connection is currently in Ready state.
func (pc *PeerConn) IsReady() bool {
	return pc.State() == connectivity.Ready
}

// ClientPool manages persistent gRPC client connections to Raft peer nodes.
type ClientPool struct {
	mu          sync.RWMutex
	clients     map[NodeID]raftpb.RaftServiceClient
	conns       map[NodeID]*grpc.ClientConn
	peerConns   map[NodeID]*PeerConn
	addresses   map[NodeID]string
	retryPolicy RetryPolicy
	rpcTimeout  time.Duration
}

// NewClientPool creates a new ClientPool with default retry policy.
func NewClientPool() *ClientPool {
	return NewClientPoolWithPolicy(DefaultRetryPolicy())
}

// NewClientPoolWithPolicy creates a new ClientPool with a specified retry policy.
func NewClientPoolWithPolicy(policy RetryPolicy) *ClientPool {
	return &ClientPool{
		clients:     make(map[NodeID]raftpb.RaftServiceClient),
		conns:       make(map[NodeID]*grpc.ClientConn),
		peerConns:   make(map[NodeID]*PeerConn),
		addresses:   make(map[NodeID]string),
		retryPolicy: policy,
		rpcTimeout:  DefaultRPCTimeout,
	}
}

// SetRetryPolicy updates the default retry policy for subsequent dials.
func (p *ClientPool) SetRetryPolicy(policy RetryPolicy) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.retryPolicy = policy
}

// GetRetryPolicy returns a copy of the current retry policy.
func (p *ClientPool) GetRetryPolicy() RetryPolicy {
	p.mu.RLock()
	defer p.mu.RUnlock()
	return p.retryPolicy
}

// SetRPCTimeout configures the default RPC timeout for peer calls through this pool.
func (p *ClientPool) SetRPCTimeout(timeout time.Duration) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.rpcTimeout = timeout
}

// RPCTimeout returns the current default RPC timeout for this pool.
func (p *ClientPool) RPCTimeout() time.Duration {
	p.mu.RLock()
	defer p.mu.RUnlock()
	if p.rpcTimeout <= 0 {
		return DefaultRPCTimeout
	}
	return p.rpcTimeout
}

// Register registers an existing RaftServiceClient for a node ID.
// This is useful for testing or pre-configured connections.
func (p *ClientPool) Register(id NodeID, client raftpb.RaftServiceClient) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.clients[id] = client
	p.peerConns[id] = &PeerConn{
		ID:        id,
		Client:    client,
		CreatedAt: time.Now(),
	}
}

// Connect dials the peer at address with the pool's configured retry/backoff policy.
func (p *ClientPool) Connect(id NodeID, address string, opts ...grpc.DialOption) (raftpb.RaftServiceClient, error) {
	p.mu.RLock()
	policy := p.retryPolicy
	p.mu.RUnlock()
	return p.ConnectWithRetry(context.Background(), id, address, policy, opts...)
}

// ConnectWithRetry dials the peer at address with explicit retry/backoff parameters.
func (p *ClientPool) ConnectWithRetry(
	ctx context.Context,
	id NodeID,
	address string,
	policy RetryPolicy,
	opts ...grpc.DialOption,
) (raftpb.RaftServiceClient, error) {
	var lastErr error

	for attempt := 0; attempt <= policy.MaxRetries; attempt++ {
		select {
		case <-ctx.Done():
			return nil, fmt.Errorf("context cancelled during dial to peer %d at %s: %w", id, address, ctx.Err())
		default:
		}

		// Optional TCP probe to verify address is accepting connections
		if policy.ProbeTCP && address != "" {
			dialer := net.Dialer{Timeout: policy.DialTimeout}
			probeConn, err := dialer.DialContext(ctx, "tcp", address)
			if err != nil {
				lastErr = err
				if attempt < policy.MaxRetries {
					backoffDur := policy.BackoffForAttempt(attempt)
					select {
					case <-ctx.Done():
						return nil, fmt.Errorf("context cancelled during dial retry for peer %d at %s: %w", id, address, ctx.Err())
					case <-time.After(backoffDur):
						continue
					}
				}
				continue
			}
			_ = probeConn.Close()
		}

		// Establish persistent gRPC connection
		client, err := p.connectDirect(id, address, opts...)
		if err == nil {
			return client, nil
		}

		lastErr = err
		if attempt < policy.MaxRetries {
			backoffDur := policy.BackoffForAttempt(attempt)
			select {
			case <-ctx.Done():
				return nil, fmt.Errorf("context cancelled during dial retry for peer %d at %s: %w", id, address, ctx.Err())
			case <-time.After(backoffDur):
			}
		}
	}

	return nil, fmt.Errorf("failed to dial peer %d at %s after %d retries: %w", id, address, policy.MaxRetries, lastErr)
}

// connectDirect establishes and stores the persistent gRPC ClientConn and client stub.
func (p *ClientPool) connectDirect(id NodeID, address string, opts ...grpc.DialOption) (raftpb.RaftServiceClient, error) {
	p.mu.Lock()
	defer p.mu.Unlock()

	// Close existing connection if one is present
	if oldConn, exists := p.conns[id]; exists {
		_ = oldConn.Close()
	}

	dialOpts := make([]grpc.DialOption, 0, len(opts)+3)
	dialOpts = append(dialOpts, opts...)

	if len(opts) == 0 {
		dialOpts = append(dialOpts, grpc.WithTransportCredentials(insecure.NewCredentials()))
	}

	// Persistent keepalive & backoff configuration
	policy := p.retryPolicy

	dialOpts = append(dialOpts,
		grpc.WithKeepaliveParams(keepalive.ClientParameters{
			Time:                10 * time.Second,
			Timeout:             3 * time.Second,
			PermitWithoutStream: true,
		}),
		grpc.WithConnectParams(grpc.ConnectParams{
			Backoff: backoff.Config{
				BaseDelay:  policy.InitialBackoff,
				Multiplier: policy.Multiplier,
				MaxDelay:   policy.MaxBackoff,
			},
		}),
	)

	conn, err := grpc.NewClient(address, dialOpts...)
	if err != nil {
		return nil, fmt.Errorf("failed to create gRPC client for peer %d at %s: %w", id, address, err)
	}

	// Eagerly initiate connection in background to maintain persistence
	conn.Connect()

	client := raftpb.NewRaftServiceClient(conn)
	p.conns[id] = conn
	p.clients[id] = client
	p.addresses[id] = address
	p.peerConns[id] = &PeerConn{
		ID:        id,
		Address:   address,
		Conn:      conn,
		Client:    client,
		CreatedAt: time.Now(),
	}

	return client, nil
}

// Get retrieves the RaftServiceClient for the given node ID.
func (p *ClientPool) Get(id NodeID) (raftpb.RaftServiceClient, bool) {
	p.mu.RLock()
	defer p.mu.RUnlock()
	client, exists := p.clients[id]
	return client, exists
}

// GetPeerConn retrieves the PeerConn metadata for the given node ID.
func (p *ClientPool) GetPeerConn(id NodeID) (*PeerConn, bool) {
	p.mu.RLock()
	defer p.mu.RUnlock()
	pc, exists := p.peerConns[id]
	return pc, exists
}

// GetAddress retrieves the registered address for a peer ID.
func (p *ClientPool) GetAddress(id NodeID) (string, bool) {
	p.mu.RLock()
	defer p.mu.RUnlock()
	addr, exists := p.addresses[id]
	return addr, exists
}

// GetState returns the connectivity state for the given peer ID.
func (p *ClientPool) GetState(id NodeID) (connectivity.State, bool) {
	p.mu.RLock()
	defer p.mu.RUnlock()
	pc, exists := p.peerConns[id]
	if !exists || pc.Conn == nil {
		return connectivity.Shutdown, false
	}
	return pc.Conn.GetState(), true
}

// IsConnected returns true if the connection to the peer exists and is not Shutdown.
func (p *ClientPool) IsConnected(id NodeID) bool {
	state, exists := p.GetState(id)
	return exists && state != connectivity.Shutdown
}

// GetOrCreate returns an existing client or connects to the given peer address.
func (p *ClientPool) GetOrCreate(id NodeID, address string, opts ...grpc.DialOption) (raftpb.RaftServiceClient, error) {
	p.mu.RLock()
	client, exists := p.clients[id]
	p.mu.RUnlock()
	if exists {
		return client, nil
	}
	return p.Connect(id, address, opts...)
}

// Reconnect reconnects to an existing peer using its stored address and retry policy.
func (p *ClientPool) Reconnect(id NodeID, opts ...grpc.DialOption) (raftpb.RaftServiceClient, error) {
	p.mu.RLock()
	addr, exists := p.addresses[id]
	p.mu.RUnlock()

	if !exists || addr == "" {
		return nil, fmt.Errorf("cannot reconnect: address for peer %d is unknown", id)
	}

	return p.Connect(id, addr, opts...)
}

// ClosePeer closes the connection to the specified peer and removes it from the pool.
func (p *ClientPool) ClosePeer(id NodeID) error {
	p.mu.Lock()
	defer p.mu.Unlock()

	delete(p.clients, id)
	delete(p.peerConns, id)
	delete(p.addresses, id)
	if conn, exists := p.conns[id]; exists {
		delete(p.conns, id)
		return conn.Close()
	}
	return nil
}

// Close closes all client connections in the pool.
func (p *ClientPool) Close() error {
	p.mu.Lock()
	defer p.mu.Unlock()

	var firstErr error
	for id, conn := range p.conns {
		if err := conn.Close(); err != nil && firstErr == nil {
			firstErr = fmt.Errorf("failed to close connection to peer %d: %w", id, err)
		}
	}
	p.clients = make(map[NodeID]raftpb.RaftServiceClient)
	p.conns = make(map[NodeID]*grpc.ClientConn)
	p.peerConns = make(map[NodeID]*PeerConn)
	p.addresses = make(map[NodeID]string)
	return firstErr
}

// Peers returns the list of node IDs currently managed in the pool.
func (p *ClientPool) Peers() []NodeID {
	p.mu.RLock()
	defer p.mu.RUnlock()

	peers := make([]NodeID, 0, len(p.clients))
	for id := range p.clients {
		peers = append(peers, id)
	}
	return peers
}

