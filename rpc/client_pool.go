package rpc

import (
	"fmt"
	"sync"

	raftpb "raft/proto"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// ClientPool manages gRPC client connections to Raft peer nodes.
type ClientPool struct {
	mu      sync.RWMutex
	clients map[NodeID]raftpb.RaftServiceClient
	conns   map[NodeID]*grpc.ClientConn
}

// NewClientPool creates a new empty ClientPool.
func NewClientPool() *ClientPool {
	return &ClientPool{
		clients: make(map[NodeID]raftpb.RaftServiceClient),
		conns:   make(map[NodeID]*grpc.ClientConn),
	}
}

// Register registers an existing RaftServiceClient for a node ID.
// This is useful for testing or pre-configured connections.
func (p *ClientPool) Register(id NodeID, client raftpb.RaftServiceClient) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.clients[id] = client
}

// Connect dials the peer at address and registers the gRPC client in the pool.
func (p *ClientPool) Connect(id NodeID, address string, opts ...grpc.DialOption) (raftpb.RaftServiceClient, error) {
	p.mu.Lock()
	defer p.mu.Unlock()

	// Close existing connection if one is present
	if oldConn, exists := p.conns[id]; exists {
		_ = oldConn.Close()
	}

	if len(opts) == 0 {
		opts = append(opts, grpc.WithTransportCredentials(insecure.NewCredentials()))
	}

	conn, err := grpc.NewClient(address, opts...)
	if err != nil {
		return nil, fmt.Errorf("failed to dial peer %d at %s: %w", id, address, err)
	}

	client := raftpb.NewRaftServiceClient(conn)
	p.conns[id] = conn
	p.clients[id] = client

	return client, nil
}

// Get retrieves the RaftServiceClient for the given node ID.
func (p *ClientPool) Get(id NodeID) (raftpb.RaftServiceClient, bool) {
	p.mu.RLock()
	defer p.mu.RUnlock()
	client, exists := p.clients[id]
	return client, exists
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

// ClosePeer closes the connection to the specified peer and removes it from the pool.
func (p *ClientPool) ClosePeer(id NodeID) error {
	p.mu.Lock()
	defer p.mu.Unlock()

	delete(p.clients, id)
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
