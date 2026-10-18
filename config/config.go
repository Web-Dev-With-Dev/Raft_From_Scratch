package config

import (
	"encoding/json"
	"fmt"
	"os"
	"strings"

	"raft/raft"
)

// DefaultClusterConfigFile is the default file path for the cluster configuration.
const DefaultClusterConfigFile = "cluster.json"

// NodeConfig represents a single cluster node's ID and gRPC network address.
type NodeConfig struct {
	ID      raft.NodeID `json:"id"`
	Address string      `json:"address"`
}

// ToPeer converts NodeConfig into a raft.Peer.
func (n NodeConfig) ToPeer() raft.Peer {
	return raft.Peer{
		ID:      n.ID,
		Address: n.Address,
	}
}

// ClusterConfig contains the list of nodes comprising the Raft cluster.
type ClusterConfig struct {
	Nodes []NodeConfig `json:"nodes"`
}

// Validate checks that the cluster configuration is non-empty, contains valid addresses,
// and has no duplicate node IDs.
func (c *ClusterConfig) Validate() error {
	if len(c.Nodes) == 0 {
		return fmt.Errorf("cluster config must contain at least one node")
	}

	seen := make(map[raft.NodeID]bool, len(c.Nodes))
	for _, node := range c.Nodes {
		if strings.TrimSpace(node.Address) == "" {
			return fmt.Errorf("node %d has empty address", node.ID)
		}
		if seen[node.ID] {
			return fmt.Errorf("duplicate node id %d in cluster config", node.ID)
		}
		seen[node.ID] = true
	}
	return nil
}

// Peers returns all cluster nodes as a slice of raft.Peer.
func (c *ClusterConfig) Peers() []raft.Peer {
	peers := make([]raft.Peer, len(c.Nodes))
	for i, node := range c.Nodes {
		peers[i] = node.ToPeer()
	}
	return peers
}

// PeersFor returns all cluster peers excluding the specified self node ID.
// This is typically passed to Server initialization so a node only stores its remote peers.
func (c *ClusterConfig) PeersFor(selfID raft.NodeID) []raft.Peer {
	peers := make([]raft.Peer, 0, len(c.Nodes)-1)
	for _, node := range c.Nodes {
		if node.ID != selfID {
			peers = append(peers, node.ToPeer())
		}
	}
	return peers
}

// PeerIDs returns a slice of all node IDs defined in the configuration.
func (c *ClusterConfig) PeerIDs() []raft.NodeID {
	ids := make([]raft.NodeID, len(c.Nodes))
	for i, node := range c.Nodes {
		ids[i] = node.ID
	}
	return ids
}

// GetNode retrieves the NodeConfig for a specific node ID.
func (c *ClusterConfig) GetNode(id raft.NodeID) (NodeConfig, bool) {
	for _, node := range c.Nodes {
		if node.ID == id {
			return node, true
		}
	}
	return NodeConfig{}, false
}

// ParseClusterConfig parses JSON bytes into a ClusterConfig.
// It supports both an object with a "nodes" field ({"nodes": [...]})
// and a top-level JSON array ([{...}]).
func ParseClusterConfig(data []byte) (*ClusterConfig, error) {
	// 1. Try parsing as {"nodes": [...]}
	var objCfg ClusterConfig
	if err := json.Unmarshal(data, &objCfg); err == nil && len(objCfg.Nodes) > 0 {
		if err := objCfg.Validate(); err != nil {
			return nil, err
		}
		return &objCfg, nil
	}

	// 2. Try parsing as direct array [{...}]
	var nodes []NodeConfig
	if err := json.Unmarshal(data, &nodes); err == nil {
		cfg := &ClusterConfig{Nodes: nodes}
		if err := cfg.Validate(); err != nil {
			return nil, err
		}
		return cfg, nil
	}

	return nil, fmt.Errorf("invalid cluster config format: expected {\"nodes\": [...]} or [...]")
}

// LoadClusterConfig reads and parses a cluster configuration JSON file from disk.
func LoadClusterConfig(filePath string) (*ClusterConfig, error) {
	data, err := os.ReadFile(filePath)
	if err != nil {
		return nil, fmt.Errorf("failed to read cluster config file %q: %w", filePath, err)
	}
	return ParseClusterConfig(data)
}

// SaveClusterConfig writes a ClusterConfig as formatted JSON to the specified file path.
func SaveClusterConfig(filePath string, cfg *ClusterConfig) error {
	if cfg == nil {
		return fmt.Errorf("cannot save nil cluster config")
	}

	data, err := json.MarshalIndent(cfg, "", "  ")
	if err != nil {
		return fmt.Errorf("failed to marshal cluster config: %w", err)
	}

	if err := os.WriteFile(filePath, data, 0644); err != nil {
		return fmt.Errorf("failed to write cluster config file %q: %w", filePath, err)
	}
	return nil
}
