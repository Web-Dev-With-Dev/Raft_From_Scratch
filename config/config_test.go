package config

import (
	"path/filepath"
	"testing"

	"raft/raft"
)

func TestLoadClusterConfig_RootClusterJSON(t *testing.T) {
	// Root cluster.json
	cfg, err := LoadClusterConfig("../cluster.json")
	if err != nil {
		t.Fatalf("failed to load root cluster.json: %v", err)
	}

	if len(cfg.Nodes) != 3 {
		t.Fatalf("expected 3 nodes, got %d", len(cfg.Nodes))
	}

	expected := map[raft.NodeID]string{
		1: "127.0.0.1:8001",
		2: "127.0.0.1:8002",
		3: "127.0.0.1:8003",
	}

	for _, node := range cfg.Nodes {
		addr, exists := expected[node.ID]
		if !exists {
			t.Fatalf("unexpected node ID %d", node.ID)
		}
		if node.Address != addr {
			t.Fatalf("expected address %s for node %d, got %s", addr, node.ID, node.Address)
		}
	}
}

func TestParseClusterConfig_Formats(t *testing.T) {
	// 1. Object format {"nodes": [...]}
	objJSON := []byte(`{
		"nodes": [
			{"id": 1, "address": "localhost:8001"},
			{"id": 2, "address": "localhost:8002"}
		]
	}`)
	cfg1, err := ParseClusterConfig(objJSON)
	if err != nil {
		t.Fatalf("failed to parse object format: %v", err)
	}
	if len(cfg1.Nodes) != 2 {
		t.Fatalf("expected 2 nodes, got %d", len(cfg1.Nodes))
	}

	// 2. Direct array format [...]
	arrJSON := []byte(`[
		{"id": 1, "address": "localhost:8001"},
		{"id": 2, "address": "localhost:8002"}
	]`)
	cfg2, err := ParseClusterConfig(arrJSON)
	if err != nil {
		t.Fatalf("failed to parse array format: %v", err)
	}
	if len(cfg2.Nodes) != 2 {
		t.Fatalf("expected 2 nodes, got %d", len(cfg2.Nodes))
	}
}

func TestClusterConfig_Helpers(t *testing.T) {
	cfg := &ClusterConfig{
		Nodes: []NodeConfig{
			{ID: 1, Address: "localhost:8001"},
			{ID: 2, Address: "localhost:8002"},
			{ID: 3, Address: "localhost:8003"},
		},
	}

	// PeerIDs
	ids := cfg.PeerIDs()
	if len(ids) != 3 || ids[0] != 1 || ids[1] != 2 || ids[2] != 3 {
		t.Fatalf("unexpected PeerIDs: %v", ids)
	}

	// Peers
	peers := cfg.Peers()
	if len(peers) != 3 || peers[0].ID != 1 || peers[0].Address != "localhost:8001" {
		t.Fatalf("unexpected Peers: %v", peers)
	}

	// PeersFor
	peersFor1 := cfg.PeersFor(1)
	if len(peersFor1) != 2 {
		t.Fatalf("expected 2 peers for node 1, got %d", len(peersFor1))
	}
	for _, p := range peersFor1 {
		if p.ID == 1 {
			t.Fatalf("expected node 1 to be excluded from PeersFor(1)")
		}
	}

	// GetNode
	node2, found := cfg.GetNode(2)
	if !found || node2.Address != "localhost:8002" {
		t.Fatalf("expected to find node 2, got %v (found: %v)", node2, found)
	}

	_, found = cfg.GetNode(99)
	if found {
		t.Fatalf("expected node 99 to not be found")
	}
}

func TestClusterConfig_Validation(t *testing.T) {
	// Empty nodes
	emptyCfg := &ClusterConfig{Nodes: []NodeConfig{}}
	if err := emptyCfg.Validate(); err == nil {
		t.Fatalf("expected error for empty nodes")
	}

	// Empty address
	badAddrCfg := &ClusterConfig{
		Nodes: []NodeConfig{
			{ID: 1, Address: ""},
		},
	}
	if err := badAddrCfg.Validate(); err == nil {
		t.Fatalf("expected error for empty node address")
	}

	// Duplicate IDs
	dupIDCfg := &ClusterConfig{
		Nodes: []NodeConfig{
			{ID: 1, Address: "localhost:8001"},
			{ID: 1, Address: "localhost:8002"},
		},
	}
	if err := dupIDCfg.Validate(); err == nil {
		t.Fatalf("expected error for duplicate node IDs")
	}
}

func TestSaveClusterConfig(t *testing.T) {
	tempDir := t.TempDir()
	filePath := filepath.Join(tempDir, "test_cluster.json")

	cfg := &ClusterConfig{
		Nodes: []NodeConfig{
			{ID: 1, Address: "10.0.0.1:8001"},
			{ID: 2, Address: "10.0.0.2:8002"},
		},
	}

	if err := SaveClusterConfig(filePath, cfg); err != nil {
		t.Fatalf("SaveClusterConfig failed: %v", err)
	}

	loaded, err := LoadClusterConfig(filePath)
	if err != nil {
		t.Fatalf("LoadClusterConfig failed: %v", err)
	}

	if len(loaded.Nodes) != 2 {
		t.Fatalf("expected 2 nodes, got %d", len(loaded.Nodes))
	}

	if loaded.Nodes[0].Address != "10.0.0.1:8001" {
		t.Fatalf("unexpected address: %s", loaded.Nodes[0].Address)
	}
}

func TestLoadClusterConfig_NonExistentFile(t *testing.T) {
	_, err := LoadClusterConfig("non_existent_file.json")
	if err == nil {
		t.Fatalf("expected error loading non-existent file")
	}
}
