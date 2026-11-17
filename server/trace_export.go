package server

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"

	"raft/raft"
)

// NodeEventTrace contains the complete exported event trace and metrics for a single node.
type NodeEventTrace struct {
	NodeID      raft.NodeID        `json:"node_id"`
	Term        raft.Term          `json:"term"`
	State       string             `json:"state"`
	VotedFor    raft.NodeID        `json:"voted_for"`
	IsKilled    bool               `json:"is_killed"`
	EventsCount int                `json:"events_count"`
	Metrics     NodeMetricsSummary `json:"metrics"`
	Events      []Event            `json:"events"`
}

// ExportEventTrace exports the node's current consensus state, metrics, and recorded events.
func (s *Server) ExportEventTrace() NodeEventTrace {
	s.mu.RLock()
	defer s.mu.RUnlock()

	var events []Event
	if s.eventLog != nil {
		events = s.eventLog.Entries()
	}

	var curTerm raft.Term
	var votedFor raft.NodeID = -1
	if s.PersistentState != nil {
		curTerm = s.PersistentState.CurrentTerm
		votedFor = s.PersistentState.VotedFor
	}

	return NodeEventTrace{
		NodeID:      s.ID,
		Term:        curTerm,
		State:       s.State.String(),
		VotedFor:    votedFor,
		IsKilled:    s.IsKilled(),
		EventsCount: len(events),
		Metrics:     s.metricsSummaryLocked(),
		Events:      events,
	}
}

// ExportEventTraceJSON serializes the node's event trace as indented JSON bytes.
func (s *Server) ExportEventTraceJSON() ([]byte, error) {
	trace := s.ExportEventTrace()
	return json.MarshalIndent(trace, "", "  ")
}

// ExportEventTraceJSONFile writes the node's event trace to the specified file path.
func (s *Server) ExportEventTraceJSONFile(path string) error {
	dir := filepath.Dir(path)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return fmt.Errorf("failed to create directory %q: %w", dir, err)
	}

	data, err := s.ExportEventTraceJSON()
	if err != nil {
		return fmt.Errorf("failed to marshal node %d trace JSON: %w", s.ID, err)
	}

	if err := os.WriteFile(path, data, 0o644); err != nil {
		return fmt.Errorf("failed to write trace file %q: %w", path, err)
	}
	return nil
}

// ExportNodeEventTraces collects per-node event traces for all servers in the cluster.
func (c *Cluster) ExportNodeEventTraces() map[raft.NodeID]NodeEventTrace {
	res := make(map[raft.NodeID]NodeEventTrace)
	if c == nil {
		return res
	}
	for _, node := range c.Nodes() {
		res[node.ID] = node.ExportEventTrace()
	}
	return res
}

// ExportNodeEventTracesJSON writes each node's event trace to <dir>/node_<id>_trace.json.
func (c *Cluster) ExportNodeEventTracesJSON(dir string) (map[raft.NodeID]string, error) {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, fmt.Errorf("failed to create trace dir %q: %w", dir, err)
	}

	paths := make(map[raft.NodeID]string)
	if c == nil {
		return paths, nil
	}

	for _, node := range c.Nodes() {
		path := filepath.Join(dir, fmt.Sprintf("node_%d_trace.json", node.ID))
		if err := node.ExportEventTraceJSONFile(path); err != nil {
			return nil, err
		}
		paths[node.ID] = path
	}
	return paths, nil
}

// ExportNodeTracesJSON writes per-node trace JSON files from a ScenarioTrace.
func (st *ScenarioTrace) ExportNodeTracesJSON(dir string) (map[raft.NodeID]string, error) {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, fmt.Errorf("failed to create trace dir %q: %w", dir, err)
	}

	paths := make(map[raft.NodeID]string)
	for id, node := range st.Nodes {
		path := filepath.Join(dir, fmt.Sprintf("node_%d_trace.json", id))
		data, err := json.MarshalIndent(node, "", "  ")
		if err != nil {
			return nil, fmt.Errorf("failed to marshal node %d trace JSON: %w", id, err)
		}
		if err := os.WriteFile(path, data, 0o644); err != nil {
			return nil, fmt.Errorf("failed to write trace file %q: %w", path, err)
		}
		paths[id] = path
	}
	return paths, nil
}
