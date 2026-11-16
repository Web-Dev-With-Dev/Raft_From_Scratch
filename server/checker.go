package server

import (
	"fmt"
	"sync"
	"testing"

	"raft/raft"
)

// GlobalChecker verifies Raft consensus invariants across all nodes after every simulated step:
// 1. Election Safety: At most one leader per term (both instantaneous and historical).
// 2. Term Monotonicity: Node terms never decrease.
// 3. VotedFor Stability: Once a vote is cast in term T, it remains stable throughout term T.
type GlobalChecker struct {
	mu            sync.Mutex
	termLeaders   map[raft.Term]raft.NodeID
	lastNodeTerm  map[raft.NodeID]raft.Term
	nodeTermVotes map[raft.NodeID]map[raft.Term]raft.NodeID
	stepCount     uint64
}

// NewGlobalChecker constructs a new initialized GlobalChecker.
func NewGlobalChecker() *GlobalChecker {
	return &GlobalChecker{
		termLeaders:   make(map[raft.Term]raft.NodeID),
		lastNodeTerm:  make(map[raft.NodeID]raft.Term),
		nodeTermVotes: make(map[raft.NodeID]map[raft.Term]raft.NodeID),
	}
}

// Reset clears all recorded history and resets step counter.
func (gc *GlobalChecker) Reset() {
	gc.mu.Lock()
	defer gc.mu.Unlock()
	gc.termLeaders = make(map[raft.Term]raft.NodeID)
	gc.lastNodeTerm = make(map[raft.NodeID]raft.Term)
	gc.nodeTermVotes = make(map[raft.NodeID]map[raft.Term]raft.NodeID)
	gc.stepCount = 0
}

// checkElectionSafetyLocked verifies Raft §5.2 Election Safety.
func (gc *GlobalChecker) checkElectionSafetyLocked(statuses []NodeStatus) error {
	instantLeaders := make(map[raft.Term]raft.NodeID)
	for _, st := range statuses {
		if st.State != raft.Leader.String() {
			continue
		}
		if other, exists := instantLeaders[st.Term]; exists && other != st.ID {
			return fmt.Errorf("election safety violated (instantaneous): multiple leaders in term %d (nodes %d and %d) at step %d",
				st.Term, other, st.ID, gc.stepCount)
		}
		instantLeaders[st.Term] = st.ID

		if histLeader, exists := gc.termLeaders[st.Term]; exists && histLeader != st.ID {
			return fmt.Errorf("election safety violated (historical): term %d has multiple leaders across history (first node %d, now node %d) at step %d",
				st.Term, histLeader, st.ID, gc.stepCount)
		}
		gc.termLeaders[st.Term] = st.ID
	}
	return nil
}

// checkTermMonotonicityLocked verifies that a node's term never decreases (§5.1).
func (gc *GlobalChecker) checkTermMonotonicityLocked(st NodeStatus) error {
	lastTerm, exists := gc.lastNodeTerm[st.ID]
	if exists && st.Term < lastTerm {
		return fmt.Errorf("term monotonicity violated: node %d term decreased from %d to %d at step %d",
			st.ID, lastTerm, st.Term, gc.stepCount)
	}
	gc.lastNodeTerm[st.ID] = st.Term
	return nil
}

// checkVotedForStabilityLocked verifies that votedFor is stable within a term (§5.2).
func (gc *GlobalChecker) checkVotedForStabilityLocked(st NodeStatus) error {
	votesForNode, exists := gc.nodeTermVotes[st.ID]
	if !exists {
		votesForNode = make(map[raft.Term]raft.NodeID)
		gc.nodeTermVotes[st.ID] = votesForNode
	}

	priorVote, hasPrior := votesForNode[st.Term]
	if st.VotedFor != -1 {
		if hasPrior && priorVote != st.VotedFor {
			return fmt.Errorf("votedFor stability violated: node %d changed vote in term %d from candidate %d to %d at step %d",
				st.ID, st.Term, priorVote, st.VotedFor, gc.stepCount)
		}
		votesForNode[st.Term] = st.VotedFor
	} else if hasPrior && priorVote != -1 {
		return fmt.Errorf("votedFor stability violated: node %d erased vote for candidate %d in term %d back to -1 at step %d",
			st.ID, priorVote, st.Term, gc.stepCount)
	}
	return nil
}

// checkNodeStatusesLocked checks all 3 invariants against a snapshot of node statuses.
func (gc *GlobalChecker) checkNodeStatusesLocked(statuses []NodeStatus) error {
	gc.stepCount++
	if err := gc.checkElectionSafetyLocked(statuses); err != nil {
		return err
	}
	for _, st := range statuses {
		if err := gc.checkTermMonotonicityLocked(st); err != nil {
			return err
		}
		if err := gc.checkVotedForStabilityLocked(st); err != nil {
			return err
		}
	}
	return nil
}

// Check checks all invariants across all nodes in the cluster.
func (gc *GlobalChecker) Check(cluster *Cluster) error {
	if cluster == nil {
		return nil
	}
	statuses := cluster.NodeStatuses()
	gc.mu.Lock()
	defer gc.mu.Unlock()
	return gc.checkNodeStatusesLocked(statuses)
}

// CheckNodes checks all invariants directly on a slice of servers.
func (gc *GlobalChecker) CheckNodes(nodes []*Server) error {
	statuses := make([]NodeStatus, 0, len(nodes))
	for _, n := range nodes {
		if n != nil {
			statuses = append(statuses, n.Status())
		}
	}
	gc.mu.Lock()
	defer gc.mu.Unlock()
	return gc.checkNodeStatusesLocked(statuses)
}

// Assert checks all invariants and fails the test immediately if violated.
func (gc *GlobalChecker) Assert(t testing.TB, cluster *Cluster) {
	t.Helper()
	if err := gc.Check(cluster); err != nil {
		t.Fatalf("global checker assertion failed: %v", err)
	}
}

// StepCount returns the total number of simulated steps evaluated.
func (gc *GlobalChecker) StepCount() uint64 {
	gc.mu.Lock()
	defer gc.mu.Unlock()
	return gc.stepCount
}

// LeaderHistory returns a copy of historical term -> leader assignments.
func (gc *GlobalChecker) LeaderHistory() map[raft.Term]raft.NodeID {
	gc.mu.Lock()
	defer gc.mu.Unlock()
	res := make(map[raft.Term]raft.NodeID, len(gc.termLeaders))
	for k, v := range gc.termLeaders {
		res[k] = v
	}
	return res
}

