package raft

import "fmt"

type NodeID int
type Term uint64
type LogIndex uint64

// NoneNodeID represents a null or absent node ID (e.g. initial votedFor state).
const NoneNodeID NodeID = -1

type Peer struct {
	ID      NodeID `json:"id"`
	Address string `json:"address"`
}

type LogEntry struct {
	Term    Term     `json:"term"`
	Index   LogIndex `json:"index"`
	Command any      `json:"command"`
}

func (e LogEntry) String() string {
	return fmt.Sprintf("LogEntry[Term=%d, Index=%d, Command=%v]", e.Term, e.Index, e.Command)
}

type PersistentState struct {
	CurrentTerm Term   `json:"current_term"`
	VotedFor    NodeID `json:"voted_for"`
	Log         []LogEntry
}

// LastLogInfo returns the (index, term) of the last log entry in PersistentState.
// Returns (0, 0) if the state is nil or the log contains no entries.
func (p *PersistentState) LastLogInfo() (LogIndex, Term) {
	if p == nil || len(p.Log) == 0 {
		return 0, 0
	}
	last := p.Log[len(p.Log)-1]
	return last.Index, last.Term
}

// IsLogUpToDate evaluates whether a candidate's log (with candidateLastIndex, candidateLastTerm)
// is at least as up-to-date as the receiver's log (with receiverLastIndex, receiverLastTerm)
// per Raft paper §5.4.1 (Election restriction):
//
// "If the logs have last entries with different terms, then the log with the later term
// is more up-to-date. If the logs end with the same term, then whichever log is longer
// is more up-to-date."
func IsLogUpToDate(candidateLastIndex LogIndex, candidateLastTerm Term, receiverLastIndex LogIndex, receiverLastTerm Term) bool {
	if candidateLastTerm != receiverLastTerm {
		return candidateLastTerm > receiverLastTerm
	}
	return candidateLastIndex >= receiverLastIndex
}

type VolatileState struct {
	CommitIndex LogIndex `json:"commit_index"`
	LastApplied LogIndex `json:"last_applied"`
}

type VolatileStateLeader struct {
	NextIndex  map[NodeID]LogIndex `json:"next_index"`
	MatchIndex map[NodeID]LogIndex `json:"match_index"`
}

type RaftState int

const (
	Follower RaftState = iota
	Candidate
	Leader
)

func (s RaftState) String() string {
	switch s {
	case Follower:
		return "Follower"
	case Candidate:
		return "Candidate"
	case Leader:
		return "Leader"
	default:
		return "Unknown"
	}
}

type RequestVoteArgs struct {
	Term         Term     `json:"term"`
	CandidateID  NodeID   `json:"candidate_id"`
	LastLogIndex LogIndex `json:"last_log_index"`
	LastLogTerm  Term     `json:"last_log_term"`
}

type RequestVoteReply struct {
	Term        Term `json:"term"`
	VoteGranted bool `json:"vote_granted"`
}

type AppendEntriesArgs struct {
	Term         Term       `json:"term"`
	LeaderID     NodeID     `json:"leader_id"`
	PrevLogIndex LogIndex   `json:"prev_log_index"`
	PrevLogTerm  Term       `json:"prev_log_term"`
	Entries      []LogEntry `json:"entries"`
	LeaderCommit LogIndex   `json:"leader_commit"`
}

type AppendEntriesReply struct {
	Term          Term     `json:"term"`
	Success       bool     `json:"success"`
	ConflictIndex LogIndex `json:"conflict_index"`
	ConflictTerm  Term     `json:"conflict_term"`
}

type ApplyMsg struct {
	CommandValid bool
	Command      any
	CommandIndex LogIndex

	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  Term
	SnapshotIndex LogIndex
}
