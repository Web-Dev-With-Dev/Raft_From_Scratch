package raft

import "fmt"

type NodeID int
type Term uint64
type LogIndex uint64

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
