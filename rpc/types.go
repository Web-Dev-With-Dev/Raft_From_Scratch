package rpc

import "raft/raft"

type NodeID = raft.NodeID
type Term = raft.Term
type LogIndex = raft.LogIndex
type LogEntry = raft.LogEntry
type Peer = raft.Peer
type PersistentState = raft.PersistentState
type VolatileState = raft.VolatileState
type VolatileStateLeader = raft.VolatileStateLeader
type RaftState = raft.RaftState
type ElectionTimer = raft.ElectionTimer

const (
	Follower  = raft.Follower
	Candidate = raft.Candidate
	Leader    = raft.Leader

	DefaultMinElectionTimeout = raft.DefaultMinElectionTimeout
	DefaultMaxElectionTimeout = raft.DefaultMaxElectionTimeout
)

var (
	RandomElectionTimeout = raft.RandomElectionTimeout
	NewElectionTimer      = raft.NewElectionTimer
)
	
type RequestVoteArgs = raft.RequestVoteArgs
type RequestVoteReply = raft.RequestVoteReply
type AppendEntriesArgs = raft.AppendEntriesArgs
type AppendEntriesReply = raft.AppendEntriesReply

type ClientRequest struct {
	Op    string `json:"op"`
	Key   string `json:"key"`
	Value string `json:"value"`
}

type ClientReply struct {
	Success  bool        `json:"success"`
	LeaderID raft.NodeID `json:"leader_id"`
	Value    string      `json:"value"`
	Error    string      `json:"error"`
}
