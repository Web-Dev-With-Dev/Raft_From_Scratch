package server

import (
	"fmt"
	"io"
	"testing"

	"pgregory.net/rapid"
	"raft/logger"
	"raft/proto"
	"raft/raft"
)

func buildRandomRecvLog(rt *rapid.T, maxTerm raft.Term) *raft.MemoryLog {
	recvLogLen := rapid.IntRange(0, 15).Draw(rt, "recv_log_len")
	recvLog := raft.NewMemoryLog()
	rTerm := raft.Term(1)
	for i := 1; i <= recvLogLen; i++ {
		if rapid.Bool().Draw(rt, "r_term_inc") && rTerm < maxTerm {
			rTerm++
		}
		_ = recvLog.Append(raft.LogEntry{
			Index:   raft.LogIndex(i),
			Term:    rTerm,
			Command: fmt.Sprintf("cmd-recv-%d", i),
		})
	}
	return recvLog
}

func expectedVoteDecision(reqTerm, recvTerm raft.Term, votedFor, candID raft.NodeID, candIdx raft.LogIndex, candTerm raft.Term, recvLog raft.Log) bool {
	if reqTerm < recvTerm {
		return false
	}
	effectiveVotedFor := votedFor
	if reqTerm > recvTerm {
		effectiveVotedFor = raft.NoneNodeID
	}
	canVote := (effectiveVotedFor == raft.NoneNodeID || effectiveVotedFor == candID)
	logOk := raft.IsLogUpToDate(candIdx, candTerm, recvLog.LastIndex(), recvLog.LastTerm())
	return canVote && logOk
}

// TestProperty_VoteGranted_Iff_CandidateLogUpToDate verifies using rapid property-based testing
// the Raft invariant (§5.2, §5.4.1): vote is granted iff candidate's log is at least as up-to-date
// (using Log.LastIndex and Log.LastTerm) and receiver has not already voted for another candidate.
func TestProperty_VoteGranted_Iff_CandidateLogUpToDate(t *testing.T) {
	rapid.Check(t, func(rt *rapid.T) {
		candID := raft.NodeID(rapid.IntRange(1, 4).Draw(rt, "candidate_id"))
		recvID := raft.NodeID(rapid.IntRange(5, 8).Draw(rt, "receiver_id"))
		reqTerm := raft.Term(rapid.Uint64Range(2, 50).Draw(rt, "req_term"))

		recvTerm := reqTerm
		switch rapid.IntRange(0, 2).Draw(rt, "term_scenario") {
		case 1:
			recvTerm = reqTerm - 1
		case 2:
			recvTerm = reqTerm + 1
		}

		votedFor := raft.NoneNodeID
		switch rapid.IntRange(0, 2).Draw(rt, "voted_for_scenario") {
		case 1:
			votedFor = candID
		case 2:
			votedFor = 99
		}

		recvLog := buildRandomRecvLog(rt, reqTerm)
		candLastIndex := raft.LogIndex(rapid.Uint64Range(0, 20).Draw(rt, "cand_last_index"))
		candLastTerm := raft.Term(rapid.Uint64Range(0, uint64(reqTerm)).Draw(rt, "cand_last_term"))

		srv := NewServer(recvID, nil, &raft.PersistentState{
			CurrentTerm: recvTerm,
			VotedFor:    votedFor,
		}, nil)
		srv.SetLogger(logger.NewNodeLogger(recvID, io.Discard, nil))
		srv.SetLog(recvLog)

		reply := srv.processRequestVote(&raftpb.RequestVoteArgs{
			Term:         uint64(reqTerm),
			CandidateId:  int64(candID),
			LastLogIndex: uint64(candLastIndex),
			LastLogTerm:  uint64(candLastTerm),
		})

		expected := expectedVoteDecision(reqTerm, recvTerm, votedFor, candID, candLastIndex, candLastTerm, recvLog)
		if reply.VoteGranted != expected {
			rt.Fatalf("VoteGranted=%v, want %v (reqTerm=%d, recvTerm=%d, votedFor=%d, cand=[idx=%d, term=%d], recv=[idx=%d, term=%d])",
				reply.VoteGranted, expected, reqTerm, recvTerm, votedFor, candLastIndex, candLastTerm, recvLog.LastIndex(), recvLog.LastTerm())
		}
	})
}
