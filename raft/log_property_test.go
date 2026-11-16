package raft

import (
	"fmt"
	"testing"

	"pgregory.net/rapid"
)

// TestProperty_Log_IsUpToDate verifies using rapid property-based testing that
// Log.IsUpToDate matches the canonical Raft paper §5.4.1 election restriction
// across arbitrary combinations of log lengths, terms, and monotonic entry sequences.
func TestProperty_Log_IsUpToDate(t *testing.T) {
	rapid.Check(t, func(rt *rapid.T) {
		recvLen := rapid.IntRange(0, 30).Draw(rt, "recv_log_len")
		recvLog := NewMemoryLog()

		curTerm := Term(1)
		for i := 1; i <= recvLen; i++ {
			if rapid.Bool().Draw(rt, "recv_term_step") {
				curTerm += Term(rapid.IntRange(1, 3).Draw(rt, "recv_term_delta"))
			}
			_ = recvLog.Append(LogEntry{
				Index:   LogIndex(i),
				Term:    curTerm,
				Command: fmt.Sprintf("cmd-%d", i),
			})
		}

		candIndex := LogIndex(rapid.Uint64Range(0, 50).Draw(rt, "cand_index"))
		candTerm := Term(rapid.Uint64Range(0, uint64(curTerm)+5).Draw(rt, "cand_term"))

		got := recvLog.IsUpToDate(candIndex, candTerm)
		expected := IsLogUpToDate(candIndex, candTerm, recvLog.LastIndex(), recvLog.LastTerm())

		if got != expected {
			rt.Fatalf("IsUpToDate(%d, %d) = %v; want %v (recv lastIdx=%d, lastTerm=%d)",
				candIndex, candTerm, got, expected, recvLog.LastIndex(), recvLog.LastTerm())
		}
	})
}
