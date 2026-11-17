package server

import (
	"sync/atomic"
	"time"

	"raft/raft"
)

// StepDownReasons records counts of step-down occurrences by specific cause.
type StepDownReasons struct {
	HigherTermRequest uint64 `json:"higher_term_request"`
	HigherTermReply   uint64 `json:"higher_term_reply"`
	QuorumLoss        uint64 `json:"quorum_loss"`
	LeaderHeartbeat   uint64 `json:"leader_heartbeat"`
	Other             uint64 `json:"other,omitempty"`
}

// Total returns the sum of all step-down occurrences.
func (s StepDownReasons) Total() uint64 {
	return s.HigherTermRequest + s.HigherTermReply + s.QuorumLoss + s.LeaderHeartbeat + s.Other
}

type stepDownTracker struct {
	higherTermRequest atomic.Uint64
	higherTermReply   atomic.Uint64
	quorumLoss        atomic.Uint64
	leaderHeartbeat   atomic.Uint64
	other             atomic.Uint64
}

func (t *stepDownTracker) record(reason string) {
	switch reason {
	case "higher_term_request":
		t.higherTermRequest.Add(1)
	case "higher_term_reply":
		t.higherTermReply.Add(1)
	case "quorum_loss":
		t.quorumLoss.Add(1)
	case "leader_heartbeat":
		t.leaderHeartbeat.Add(1)
	default:
		t.other.Add(1)
	}
}

func (t *stepDownTracker) snapshot() StepDownReasons {
	return StepDownReasons{
		HigherTermRequest: t.higherTermRequest.Load(),
		HigherTermReply:   t.higherTermReply.Load(),
		QuorumLoss:        t.quorumLoss.Load(),
		LeaderHeartbeat:   t.leaderHeartbeat.Load(),
		Other:             t.other.Load(),
	}
}

// NodeMetricsSummary aggregates key consensus runtime metrics for a node.
type NodeMetricsSummary struct {
	NodeID             raft.NodeID     `json:"node_id"`
	UptimeSeconds      float64         `json:"uptime_seconds"`
	ElectionsStarted   uint64          `json:"elections_started"`
	ElectionsPerHour   float64         `json:"elections_per_hour"`
	PreVotesStarted    uint64          `json:"pre_votes_started"`
	PreVotesWon        uint64          `json:"pre_votes_won"`
	PreVoteSuccessRate float64         `json:"pre_vote_success_rate"`
	StepDownReasons    StepDownReasons `json:"step_down_reasons"`
}

// ElectionsPerHour computes the rate of election initiations normalized to per hour.
func (s *Server) ElectionsPerHour() float64 {
	uptime := s.Uptime()
	if uptime < time.Millisecond {
		uptime = time.Millisecond
	}
	hours := uptime.Hours()
	if hours <= 0 {
		return 0.0
	}
	return float64(s.electionsStarted.Load()) / hours
}

// PreVoteSuccessRate computes the ratio of pre-vote rounds won to rounds started.
func (s *Server) PreVoteSuccessRate() float64 {
	started := s.preVotesStarted.Load()
	if started == 0 {
		return 0.0
	}
	return float64(s.preVotesWon.Load()) / float64(started)
}

// PreVotesStarted returns the total number of pre-vote rounds initiated by this node.
func (s *Server) PreVotesStarted() uint64 {
	return s.preVotesStarted.Load()
}

// PreVotesWon returns the total number of pre-vote rounds won by this node.
func (s *Server) PreVotesWon() uint64 {
	return s.preVotesWon.Load()
}

// StepDownReasons returns a snapshot of step-down counts grouped by reason.
func (s *Server) StepDownReasons() StepDownReasons {
	return s.stepDowns.snapshot()
}

// MetricsSummary returns a comprehensive metrics summary for the node.
func (s *Server) MetricsSummary() NodeMetricsSummary {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.metricsSummaryLocked()
}

func (s *Server) metricsSummaryLocked() NodeMetricsSummary {
	uptime := s.uptimeLocked()
	if uptime < time.Millisecond {
		uptime = time.Millisecond
	}
	hours := uptime.Hours()
	electionsPerHour := 0.0
	if hours > 0 {
		electionsPerHour = float64(s.electionsStarted.Load()) / hours
	}

	preVotesStarted := s.preVotesStarted.Load()
	preVotesWon := s.preVotesWon.Load()
	successRate := 0.0
	if preVotesStarted > 0 {
		successRate = float64(preVotesWon) / float64(preVotesStarted)
	}

	return NodeMetricsSummary{
		NodeID:             s.ID,
		UptimeSeconds:      uptime.Seconds(),
		ElectionsStarted:   s.electionsStarted.Load(),
		ElectionsPerHour:   electionsPerHour,
		PreVotesStarted:    preVotesStarted,
		PreVotesWon:        preVotesWon,
		PreVoteSuccessRate: successRate,
		StepDownReasons:    s.StepDownReasons(),
	}
}

// Uptime returns the duration the server has been running.
func (s *Server) Uptime() time.Duration {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.uptimeLocked()
}

func (s *Server) uptimeLocked() time.Duration {
	if s.startTime.IsZero() {
		return 0
	}
	return s.getClockLocked().Now().Sub(s.startTime)
}
