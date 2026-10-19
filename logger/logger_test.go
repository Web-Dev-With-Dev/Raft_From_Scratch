package logger

import (
	"bytes"
	"encoding/json"
	"log/slog"
	"strings"
	"testing"
)

func TestNodeLogger_PrefixOnEveryLine(t *testing.T) {
	buf := &bytes.Buffer{}
	opts := &Options{
		Level:       slog.LevelDebug,
		DisableTime: true,
	}
	l := NewNodeLogger(1, buf, opts)

	l.Info("server starting", "port", 8001)
	l.Warn("disk usage high", "percent", 85)
	l.Debug("heartbeat tick", "term", 2)

	output := buf.String()
	lines := strings.Split(strings.TrimSpace(output), "\n")

	if len(lines) != 3 {
		t.Fatalf("expected 3 lines, got %d:\n%s", len(lines), output)
	}

	for i, line := range lines {
		if !strings.HasPrefix(line, "[Node 1] ") {
			t.Fatalf("line %d does not start with '[Node 1] ': %q", i, line)
		}
		if !strings.Contains(line, "node_id=1") {
			t.Fatalf("line %d missing structured node_id attribute: %q", i, line)
		}
	}
}

func TestNodeLogger_MultilinePrefix(t *testing.T) {
	buf := &bytes.Buffer{}
	opts := &Options{
		Level:       slog.LevelInfo,
		DisableTime: true,
	}
	l := NewNodeLogger(42, buf, opts)

	l.Error("first line\nsecond line\nthird line")

	output := buf.String()
	lines := strings.Split(strings.TrimSpace(output), "\n")

	for i, line := range lines {
		if !strings.HasPrefix(line, "[Node 42] ") {
			t.Fatalf("line %d does not start with '[Node 42] ': %q", i, line)
		}
	}
}

func TestNodeLogger_JSONFormat(t *testing.T) {
	buf := &bytes.Buffer{}
	opts := &Options{
		Level:  slog.LevelInfo,
		Format: "json",
	}
	l := NewNodeLogger(3, buf, opts)

	l.Info("election started", "candidate", 3, "term", 5)

	line := strings.TrimSpace(buf.String())
	prefix := "[Node 3] "
	if !strings.HasPrefix(line, prefix) {
		t.Fatalf("expected line to start with %q, got: %q", prefix, line)
	}

	jsonPart := strings.TrimPrefix(line, prefix)
	var parsed map[string]any
	if err := json.Unmarshal([]byte(jsonPart), &parsed); err != nil {
		t.Fatalf("failed to parse JSON part %q: %v", jsonPart, err)
	}

	if parsed["msg"] != "election started" {
		t.Fatalf("unexpected msg: %v", parsed["msg"])
	}
	if parsed["node_id"] != float64(3) {
		t.Fatalf("expected node_id 3, got: %v", parsed["node_id"])
	}
	if parsed["candidate"] != float64(3) {
		t.Fatalf("expected candidate 3, got: %v", parsed["candidate"])
	}
}

func TestNodeLogger_WithSubLogger(t *testing.T) {
	buf := &bytes.Buffer{}
	opts := &Options{
		Level:       slog.LevelInfo,
		DisableTime: true,
	}
	parent := NewNodeLogger(7, buf, opts)
	child := parent.With("component", "consensus")

	child.Info("transition to leader", "term", 10)

	line := strings.TrimSpace(buf.String())
	if !strings.HasPrefix(line, "[Node 7] ") {
		t.Fatalf("sub-logger line missing prefix: %q", line)
	}
	if !strings.Contains(line, "component=consensus") {
		t.Fatalf("sub-logger line missing component attribute: %q", line)
	}
	if !strings.Contains(line, "node_id=7") {
		t.Fatalf("sub-logger line missing node_id attribute: %q", line)
	}
}
