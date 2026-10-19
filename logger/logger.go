package logger

import (
	"bytes"
	"fmt"
	"io"
	"log/slog"
	"os"
	"sync"

	"raft/raft"
)

// Options configures the structured node logger.
type Options struct {
	Level       slog.Leveler // Logging level (slog.LevelDebug, slog.LevelInfo, etc.)
	Format      string       // "text" (default) or "json"
	DisableTime bool         // If true, omits timestamp for deterministic testing
	AddSource   bool         // If true, adds source code line information
}

// DefaultOptions returns standard defaults for server logging.
func DefaultOptions() *Options {
	return &Options{
		Level:       slog.LevelInfo,
		Format:      "text",
		DisableTime: false,
		AddSource:   false,
	}
}

// NodePrefix returns the standardized string prefix for a given node ID.
func NodePrefix(id raft.NodeID) string {
	return fmt.Sprintf("[Node %d] ", id)
}

// PrefixWriter wraps an io.Writer and prepends a prefix at the beginning of every written line.
type PrefixWriter struct {
	mu     sync.Mutex
	w      io.Writer
	prefix []byte
	atBOL  bool
}

// NewPrefixWriter creates an io.Writer that inserts prefix at the start of every line.
func NewPrefixWriter(w io.Writer, prefix string) *PrefixWriter {
	if w == nil {
		w = os.Stderr
	}
	return &PrefixWriter{
		w:      w,
		prefix: []byte(prefix),
		atBOL:  true,
	}
}

// Write writes p to the underlying writer, ensuring prefix is prepended to every line.
func (pw *PrefixWriter) Write(p []byte) (n int, err error) {
	pw.mu.Lock()
	defer pw.mu.Unlock()

	total := len(p)
	for len(p) > 0 {
		if pw.atBOL {
			if _, err := pw.w.Write(pw.prefix); err != nil {
				return 0, err
			}
			pw.atBOL = false
		}

		idx := bytes.IndexByte(p, '\n')
		if idx == -1 {
			// No newline in remainder, write all and remain inside the line
			if _, err := pw.w.Write(p); err != nil {
				return 0, err
			}
			break
		}

		// Write up to and including the newline
		if _, err := pw.w.Write(p[:idx+1]); err != nil {
			return 0, err
		}
		pw.atBOL = true
		p = p[idx+1:]
	}

	return total, nil
}

// NewNodeLogger constructs an *slog.Logger where every line of output is prefixed with "[Node <ID>] "
// and includes a structured "node_id" attribute.
func NewNodeLogger(id raft.NodeID, w io.Writer, opts *Options) *slog.Logger {
	if opts == nil {
		opts = DefaultOptions()
	}
	if w == nil {
		w = os.Stderr
	}

	prefix := NodePrefix(id)
	pw := NewPrefixWriter(w, prefix)

	replaceAttr := func(groups []string, a slog.Attr) slog.Attr {
		if opts.DisableTime && a.Key == slog.TimeKey {
			return slog.Attr{} // Drop time attribute
		}
		return a
	}

	handlerOpts := &slog.HandlerOptions{
		Level:       opts.Level,
		AddSource:   opts.AddSource,
		ReplaceAttr: replaceAttr,
	}

	var handler slog.Handler
	if opts.Format == "json" {
		handler = slog.NewJSONHandler(pw, handlerOpts)
	} else {
		handler = slog.NewTextHandler(pw, handlerOpts)
	}

	// Attach node_id attribute to all structured logs
	return slog.New(handler).With("node_id", int(id))
}
