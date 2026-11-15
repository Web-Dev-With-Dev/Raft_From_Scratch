// Package rpc implements the gRPC networking transport, client connection pooling,
// dial retry policies, and proto serialization conversions for Raft peer communication.
//
// # Client Connection Pooling
//
// The ClientPool manages persistent gRPC client connections to cluster peers. To prevent
// socket exhaustion and avoid connection setup latency during time-critical elections,
// connections are lazily established, pooled, and reused across RPC invocations.
//
// Features include:
//   - Thread-Safe Access: Connection creation and lookup are guarded by an internal RWMutex.
//   - Dial Retries with Backoff: Peer connections can be established using ConnectWithRetry,
//     which employs exponential backoff and randomized jitter to prevent connection storms.
//   - Pluggable Mocking: Mock or in-memory peer clients (e.g., inMemoryPeerClient in the test harness)
//     can be registered directly into the ClientPool, allowing 100% in-process simulation
//     without opening OS TCP sockets.
//
// # RPC Interfaces
//
// Communication conforms to the RaftService gRPC definition defined in proto/raft.proto:
//   - RequestVote: Candidate election campaigning.
//   - AppendEntries: Leader heartbeats and log replication.
package rpc

