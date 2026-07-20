# Raft Distributed Consensus

## Goal

Build a distributed key-value database implementing the Raft consensus algorithm.

---

## Components

### Client

Sends requests to the leader.

### Leader

Receives client requests and replicates logs.

### Followers

Receive replicated logs and acknowledge them.

### RPC Layer

Responsible for communication between nodes.

### Persistent Store

Stores:

- Current Term
- VotedFor
- Log Entries
- Snapshots

---

## Project Structure

cmd/
raft/
rpc/
server/
client/
store/

---

## Cluster Configuration

config.json

{
  "nodes": [
    {
      "id":1,
      "address":"localhost:8001"
    },
    {
      "id":2,
      "address":"localhost:8002"
    },
    {
      "id":3,
      "address":"localhost:8003"
    }
  ]
}

---

## Future Features

- Leader Election
- Heartbeats
- Log Replication
- Persistent Storage
- Snapshotting
- Membership Changes