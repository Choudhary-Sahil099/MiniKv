# MiniKV

MiniKV is a distributed in-memory key-value database written in Go, designed to explore the core concepts behind modern distributed storage systems such as Amazon Dynamo and Apache Cassandra.

The project implements data replication, consistent hashing, failure detection, automatic recovery, anti-entropy repair, write-ahead logging, snapshotting, and observability using Prometheus and Grafana.

---

## Features

### Storage Engine

* In-memory key-value store
* Write-Ahead Logging (WAL)
* Crash recovery from WAL
* Periodic snapshots
* WAL compaction

### Distributed System Features

* Consistent hashing for key distribution
* Request forwarding to owner nodes
* Data replication
* Replica failover
* Gossip-based failure detection
* Startup synchronization for recovering nodes
* Read Repair
* Anti-Entropy Repair

### Observability

* Prometheus metrics
* Grafana dashboards
* Structured logging with Zap

---

## Architecture

Client requests can be sent to any node.

The receiving node determines the owner of the key using consistent hashing.

If the node is not the owner, the request is automatically forwarded to the correct node.

Data is replicated to a secondary replica node for fault tolerance.

Background repair mechanisms ensure eventual consistency across replicas.

```text
                +----------+
                |  Client  |
                +----------+
                      |
                      v
             +----------------+
             | Any MiniKV Node|
             +----------------+
                      |
                      v
             Consistent Hashing
                      |
        +-------------+-------------+
        |                           |
        v                           v
   Owner Node                Replica Node
        |                           |
        +-------------+-------------+
                      |
                      v
              WAL + Snapshot
```

---

## Consistency Mechanisms

### Replication

Every write is stored on the owner node and asynchronously replicated to a replica node.

### Read Repair

During reads, owner and replica values are compared.

If stale data is detected, the replica is automatically repaired.

### Anti-Entropy Repair

A background process periodically compares node data and repairs inconsistencies without requiring client reads.

---

## Failure Handling

### Gossip Protocol

Nodes periodically exchange heartbeats.

Failed nodes are automatically detected and marked unavailable.

### Failover

If an owner node becomes unavailable, requests are served by its replica.

### Startup Synchronization

When a node rejoins the cluster, it synchronizes its data from healthy peers.

---

## Durability

### Write-Ahead Log (WAL)

All write operations are first persisted to disk before being applied to memory.

### Recovery

On restart, MiniKV replays WAL entries to reconstruct state.

### Snapshots

Periodic snapshots reduce recovery time by storing the current database state.

### WAL Compaction

After snapshot creation, WAL files are truncated to prevent unbounded growth.

---

## Metrics

MiniKV exposes Prometheus metrics for:

* Total Requests
* GET Requests
* SET Requests
* DELETE Requests
* Request Latency
* Forwarded Requests
* Replication Operations
* Read Repairs
* Anti-Entropy Repairs
* Cluster Synchronizations
* Snapshots Created
* WAL Compactions
* Node Recoveries
* Alive Nodes
* Dead Nodes

Metrics Endpoint:

```text
http://localhost:2112/metrics
```

---

## Running MiniKV

### Start Node A

```bash
go run cmd/server/main.go NodeA 5000
```

### Start Node B

```bash
go run cmd/server/main.go NodeB 5001
```

### Start Node C

```bash
go run cmd/server/main.go NodeC 5002
```

### Start Client

```bash
go run cmd/client/main.go
```

---

## Example Commands

```text
SET user1 sahil
GET user1
DEL user1
LOCAL_GET user1
DUMP
```

---

## Technology Stack

* Go
* TCP Networking
* Prometheus
* Grafana
* Zap Logging

---

## Future Improvements

* Multi-replica support
* Quorum reads and writes
* Merkle-tree based anti-entropy
* Hinted handoff
* Vector clocks
* Dynamic cluster membership

---

## Learning Objectives

MiniKV was built to understand the internal mechanisms of distributed databases, including:

* Data partitioning
* Replication
* Fault tolerance
* Eventual consistency
* Failure detection
* Recovery mechanisms
* Distributed system observability
