# MiniKV engineering and deployment audit

**Review workspace status:** hardened development/release-candidate preparation; **not yet certified for production deployment**.

## Scope and method

The supplied archive was unpacked into a clean review workspace. The review covered the Go packages, unit/integration tests, benchmark harness, Dockerfile, Docker Compose topology, Prometheus/Grafana/Caddy configuration, and operator documentation.

The work followed three passes:

1. Correctness and data safety: mutation ordering, WAL replay, snapshots, replication, quorum behavior, tombstones, repair and handoff.
2. Concurrency and performance: goroutine lifecycle, locking, network I/O, resource bounds, WAL batching, hot-path logging and read quorum latency.
3. Deployment and operations: configuration validation, secrets, container hardening, port exposure, backup, CI and operator documentation.

A source review cannot prove distributed correctness for every failure schedule. Fixes below are described with their tests and remaining limits.

## Critical correctness and durability findings

### 1. WAL replay omitted replica records — fixed in review workspace

Recovery now replays legacy `SET`/`DEL`, versioned `SET_V2`/`DEL_V2`, and replica `REPL_SET`/`REPL_DEL` records. Versioned values restore timestamps, vector clocks and tombstones. Segment ordering uses parsed numeric sequence numbers.

Regression coverage includes replica values with spaces and versioned tombstone replay.

### 2. WAL recovery could append after an incomplete tail — fixed in review workspace

Recovery now truncates a non-newline-terminated final tail to the last complete record boundary and syncs the truncation before replay. Recognized but malformed mutation records now fail recovery rather than being silently skipped. Unknown legacy/non-WAL lines remain ignored for compatibility.

Scanner token size is explicitly bounded at 16 MiB. Network commands are separately bounded to 1 MiB.

### 3. WAL created a new segment on every restart and had no size rotation — fixed in review workspace

`NewWAL` now reopens the highest existing segment after `Recover`. `NewWALWithSegmentSize` supports a configurable segment limit; the server reads `MINIKV_WAL_SEGMENT_BYTES` (default 64 MiB). Rotation occurs before a commit batch would exceed the limit, and a single record larger than the configured segment limit is rejected. WAL segments are retained; automatic purge remains disabled because snapshots are not tied to a WAL sequence checkpoint.

### 4. WAL batch failure could leave a partial append — remediation applied; fault injection still needed

A failed write/flush/sync attempts to reset the buffer, truncate to the batch's starting offset, and sync the rollback. If rollback itself fails, the WAL is poisoned and later writes fail rather than appending behind an uncertain tail. `Close` attempts flush, sync and close even when an earlier close stage fails. Rotation opens the next segment before closing the old one.

Disk-full, filesystem I/O fault and power-loss injection have not been run. This area remains a release validation requirement.

### 5. Recovery skipped version metadata for replicas — fixed in review workspace

Versioned WAL records restore `CreatedAt`, vector clocks and deletion state. Legacy records remain replayable but cannot restore metadata that was never recorded in the old format.

### 6. Deletes lacked durable versioned tombstones — active paths fixed

Primary DEL creates a vector-clock tombstone, persists it to WAL before publishing it in memory, replicates a versioned `REPL_DEL`, and retains tombstones in snapshots and Merkle state. Legacy physical-delete records remain supported for compatibility.

Tombstone garbage collection is intentionally not implemented. Deleting tombstones without a cluster-wide safe watermark can resurrect old values.

### 7. DEL returned success without a write quorum — fixed in review workspace

DEL now counts the local WAL-synced tombstone and successful replica acknowledgements, creates hints on transport failures, and only returns `Deleted` after the configured write quorum is met. Duplicate tombstone acknowledgements count as acknowledgements.

### 8. Hinted handoff removed entries using stale indexes — fixed; persistence remains limited

Successful hints are removed by matching hint identity rather than indexes from a copied slice. A regression test covers consecutive removals. Hints are still in-memory only and can be lost on process restart. With RF=3 and W=2, an acknowledged write has two durable copies, and anti-entropy can repair the third replica; however, loss of pending hints reduces repair speed and resilience. Durable hints remain a recommended next reliability feature.

### 9. Snapshot loading conflated absence with corruption — fixed in review workspace

Only `os.ErrNotExist` is treated as a missing snapshot. Permission, I/O and JSON errors stop startup. JSON `null` is rejected as an invalid snapshot map. Tombstones round-trip through snapshots.

Snapshot writing uses temp-file write, file sync, close and rename. Parent-directory fsync and injected crash testing around rename are not yet implemented, so crash consistency should be validated on the target filesystem.

### 10. Quorum reads waited for every replica — optimized and tested

Replica reads are concurrent. The handler counts a local response (including local absence) and remote NOT_FOUND responses toward read quorum, then returns once `ReadQuorum` successful responses have arrived rather than waiting for every replica. The result is selected by vector-clock ordering with timestamp tie-breaking for concurrent/equal clocks. Late replica reads are bounded by client I/O deadlines and their results are sent to a sufficiently buffered channel; periodic anti-entropy repairs replicas not needed to satisfy the read.

A regression test verifies that a slow third replica does not delay a read once quorum is achieved. This relies on the configured quorum intersection (`R + W > RF`); do not lower these settings without re-evaluating the consistency guarantee.

### 11. Equal-clock timestamp tie-breaking was inconsistent — fixed in review workspace

`Store.MergeIfNewer`, replica SET/DEL handlers and read repair now consistently apply the project's timestamp tie-breaker when vector clocks are equal or concurrent. Regression tests cover equal-clock later/earlier updates and later-timestamp replica tombstones.

The conflict policy for concurrent versions remains timestamp-based last-writer-wins. This is deterministic enough for the current design but can discard concurrent values; a conflict-preserving application policy is still a future design decision.

### 12. Anti-entropy omitted tombstone/version state — current loop improved

Merkle leaves include payload, timestamp, vector clock and deletion state. Pulled repairs are WAL-persisted before applying, and tombstones are propagated. The repair loop is cancellable and returns a completion channel. Full partition/rejoin chaos testing and multi-node convergence testing under concurrent writes remain required.

## Network and lifecycle hardening

- `Server.StartContext` supports context cancellation, active connection tracking, bounded connection admission (default 1024), listener closure, and waiting for handlers. The original blocking `Start()` API remains available.
- Temporary accept errors use bounded backoff; fatal accept errors terminate the server instead of spinning indefinitely.
- `HandleConnection` bounds command lines to 1 MiB, uses read/write deadlines, rejects oversized requests, and flushes responses. Response output is bounded to 64 MiB.
- Hot-path logs record command names rather than raw request/response payloads. Replica SET hot-path logs were reduced from Info to Debug.
- `ProcessCommand` retains the existing command dispatch and forwarded-request semantics.
- Main uses signal-aware cancellation, stops background workers, waits for metrics shutdown, writes a final snapshot, and closes WAL.
- The integration command helper reads a complete newline-terminated response with a deadline instead of assuming one TCP `Read` returns a full response.

## Configuration, testing and deployment changes

1. Hash-ring traversal is bounded by one full ring; it cannot loop indefinitely if RF exceeds physical node count. No nonexistent replica is returned as the owner.
2. Cluster configuration rejects empty clusters, blank/duplicate IDs, duplicate addresses, invalid host/port addresses, and node IDs containing vector-clock delimiter characters. Startup rejects fewer nodes than configured RF.
3. Integration tests use a temporary per-test data directory and no longer clear shared repository `data/` files.
4. `.env` is excluded from the review package; `.env.example` contains no password value and Compose requires an operator-supplied Grafana password.
5. Compose publishes MiniKV TCP ports on host loopback by default, not on all interfaces. Node containers use read-only roots, tmpfs `/tmp`, dropped capabilities, `no-new-privileges`, PID limits and stop grace periods. Each node has a distinct persistent volume.
6. Grafana and Prometheus remain loopback-bound by default. Optional Caddy profile provides HTTPS access to Grafana only.
7. `MINIKV_WAL_SEGMENT_BYTES` is configurable; old WAL segments are retained until a snapshot checkpoint protocol exists.
8. Added GitHub Actions CI for formatting, `go test -race ./...`, `go vet ./...`, binary builds and Docker image build.
9. Added a PowerShell offline backup script. It stops all MiniKV nodes, archives each named data volume separately, and restarts nodes. Restore instructions are documented.
10. Added a PowerShell pre-deployment verification script that checks the declared Go version, formatting, race tests, vet, builds, Compose configuration and (unless skipped) the Docker image.
11. README performance claims were corrected to distinguish an older recorded 1,650 req/s run from the separately reported 12,000 req/s three-node measurement. The latter lacks a captured workload definition and is not independently reproduced here.
12. The load-test harness now accepts total requests, concurrency, timeout and a comma-separated list of node addresses; it distributes persistent clients across the three nodes and reports successes, failures and latency percentiles.

## Validation results

### Executed in this review environment

- Go installed: `go1.23.2`; repository declares `go 1.26.3`.
- Network access was unavailable, so Go 1.26.3 and the real external Zap/Prometheus modules could not be downloaded.
- To validate source/test integration, a temporary Go 1.23 modfile and local API-compatible Zap/Prometheus stubs outside the repository were used. The repository's `go.mod` and `go.sum` were not downgraded or replaced.
- `go test -race -count=1 ./...` passed using that temporary modfile/stub setup, including multi-process integration tests.
- `go vet ./...` passed using the same temporary setup.
- `gofmt` was applied to all Go files.
- Docker and Docker Compose are not installed in this environment. Docker image build, `docker compose config`, container startup, volume persistence and backup/restore execution were not run.

### Mandatory validation on the declared toolchain

Run on a machine with Go 1.26.3 and network access:

```powershell
go version
go mod download
gofmt -l (Get-ChildItem -Recurse -Filter *.go | ForEach-Object FullName)
go test -race -count=1 ./...
go vet ./...
go build -trimpath ./cmd/server
go build -trimpath ./cmd/client
docker compose config
docker compose build
docker compose up -d
docker compose ps
docker compose logs --tail 200 minikv-a minikv-b minikv-c
```

Then verify all Prometheus targets are UP, SET/GET/DEL across nodes, stop/restart each node, confirm data recovery, execute the offline backup script, and perform a restore drill in a disposable environment.

## Remaining release blockers and limits

- Hinted handoff remains volatile; persist hints if the operational requirement demands guaranteed pending-repair replay after coordinator restart.
- Snapshot durability needs parent-directory sync and crash/failure injection on the target filesystem.
- WAL rollback/rotation needs disk-full and I/O fault injection; process-kill tests do not simulate power loss or device-cache behavior.
- The protocol is newline-delimited plain TCP with no authentication, TLS, authorization, or encryption. Keep it on a trusted private network or add a secure transport before exposing it to untrusted clients.
- The three-node Compose stack is single-host only. It does not provide independent host/zone failure domains or consensus-based membership/leader election.
- Gossip is direct heartbeat probing rather than a full disseminating gossip protocol; false-positive/split-brain behavior needs fault testing.
- Timestamp-based conflict resolution may lose concurrent application updates; define application-level conflict handling before production use.
- No safe WAL segment purge/checkpoint or tombstone garbage collection is implemented. Disk usage can grow over time; monitor volume capacity and plan retention.
- Benchmarks have not been run against this review copy. The 16,000 successful requests/sec target is not yet demonstrated. Capture the exact workload, concurrency, host, operation mix, quorum, durability, success/failure counts and latency percentiles before comparing to the 12,000 baseline.
- Actual Go 1.26.3 module tests, Docker build/Compose validation, container smoke tests, cloud firewall review and restore drill remain unverified.

## Performance target and safety invariant

The stated goal is at least 16,000 **successful** requests/sec across the three-node cluster, compared with a reported 12,000 requests/sec baseline. No optimization may reduce RF=3, R=2, W=2, skip WAL sync before acknowledging a local write, or return success before the required write quorum is reached merely to meet the throughput target.

**Conclusion:** the workspace contains meaningful correctness, lifecycle, WAL, network, testing and deployment hardening. It is suitable for review and controlled pre-deployment validation, but it is not honestly certifiable as production-ready until the mandatory validation and remaining release gates above are completed.

## License note

The supplied archive did not contain a `LICENSE` file even though the original README referred to MIT. The README has been corrected; select and add the intended license before redistribution.
