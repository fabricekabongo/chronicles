# Chronicles

Step 1-4 scaffold for the distributed replicated engine spec.

## Included in this increment

- Repository skeleton with `chroniclesd` and `chroniclesctl` commands.
- Config loader (`internal/config`) supporting YAML/TOML files and `CHRONICLES_*` env overrides.
- Feature flags to enable socket/Kafka/RabbitMQ adapters simultaneously.
- Core domain envelopes and route/commit metadata structs.
- Deterministic hash routing (`partition_id = hash(canonical_stream_key) % 25`).
- UTC-pinned route creation day on first accepted event.
- Multi-Raft partition engine (`internal/raftengine`) using `go.etcd.io/raft/v3` with:
  - 25 partition workers/goroutines (one Raft group each)
  - leader-only propose semantics for external writes
  - TCP length-prefixed protobuf Raft transport, partition-multiplexed
  - apply-as-commit command handling + ACK callback hooks
  - restart support via persisted per-partition raft memory stores
- Integration tests for election, quorum commit behavior, no-ACK without quorum, leader crash/restart recovery, and no split-brain leadership per term.
- SQLite storage engine scaffolding (`internal/storage/sqlite`) with:
  - per-partition catalog DBs (`catalog-pXX.db`)
  - per-day/per-partition event DBs (`events-YYYY-MM-DD-pXX.db`)
  - WAL + FULL synchronous pragmas
  - append-only triggers forbidding `UPDATE` and `DELETE` on entries
  - committed batch append, route upsert, and stream query paths (commit order and visual order)
- Unit/property tests for routing, config behavior, and SQLite storage guarantees.

## Run

```bash
go test ./...
go build ./...
```
