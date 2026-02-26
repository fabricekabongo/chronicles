# Chronicles

Step 1-3 scaffold for the distributed replicated engine spec.

## Included in this increment

- Repository skeleton with `chroniclesd` and `chroniclesctl` commands.
- Config loader (`internal/config`) supporting YAML/TOML files and `CHRONICLES_*` env overrides.
- Feature flags to enable socket/Kafka/RabbitMQ adapters simultaneously.
- Core domain envelopes and route/commit metadata structs.
- Deterministic hash routing (`partition_id = hash(canonical_stream_key) % 25`).
- UTC-pinned route creation day on first accepted event.
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
