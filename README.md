# Chronicles

Step 1-2 scaffold for the distributed replicated engine spec.

## Included in this increment

- Repository skeleton with `chroniclesd` and `chroniclesctl` commands.
- Config loader (`internal/config`) supporting YAML/TOML files and `CHRONICLES_*` env overrides.
- Feature flags to enable socket/Kafka/RabbitMQ adapters simultaneously.
- Core domain envelopes and route/commit metadata structs.
- Deterministic hash routing (`partition_id = hash(canonical_stream_key) % 25`).
- UTC-pinned route creation day on first accepted event.
- Unit/property tests for routing determinism and config behavior.

## Run

```bash
go test ./...
go build ./...
```
