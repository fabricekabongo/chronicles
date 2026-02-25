# Chronicles

Initial Step 0 scaffolding for the distributed replicated engine spec.

## Step 0 invariants encoded

- Append-only event model (`Event` as immutable record; corrections should be represented as new events).
- Quorum majority math helper (`QuorumSize`).
- Deterministic routing (`partition_id = hash(stream_key) % 25`).
- First accepted event creates immutable route entry (`Router.EnsureRoute`).
- Routing day pinned in UTC (`CreationDayUTC` as `YYYY-MM-DD`, UTC-only).
- Generic stream identity via `subject_type + stream_key`.
- Dual time model fields on events:
  - `event_time_utc` (producer timeline)
  - `received_at_utc` / `committed_at_utc` (Chronicles operational timeline)

## Run tests

```bash
go test ./...
```
