# OutboxRelay architecture

The engine owns three consistent in-memory ledgers: task state, idempotency
keys, and outbox events. `Apply` validates a command, updates the task and
appends an event before returning. A caller can persist `Snapshot()` at that
point, before a relay has delivered anything.

`RelayPending` delivers non-acknowledged events in sequence. A failed sequence
remains pending. `Consumer` records received event IDs and projects the latest
event type for each task; repeated delivery of the same ID is a no-op.
`ConsumerSnapshot` persists the minimal received-event ledger; its restore path
strictly decodes JSON and reconstructs the projection from valid event
transitions instead of trusting a separately written projection map.

`DecodeSnapshot` accepts exactly one JSON value, rejects unknown fields, then
uses `Restore`. Recovery accepts only version 1 snapshots with contiguous event
sequence/ID pairs, canonical SHA-256-shaped payload digests, known task
references, valid state transitions, task states reconstructed from the event
ledger, sensible delivery counters, and a one-to-one idempotency ledger. It
models structural ledger integrity, not a signed or distributed transaction
protocol.
