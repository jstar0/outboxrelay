# Design decisions

This document names the real contract, alternatives it rejects, and the limits
a reviewer should challenge.

## 1. Command state and outbox event commit together

`Engine.Apply` updates a synthetic task and appends its event in the same
in-memory operation. The snapshot is taken after that point and before relay
delivery, so a restored engine can see pending work.

Rejected alternative: emit an event after changing task state in another call.
A crash between the calls creates the exact lost-event gap the outbox pattern is
meant to expose.

## 2. Idempotency keys bind command content

The key maps to a hash of task ID and action. Retrying the same command returns
the existing event; using that key for a different action fails.

Rejected alternative: deduplicate only by key. That accepts a client bug where
one retry key is accidentally reused for another mutation.

## 3. Relay semantics are at least once, consumer semantics are idempotent and recoverable

A failed delivery stays pending. The consumer stores received event IDs and does
not apply a duplicate projection. Its compact snapshot persists those IDs and
rebuilds the projection from a validated ordered event ledger, so a consumer
restart after applying an event but before relay acknowledgement still treats a
redelivery as a no-op. This makes retry visible without claiming a broker or
end-to-end exactly-once guarantee.

Rejected alternative: mark delivery complete before calling the consumer. A
crash then loses the event permanently.

## 4. Decode and restore validate ledger shape before replay

`DecodeSnapshot` rejects unknown fields and trailing JSON before `Restore`
checks version, event sequence, stable ID, task reference, canonical payload
digest shape, delivery counters, valid event transitions, terminal task state,
unique payload hash, and one-to-one idempotency references. A structurally
modified snapshot fails before any new command can use it. The CLI persists the
snapshot and recovers through this JSON boundary before relaying it.

Rejected alternative: deserialize JSON into maps and trust it because this demo
wrote it. Snapshots are precisely what gets copied and edited during recovery
debugging.

## Known weaknesses

1. **One process only.** There is no database transaction, lock, broker, lease,
   network partition or multi-writer ordering.
2. **No durable fsync protocol.** JSON is an inspectable artifact; a power-loss
   safe write-ahead log is not implemented.
3. **No authorization.** Command keys establish idempotency, not identity or
   permission to mutate a task.
4. **Consumer projection is intentionally tiny.** It restores a last-event
   projection from its received-event ledger, but no richer materialized-view
   rebuild or schema migration exists.
5. **No backoff schedule or dead-letter queue.** `failSequence` is deterministic
   failure injection, not a production retry policy.
6. **Payload hashes are not signatures.** They detect internal ledger mismatch,
   not a malicious actor who can rewrite the entire snapshot.
7. **The demo does not measure throughput or latency.** It is a correctness
   reference for synthetic events, not a broker benchmark.
