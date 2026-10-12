# OutboxRelay

**A deterministic local reference for transactional outbox delivery, idempotent consumers, retry, and compensation projections.**

OutboxRelay is a small Go workflow simulator. A command changes a synthetic task
state and appends an outbox event in one in-memory transaction. A relay later
delivers pending events at least once; the consumer deduplicates event IDs, so a
retry cannot apply the same projection twice.

```text
idempotent command → task transition + outbox event → relay → consumer projection
                                         │                  │
                                         └── snapshot/recover └── deduplicate/retry
```

## Run it

Requires Go 1.24+.

```bash
go test ./...
go run ./cmd/outboxrelay --output-dir outputs/demo
cat outputs/demo/report.md
```

The demo is fully synthetic. It writes a versioned snapshot, final consumer
projection, Markdown summary and a SHA-256 manifest. It uses no network,
message broker, customer task, credential or external service.

## What is demonstrated

- Idempotency keys replay an identical command and reject a different command
  under the same key.
- Valid transitions are `reserve → confirm` or `reserve → compensate`.
- A task transition and its event are committed in one engine operation before
  relay delivery, so a snapshot can recover pending events after a simulated
  crash boundary.
- The relay is **at least once**. A delivery failure leaves the event pending;
  retrying it invokes an idempotent consumer that applies each event ID once.
- The consumer persists its received event identities in `consumer_snapshot.json`;
  strict recovery rebuilds its projection before a relay retry, so the crash
  window after consumer apply and before delivery acknowledgement remains a
  no-duplicate replay.
- The CLI persists `snapshot.json`, strictly decodes that JSON again, then
  restores only a ledger with valid event transitions, reconstructed task
  states, sensible delivery counters, unique payloads, and one-to-one
  idempotency references before resuming.

## Scope and limits

OutboxRelay is not a distributed broker, database transaction manager, Kafka
client, payment system, or exactly-once guarantee. State lives in one process;
its JSON snapshot is an inspectable simulation artifact. The point is to make
the delivery and recovery contract readable and testable without claiming a
production deployment.

## Development

```bash
go test ./...
go vet ./...
go build ./...
```

See [docs/architecture.md](docs/architecture.md) for the model boundary and
[docs/design-decisions.md](docs/design-decisions.md) for rejected alternatives
and known weaknesses.

## Scope

This is an independent implementation. It contains no
client source, client schema, private message payload, broker configuration,
credential or deployment artifact. It draws only on general distributed-systems
experience and uses a new synthetic task vocabulary and API.

## License

MIT; see [LICENSE](LICENSE).
