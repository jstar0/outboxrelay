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
