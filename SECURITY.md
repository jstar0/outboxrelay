# Security policy

OutboxRelay ships synthetic commands and uses no network, database, queue or
credential. Treat external event payloads and snapshots as untrusted in any
future integration. The current restore validator checks structural integrity,
not signatures or authorization.

Do not include private task data, broker URLs, tokens or customer schemas in a
public issue. Use an agreed private channel for a suspected vulnerability.
