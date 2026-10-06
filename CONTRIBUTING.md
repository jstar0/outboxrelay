# Contributing to OutboxRelay

Keep changes small and deterministic. A behavior change needs a focused `go test`
case covering its command, ledger, relay, or recovery consequence.

Do not add customer events, real broker endpoints, credentials, deployment
manifests or claims of exactly-once processing. Describe any change to snapshot
or event semantics in the pull request.
