# Orchestration state protocol v1

This directory publishes the product-neutral wire envelope for immutable Run,
Invocation/Attempt, Effect, Command/Receipt, and DomainEvent snapshots.

Every document is wrapped by:

```json
{
  "schemaVersion": "xgc.orchestration-state/v1",
  "kind": "run",
  "value": {}
}
```

`kind` selects one closed value schema in `schema.json`. Unknown fields fail
closed. Raw secret values, capability tokens, lease tokens, and idempotency
keys are private dispatch material and are deliberately absent. Their public
state contains only content identities or opaque references.

The reducer implementation is in `kernel/execution` and `kernel/effect`.
Storage implementations must commit the aggregate revision, DomainEvent, and
all DurableIntents from one Decision atomically. A persisted Decision may be
replayed, but adapters may only receive commands by claiming the durable
outbox. `uncertain` is not failure: only reconciliation of the original
command identity is permitted.
