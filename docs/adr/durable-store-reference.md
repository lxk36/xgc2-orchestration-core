# ADR: Durable store port and local reference adapter

- Status: accepted for S3
- Date: 2026-08-12
- Decision owner: G4

## Decision

`durable/store` is the orchestration durability port. One command transaction
atomically persists:

- exact aggregate revisions under compare-and-swap;
- one immutable DomainEvent for every mutated aggregate revision;
- zero or more durable outbox, reconcile, cleanup, or wait intents;
- the command identity and canonical outcome used for idempotent replay.

Inbox dedupe and intent lease transitions use the same durable authority. An
expired lease can be adopted with a new token/revision; the old worker is then
fenced. A worker may retry only when its handler explicitly reports a
`transient` failure and a future availability time. Unknown/uncertain outcomes
are not inferred to be retryable.

`durable/filestore` is the first-stage local Docker reference adapter. It uses
only the Go standard library, holds an exclusive process lock, forces mode
`0600`, and appends a length-delimited SHA-256 frame followed by `fsync` for
every change. Each frame is a complete state checkpoint, so an incomplete
final crash frame is truncated to the last verified boundary. A checksum or
semantic mismatch in a complete frame fails closed as corruption.

## Scope boundary

The file adapter targets one local controller and favors a small, inspectable
recovery implementation over write throughput. It is not a multi-controller
database, consensus protocol, or production HA claim. Public products depend
on the Store port, not this adapter. A PostgreSQL or other HA adapter must pass
the same conformance suite before public-server deployment.

Private lease/idempotency/capability tokens are absent from aggregate/event
JSON. Durable external dispatch must use a sealed private capsule or an opaque
secure-artifact reference resolved by the worker; token hashes remain public
audit evidence.
