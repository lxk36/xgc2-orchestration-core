# Exact durable Run termination

Status: accepted for v1

## Decision

Stopping a Run is a durable orchestration command, not a UI gesture and not a
best-effort process scan. `Controller.RequestRunTermination` first commits one
immutable `TerminationIntent` under the caller's exact Run revision and command
identity. Only after that commit may the controller cancel Invocations, close
prepared Effects, propagate termination to child Runs, or compensate applied
Effects.

The intent persists that pre-transition revision as `requestedRevision`.
Replay comparison includes the Run ID and this revision; reusing one command
with a different revision is an identity conflict even after the Run and its
active-owner generation have become terminal.

The contract deliberately separates three cases:

1. A `prepared` Effect has not crossed the provider boundary. It may become
   `canceled` without inventing a provider Receipt.
2. An `applying` Effect may already have changed the target. It remains open
   until its exact Receipt or reconciliation is persisted.
3. An `applied` owned Effect is compensated in reverse workflow order. Its
   compensation intent, command envelope, Receipts, and terminal state survive
   controller restart. Required compensation closes ownership only on
   `succeeded`, or on a revision-fenced `reconciled` record carrying an exact
   evidence digest, authority, reason, command, and observation time. Failed,
   canceled, and retry-wait compensation remain open ownership.

Invocation cancellation is fenced by the frozen Run termination command and
aggregate revision, not by disclosure of the worker's private lease token. A
late worker result therefore loses the aggregate CAS and cannot revive a
canceled Invocation or a newer Run generation.

## Idempotency and concurrency

The same termination command and bytes return the first durable outcome with
`Replay=true`. Reusing the command identity with changed bytes is an identity
conflict. A different command against the old Run revision is a revision
conflict. Consequently two concurrent copies of one exact Stop produce one
new decision and one replay, not two cleanup scopes.

## Closure

Before a failed, stopped, or canceled Run becomes terminal:

- direct child Runs have reached their own terminal ownership closure;
- every local Invocation and Attempt is terminal;
- pre-dispatch Effects are canceled;
- applying Effects have terminal provider evidence;
- applied owned Effects have the compensation evidence required by policy;
- the Run cleanup signal and any late wait/child-resolution signal are
  completed; and
- the persisted `OwnershipGraph` derives zero open counts.

The closure record is a single `xgc.ownership-graph/v1` envelope. It persists
the exact pre-terminal `closureBase`, the exactly derived `closureFacts`, and
the exact next-revision `terminalRun` in the same transaction as the Run
aggregate. `closureFacts.runRevision` always names the closure-base Run, never
the terminal projection. Readers validate the complete envelope and canonical
terminal Run equality; they do not join mutable state into the proof snapshot.
The graph is a create-once aggregate at revision 1. Any pre-existing graph
prevents a later terminal transition instead of being overwritten.

The coordinator continues outbox, wait-resolution, child-resolution, and
cleanup intents while a Run is stopping. Settlement mechanically derives each
required intent identity. Missing or dead intent records fail closed, and a
terminal Effect must persist the exact revision that emitted its wait
resolution; there is no current-revision fallback for older records. These
rules apply equally to failed and canceled termination. The coordinator never
converts a missing provider observation into success. An uncertain Effect
therefore keeps closure open until an explicit reconciler supplies evidence.

## Product boundary

Products expose this command through their own authorization and API ports,
but may not reimplement cancellation, scan by process name/PID, or treat panel
unmount as Stop. A Panel, Agent, API, or scheduler must address the exact Run
revision and command identity; target-specific teardown remains inside Effect
adapters.
