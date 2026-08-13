# XGC Orchestration Core

Product-neutral contracts and orchestration infrastructure for workflows,
managed processes, tools, experiments, and bounded agent work.

The repository keeps the existing `xgc2-` GitHub family prefix, while its
portable protocol and package identities use `xgc.*`. The kernel does not
import a product source tree or interpret product resources.

## Current scope

The standard-library-only Go kernel currently provides:

- bounded canonical JSON and `sha256:` content identities;
- one Action admission path for manual, schedule, webhook, API, Panel, and
  product-builder triggers;
- schema defaults, exact presets, controlled overrides, immutable inputs, and
  leaf provenance;
- a typed structured expression AST with six explicit namespaces;
- direct SecretHandle-to-secret-slot enforcement;
- deterministic DAG compilation, explicit data edges, dominance checks, and
  child Action input maps;
- v1 JSON Schemas, golden fixtures, architecture gates, and a standalone
  reference definition;
- deterministic Run, Invocation, and append-only Attempt reducers;
- mandatory stopping and ownership-closure proof before failed, canceled, or
  stopped Run termination;
- prepare-before-mutate Effect state, fenced Command envelopes, immutable
  Receipt ledgers, explicit uncertainty, and independent compensation;
- durable outbox/reconcile/cleanup intents as reducer output, without doing I/O;
- a durability port plus a locked, checksummed, fsynced local file adapter with
  command replay, revision CAS, inbox dedupe, expiring intent leases, and crash
  tail recovery;
- an intent worker that requires explicit complete/retry/dead/leave results and
  never treats uncertain external effects as automatically retryable;
- independent RuntimeBinding ownership, generation fencing, lease takeover,
  provider observation, and exact ownership-graph closure facts;
- a Linux local-process provider with process groups, PID/start-time reuse
  protection, TERM/KILL/reap control, generation high-water fencing, and
  immutable receipts;
- a sealed Node registry, descriptor/request/result protocol, declarative
  Effect proposal boundary, explicit capability grants, structured output, and
  reusable node-pack conformance suite;
- fail-closed v1 Effect result semantics: a waiting Node may propose exactly
  one Effect matching its durable wait key and intent digest; successful,
  failed, and non-Effect-wait results cannot carry Effect proposals;
- a durable single-controller runtime that admits exact Action/Workflow pins,
  executes only the selected entrypoint, freezes node inputs/results, prepares
  that Effect atomically before waiting, and recovers expired pure-node attempts
  while failing effectful uncertainty closed;
- atomic Effect begin, public CommandLedger persistence, leased outbox dispatch,
  private credential rehydration, immutable provider Receipt observation, and
  terminal wait-resolution intents;
- a lease-free but revision-and-generation-fenced wait resolver plus optional
  pure Node `Resume`, so an asynchronous node is never re-executed to consume
  an external result; and
- a product-neutral Effect adapter port and process adapter that resolves
  public `ProcessSpec` references into private local-provider dispatch data.

The repository does not yet provide remote execution, a Studio, HTTP APIs, or
container deployment. The file store and local-process provider are for a
single local controller; public-server HA storage is a separate adapter.

## Verify

Go 1.26.2 is the reviewed toolchain.

```bash
go test ./...
go vet ./...
```

The reference definition is in
[`examples/reference-workflow/workflow.json`](examples/reference-workflow/workflow.json).
It compiles using only this repository.

## Contract map

| Concern | Source |
| --- | --- |
| Action input and preset admission | `spec/action-input/v1`, `kernel/action` |
| Normalized ingress evidence | `spec/trigger-event/v1` |
| Typed bindings and namespace rules | `spec/value-expression/v1`, `kernel/expression` |
| Product-neutral DAG definition | `spec/workflow-definition/v1`, `kernel/workflow` |
| Run, Invocation, Attempt, Effect, Command, Receipt | `spec/orchestration-state/v1`, `kernel/execution`, `kernel/effect` |
| Atomic state/event/intent persistence | `durable/store`, `durable/filestore` |
| Fenced intent draining | `durable/worker` |
| Runtime ownership and Run closure | `kernel/runtime`, `kernel/ownership` |
| Effect/provider dispatch boundary | `provider/effect`, `controller/intent_handlers.go` |
| Managed process provider port/reference | `provider/process`, `provider/processadapter`, `provider/processlocal` |
| User-extensible Node protocol and SDK | `spec/node-protocol/v1`, `kernel/node`, `sdk/go/node` |
| Independent pack acceptance | `conformance/nodepack` |
| Durable Action-to-Node controller | `controller` |
| Language-facing contract types | `sdk/go/contracts` |
| Cross-cutting acceptance gates | `conformance/architecture`, `conformance/fixtures` |

Public definitions never embed secret values. Workers in later slices must
receive already-resolved immutable invocation inputs, not an ambient Run
context or an expression evaluator.

## Canonical JSON profile

Content identities use one bounded profile: exactly one valid UTF-8 JSON value,
no unpaired Unicode surrogate, no duplicate object member after escape
decoding, object keys sorted by their decoded UTF-8 bytes, no insignificant
whitespace, and arrays kept in authored order. Strings use the short JSON
escapes for quote, backslash, and the five
named controls; other C0 controls use lowercase `\u00xx`; all other UTF-8 stays
unescaped. Numbers use arbitrary-precision decimal normalization, never
exponent notation, and negative zero becomes `0`.

Default bounds are 1 MiB input/output, depth 128, 100,000 value nodes, 1 MiB
per decoded string, and absolute exponent 10,000. A content identity is the
lowercase form `sha256:<64 hex digits>` over those canonical bytes.

## Migration boundary

This repository is a new v1 authority, not a compatibility wrapper around the
current product implementation. Product adoption happens through separately
reviewed cutover slices that remove the replaced parser, handler, and store
paths at the same switch point. See
[`docs/adr/destructive-migration.md`](docs/adr/destructive-migration.md).
