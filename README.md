# XGC Execution & Control Platform

Product-neutral contracts and execution-control infrastructure for workflows,
managed processes, tools, and bounded agent work.

The repository keeps the existing `xgc2-` GitHub family prefix, while its
portable protocol and package identities use `xgc.*`. The kernel does not
import a product source tree or interpret product resources.

## Current scope

S1 provides a standard-library-only Go contract kernel:

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
  reference definition.

S1 is not a Controller or worker runtime. It does not yet provide persistence,
durable queues, process supervision, remote execution, a Studio, HTTP APIs, or
container deployment.

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
| Language-facing contract types | `sdk/go/contracts` |
| Cross-cutting acceptance gates | `conformance/architecture`, `conformance/fixtures` |

Public definitions never embed secret values. Workers in later slices must
receive already-resolved immutable invocation inputs, not an ambient Run
context or an expression evaluator.

## Canonical JSON profile

Content identities use one bounded profile: exactly one valid UTF-8 JSON value,
no duplicate object member after escape decoding, object keys sorted by their
decoded UTF-8 bytes, no insignificant whitespace, and arrays kept in authored
order. Strings use the short JSON escapes for quote, backslash, and the five
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
