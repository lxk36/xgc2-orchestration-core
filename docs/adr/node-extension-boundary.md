# ADR: Node packs are declarative extension modules

- Status: accepted for S5
- Date: 2026-08-13
- Decision owner: G4

## Decision

The orchestration core publishes one product-neutral Node protocol and a small
Go Executor SDK. Node packs live in independent `xgc2-nodes-*` repositories.
The repository prefix is a discovery convention; portable node identities use
`xgc.*` versioned type refs.

A descriptor freezes exact package/descriptor digests, structured input/output
schemas, execution mode, determinism, capability requirements, allowed Effect
kinds, compensation type, and byte limits. The sealed registry digest becomes
Run evidence. Duplicate type refs with different content fail closed.

Executor input is limited to an immutable request: exact invocation/attempt
identity, structured input/digest, deadline, and opaque least-privilege grants.
There is no Store, Provider, clock, mutable Run, expression evaluator, or
ambient environment in the interface.

An Executor returns exactly one structured outcome:

- success with schema-valid content-addressed output;
- durable wait for timer/event/approval/Effect;
- structured failure;
- for an effectful node, zero or more declarative Effect proposals restricted
  to descriptor-declared kinds and actually granted capabilities.

The node does not execute governed mutation. The host converts accepted
proposals to stable Effect intents, commits prepare/Event/outbox atomically,
then dispatches through a fenced Provider. This is the reliability boundary
for MCP calls, model calls, storage, processes, robotics, releases, and other
external systems.

`conformance/nodepack` is mandatory for published packs. It validates the
sealed catalog, every request/result pair, coverage of every node type, and
byte-identical replay for deterministic nodes.

## Repository split

Initial public packs are:

- `xgc2-nodes-robotics`: simulation, ROS, robot, fleet, experiment probes;
- `xgc2-nodes-agent`: model, expert, tool-loop, MCP, approval, memory policy;
- `xgc2-nodes-dev`: marker intake, repository analysis, patch/test/review;
- `xgc2-nodes-knowledge`: artifact parsing, retrieval, long/short memory,
  citation and user-document production;
- `xgc2-nodes-delivery`: build, package, release, deployment and rollback.

Pack dependencies point inward only to `xgc2-orchestration-core`; the core
does not import packs. XGC2, Agent Hub, experiment automation, and later
products compose the same packs through workflows instead of adding product
switches to the kernel.
