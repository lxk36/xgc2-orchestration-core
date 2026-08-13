# Node extension protocol v1

Node packs are independently versioned modules. A Go node pack implements
`sdk/go/node.Executor`; other languages may use the same descriptor,
invocation, and result JSON envelopes out of process.

Hard boundaries:

- Descriptor digest is the canonical digest of the descriptor with
  `descriptorDigest` set to the empty string.
- The registry freezes exact package and descriptor digests before a Run.
- A request contains immutable structured input plus its canonical digest.
- Capability grants are explicit, least-privilege, opaque references. No raw
  capability or secret token appears in the public envelope.
- Executors do not receive a Store, Provider, clock, mutable Run, or ambient
  expression environment.
- Nodes cannot perform governed external mutation directly. Effectful nodes
  return declarative Effect proposals whose kind and capabilities must be
  predeclared. The Effect kernel durably prepares them before a Provider runs.
- Waiting is an explicit durable result (timer, event, approval, or Effect),
  not a blocked goroutine.
- Structured outputs are schema-checked, size-bounded, and content-addressed.

Independent packs should invoke `conformance/nodepack.Validate` in their tests.
Deterministic nodes are executed twice with the same request and must produce
byte-identical canonical results.
