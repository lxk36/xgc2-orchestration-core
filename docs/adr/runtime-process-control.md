# ADR: Runtime ownership and Linux process control

- Status: accepted for S4
- Date: 2026-08-13
- Decision owner: G4

## Decision

Run status is not process status. Long-lived external objects use an explicit
`RuntimeBinding` aggregate containing:

- the exact Run, Invocation, node, runtime key, spec, and provider identities;
- `owned`, `attached`, or `shared` ownership and a compatible cleanup policy;
- binding lifecycle, provider-observed lifecycle, and health as separate facts;
- monotonically increasing generation and fencing token;
- a hashed expiring controller lease and immutable observation evidence.

An expired binding may be taken over only by `generation + 1` with a strictly
higher fencing token. Takeover clears the old observation and emits a durable
reconciliation intent. Providers keep a per-binding generation high-water
mark: old-generation commands are rejected before mutation even if their
process identity still exists.

Owned bindings enter `stopping` and emit cleanup work. They become `released`
only after the provider proves the exact external identity is stopped.
Attached/shared bindings only detach; they never issue destructive cleanup.
Lost is explicit and does not masquerade as stopped.

`kernel/ownership` derives Run closure facts from an exact ownership graph
revision. It counts active invocations/attempts/waits/children, primary or
uncertain Effects, required Effect compensation, owned Runtime bindings, and
owned resource bindings. Run terminal reducers consume these facts rather than
inferring cleanup from a product status field.

## Local process provider

`provider/process` is the private dispatch port. Public ProcessSpec values use
references/digests; resolved executable paths, arguments, environments, log
paths, idempotency keys, and capability tokens remain private.

The Linux reference provider:

- calls `execve` through `os/exec` without an implicit shell;
- creates a dedicated process group and records PID, PGID, and Linux
  `/proc` start ticks;
- validates the exact identity before every group signal, preventing PID reuse
  from targeting an unrelated process;
- sends TERM, waits the bounded grace period, then sends KILL and waits for
  reap/absence;
- returns fenced immutable accepted/rejected/succeeded/uncertain receipts;
- persists no raw token in a public ledger.

Remote, Docker, Kubernetes, ROS launch, simulator, and hardware providers must
implement the same port and receipt/fencing rules. They are node/provider
modules, not branches inside the orchestration reducer.
