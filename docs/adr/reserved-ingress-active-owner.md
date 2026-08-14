# Reserved ingress and active Run ownership

Status: implemented

Product builders are a privileged root ingress. `Controller.Invoke` therefore
rejects every root `trigger.product-builder/v1` unless the host presents an
in-memory `IngressPermit` issued together with a `ReservedIngressPolicy` and
registered on that exact Controller. A namespace named by any registered
policy is also closed to generic root ingress. Permits have an unexported,
random non-zero seal; policy refs, booleans, trigger payloads, and serialized
values are not capabilities.

The capability is defined in `kernel/ingress` so the Controller and exact
Action catalog consume the same seal without a reverse package dependency.
An Action catalog configured with a reserved policy rejects generic installs
in that namespace; the matching permit is required, must remain namespace
scoped, and the Action must accept the policy's frozen trigger kind. The
catalog record persists the authorizing policy ref and digest.

The policy freezes namespace, trigger kind/version, source, candidate origin,
root-only behavior, and—when requested—the active-owner kind and exact sorted
identity field set. The canonical policy digest is persisted on the Run.
Genuine child Action calls use a package-private proof bound to their exact
parent, invocation, call node, child Run, Action, mappings, inputs, trigger,
scope, and command. The public Invoke method cannot manufacture that proof.

`ActiveOwnerKey` is the canonical JSON digest of namespace, product-neutral
kind, and an opaque string map. For XGC2 experiments the intended key kind is
`configuration.resource-branch`, with `domain`, `resourceId`, and `branch` as
identity fields. Commit hashes and definition digests deliberately do not
replace branch identity: a new immutable definition on the same branch must
still contend for the same live slot, while two branches may run separately.

The mutable `active-run-owner` aggregate is a non-expiring CAS fence:

- active revision is `2 * generation - 1`;
- released revision is `2 * generation`;
- acquire commits Run, Snapshot, owner, and a create-only admission receipt in
  one transaction;
- success, failure, cancellation, or stop commits the terminal Run, immutable
  ownership graph, and owner release in one transaction;
- stopping, cleanup, compensation, waiting, and process restart retain the
  active owner.

The create-only receipt freezes the original accepted Run and complete Start
request identity. It makes an old exact Start replayable after release and even
after a later generation owns the key. A changed request under the same
command is an identity conflict. Two different Runs racing for one key use the
same owner CAS; exactly one transaction publishes, and the loser publishes no
Run or Snapshot.

`GetActiveRunOwner` directly reads the exact owner aggregate.
`ResolveActiveRun` additionally cross-checks its Run, generation, namespace,
and policy fence. Owner-backed Runs can only be stopped through
`RequestActiveRunTermination` with the exact canonical key and Run ID; generic
termination fails closed. The same fence applies to external event, approval,
and timer waits: generic `ResolveExternalWait` rejects owner-backed Runs, while
`ResolveActiveExternalWait` authorizes the exact canonical key and Run before
reading a resolution receipt or mutating the wait. Exact terminal command
replays remain valid after owner release; a foreign key or changed command is
rejected.

Every Trigger also carries the canonical digest of the pinned Workflow
`TriggerSchema`. `Invoke` compares that digest before validating the payload or
creating a Run, so a caller cannot attach an unrelated schema identity to an
otherwise schema-valid event.
