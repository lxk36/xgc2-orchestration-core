# ADR: Durable store port and local reference adapter

- Status: accepted for S3
- Date: 2026-08-12
- Updated: 2026-08-13 (destructive filestore v2)
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
only the Go standard library. Its path must be absolute and clean, its parent
must already exist at the same canonical path without symlink traversal, and
the data and lock targets must each be regular, single-link files. Opens use
`O_NOFOLLOW|O_CLOEXEC`; symlinks, non-regular files, hardlink aliases, and a
data/lock inode collision fail closed.

The exclusive process lock lives on the stable `<data>.lock` sidecar, not on
the data inode replaced by compaction. `Open` pins the canonical parent
directory and acquires the sidecar lock before opening or recovering data. It
holds that lock until `Close`, including across data renames. Operations verify
authority by re-walking every absolute parent component with
`O_NOFOLLOW|O_DIRECTORY|O_CLOEXEC`, comparing the result to the pinned parent,
then resolving lock and data names relative to that verified directory. Parent
or ancestor symlink replacement (even a symlink back to the original inode),
sidecar replacement, data-path replacement, and link-count changes are thereby
detected. Both files are mode `0600`; the sidecar is persistent rather than
deleted and recreated.

The only accepted disk framing is destructive v2. There is no v1/legacy
fallback. One complete state checkpoint is encoded as:

- a 96-byte header containing the v2 magic and version, fixed header size,
  zero flags, payload length, total encoded length, payload SHA-256 digest, and
  a SHA-256 digest of the preceding header fields;
- a deterministic JSON checkpoint, at most 64 MiB, whose state version is 2;
- a 128-byte commit footer containing a distinct commit magic, v2 version,
  fixed footer size, repeated payload and total lengths, repeated payload and
  header digests, and a SHA-256 digest of the preceding footer fields.

The self-validating footer is the commit boundary. Recovery validates every
committed frame, including superseded frames; any magic, version, length,
header, payload, footer, strict JSON, or state invariant failure in a frame
whose declared bytes are present is corruption. Recovery never rolls back from
a corrupt committed frame. EOF before the declared end is the sole crash-tail
case. If the bounded remaining bytes contain a self-validating commit footer at
its repeated frame boundary, an inconsistent header length is committed
corruption instead. Otherwise the incomplete suffix is truncated to the last
fully verified boundary and the data file is synced.

Recovery has hard work limits: the data file may not exceed one maximum encoded
frame (64 MiB plus 224 framing bytes), no payload may exceed 64 MiB, and no more
than four frames are decoded. The footer search for a length-mismatched crash
tail is bounded by that same file-size limit. Normal appends allow at most four
committed frames and use the same one-maximum-frame journal byte budget (64 MiB
plus 224 framing bytes). Before a fifth frame, or before an append would exceed
the byte budget, the adapter writes one checkpoint to a mode-`0600` stage file
in the pinned parent, syncs it, renames it over the data name, and syncs the
pinned parent.

Every ordinary state change writes a whole v2 frame and syncs the data fd
before publishing the in-memory state. A write, data-sync, stage-write,
rename, directory-sync, or replaced-fd-close error poisons the handle because
the durable outcome is uncertain; later operations fail until close and
reopen. `Open` itself reports success only after recovery/compaction and after
syncing the sidecar fd, current data fd, and pinned canonical parent. Any
failure releases resources and returns no Store. `Close` attempts data close,
sidecar unlock, sidecar close, and parent close and joins all errors.

Aggregate payloads, event/intent payloads, command outcomes, and every digest
cross their bounded canonical-JSON validation boundary before entering durable
state. The enclosing checkpoint is not itself a protocol identity: its header
hash authenticates the exact encoded bytes. The writer therefore serializes
that already validated state directly instead of reparsing and re-emitting the
whole checkpoint through the canonical identity codec. Recovery still applies
the bounded strict parser and all record/digest invariants before publishing a
single byte. In memory, commits fork fresh map indexes and share unchanged
immutable records; changed records are always replaced and every read returns
a defensive copy. This preserves all-or-nothing publication without an
O(total payload) JSON round trip before every append.

## Scope boundary

The file adapter targets one local controller and favors a small, inspectable
recovery implementation over write throughput. It is not a multi-controller
database, consensus protocol, or production HA claim. Public products depend
on the Store port, not this adapter. A PostgreSQL or other HA adapter must pass
the same conformance suite before public-server deployment.

The v2 switch intentionally does not read v1 files. Local data is rebuilt for
this destructive cutover; rollback is a whole-version redeploy with data built
for that version, not a dual reader or compatibility layer.

Private lease/idempotency/capability tokens are absent from aggregate/event
JSON. Durable external dispatch must use a sealed private capsule or an opaque
secure-artifact reference resolved by the worker; token hashes remain public
audit evidence.
