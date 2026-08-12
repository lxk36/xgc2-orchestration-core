# ADR: Destructive cutover from current product execution paths

- Status: accepted migration policy; no product cutover occurs in S1
- Date: 2026-08-12
- Decision owners: G4; reviewed by G0

## Context

The current product execution code is deeply coupled to product handlers,
stores, parameter interpolation, and runtime behavior. Wrapping it behind new
names would preserve two authorities and create an apparent orchestration layer
without moving control or invariants into the platform.

## Decision

Migration from the current product implementation to platform v1 is a sequence
of explicitly leased destructive cutover slices. A cutover does not retain:

- legacy API or store compatibility;
- dual-read, dual-write, or dual-protocol paths;
- fallback to an old handler, parser, or expression evaluator;
- a long-lived adapter that makes the old engine a second authority.

Each migration slice must name one switch point, the replaced symbols and data,
the same-slice deletion list, fixtures that prove the new authority, and a
whole-version rollback or redeploy procedure. Local development definitions
and Run data may be rebuilt. Authored definitions may be converted offline only
when the conversion can be proven; unresolved expressions require explicit
author repair.

Isolated shadow compilation against copied input is allowed before cutover.
Shadow code cannot read or write production execution truth, dispatch a worker,
or duplicate an external effect.

After v1 is publicly adopted, a future public protocol upgrade may use an
expand/migrate/contract lifecycle. That policy does not retroactively authorize
a compatibility layer for the current product migration.

## Consequences

There is one execution authority after every cutover and failures are visible
instead of silently falling back. Migration slices carry more demanding
deletion and rollback evidence, but they avoid permanent split-brain semantics.

S1 adds only contracts, canonicalization, admission, expression checking, and
workflow compilation in the new repository. It modifies no product call site,
store, database, or runtime.
