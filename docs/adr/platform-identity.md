# ADR: Repository brand and product-neutral identity

- Status: accepted for S1
- Date: 2026-08-12
- Decision owners: G4; reviewed by G0

## Context

Public source repositories in this project family use an `xgc2-` prefix. The
orchestration core must nevertheless be independently usable by several
products and third-party node authors. A repository discovery convention must
not leak one product's resources, database, or lifecycle into the kernel.

## Decision

The source repository is:

```text
github.com/lxk36/xgc2-orchestration-core
```

Its public display name is **XGC Orchestration Core**. Portable
protocol, workflow, package, and node identities use `xgc.*`, for example
`xgc.workflow/v1` and `xgc.node.transform/v1`. Default namespaces are neutral,
such as `default` and `local`.

The Go module path necessarily contains the Git repository name. Go package
names and public kernel types remain product-neutral. Product adapters may use
their own qualified identities at the boundary; those identities do not grant
special kernel capabilities.

The repository must prove neutrality through executable gates:

- kernel packages do not import product, Studio, Controller, ORM, or node-host
  implementations;
- the reference definition compiles without another source tree;
- the reference definition contains no product-domain terminology;
- contracts model Action, Workflow, Invocation inputs, schemas, and
  capabilities, not a particular product's resources.

## Consequences

Repository discovery remains consistent with the existing GitHub family while
protocol identities can survive a future repository move. New consumers do not
need to impersonate a specific product. Product-specific builders and adapters
remain replaceable modules outside the pure kernel.

S1 proves compile-time independence only. Standalone Controller restart,
persistence, execution, and container acceptance belong to later slices and
must not be inferred from this ADR.
