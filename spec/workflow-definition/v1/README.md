# Workflow definition v1

A definition is a product-neutral DAG of typed node invocations. Both control and data edges are conjunctive dependencies. A `nodes.<id>.output...` reference additionally requires a direct data edge and that the producer dominate the consumer on every reachable control path after excluding the consumer's incoming data-only edges. Excluding those edges prevents several valid data bindings from a serial control chain from manufacturing false shortcut paths around one another.

Every node input is assembled from authored fixed values, typed bindings, and direct secret-slot references before dispatch. A child Action receives only its explicit `inputMap`, `triggerMap`, and `scopeMap`; there is no inherited parent context or pass-through-all option. Its structured result crosses back only through `resultMap`.
