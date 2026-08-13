# Workflow definition v1

A definition is a product-neutral DAG of typed node invocations. Both control and data edges are conjunctive dependencies. A `nodes.<id>.output...` reference additionally requires a direct data edge and that the producer dominate the consumer in the pure control-edge projection. Excluding all data edges from dominance prevents valid upstream and downstream data joins from manufacturing false alternative paths around serial control barriers.

Every node input is assembled from authored fixed values, typed bindings, and direct secret-slot references before dispatch. A child Action receives only its explicit `inputMap`, `triggerMap`, and `scopeMap`; there is no inherited parent context or pass-through-all option. Its structured result crosses back only through `resultMap`.
