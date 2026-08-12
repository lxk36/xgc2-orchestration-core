# Value expression v1

The durable wire form is a bounded structured AST, never JavaScript, a shell fragment, or a template language. Its six namespaces are `inputs`, `trigger`, `scope`, `nodes`, `iteration`, and `secrets`. Node output reads require a direct data edge and graph dominance.

Secret handles can only be direct references assigned to declared secret slots. They cannot enter operators, objects, arrays, logs, outputs, or public input fields.

An optional path that is absent evaluates to `missing`, which is not a JSON value. Only `coalesce` may consume `missing`; every other top-level, operator, object, or array use fails closed.
