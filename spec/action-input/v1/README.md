# Action input v1

Every ingress adapter resolves an exact `ActionVersion`, normalizes one `TriggerEvent`, and calls the same admission function. Inputs are merged in one fixed order: schema defaults, exact preset values, then an origin-checked candidate. The admitted object, trigger, digests, and leaf provenance are immutable Run facts.

Handlers cannot add defaults or merge trigger payload, UI state, environment variables, or a previous Run. Panel ingress requires an exact preset. Preset overrides are exact non-root JSON Pointers.

Presets are accepted only by Manual and Panel ingress. Schedule and webhook adapters must freeze their versioned input template or mapping instead of resolving a preset again at admission time.
