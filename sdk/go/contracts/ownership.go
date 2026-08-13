package contracts

type ResourceBindingState string

const (
	ResourceBindingActive    ResourceBindingState = "active"
	ResourceBindingReleasing ResourceBindingState = "releasing"
	ResourceBindingReleased  ResourceBindingState = "released"
	ResourceBindingLost      ResourceBindingState = "lost"
)

func (state ResourceBindingState) Valid() bool {
	switch state {
	case ResourceBindingActive, ResourceBindingReleasing, ResourceBindingReleased, ResourceBindingLost:
		return true
	default:
		return false
	}
}

func (state ResourceBindingState) Terminal() bool {
	return state == ResourceBindingReleased || state == ResourceBindingLost
}

type ResourceOwnershipFact struct {
	BindingID string               `json:"bindingId"`
	RunID     string               `json:"runId"`
	Ownership EffectOwnership      `json:"ownership"`
	State     ResourceBindingState `json:"state"`
}

const OwnershipGraphSchemaVersion = "xgc.ownership-graph/v1"

// OwnershipClosureBase is the exact pre-terminal state against which Run
// closure was proved. Its Run revision deliberately differs from TerminalRun
// by one; neither field is a projection of the other.
type OwnershipClosureBase struct {
	Run         Run                     `json:"run"`
	Invocations []InvocationLedger      `json:"invocations"`
	ChildRuns   []Run                   `json:"childRuns"`
	Effects     []EffectRecord          `json:"effects"`
	Runtimes    []RuntimeBinding        `json:"runtimes"`
	Resources   []ResourceOwnershipFact `json:"resources"`
}

// OwnershipGraph is the immutable, self-contained closure record committed in
// the same transaction as TerminalRun. ClosureFacts is persisted as the exact
// proof output and must equal a fresh derivation from ClosureBase. Each Run has
// exactly one graph, whose aggregate revision is always one.
type OwnershipGraph struct {
	SchemaVersion string               `json:"schemaVersion"`
	Revision      uint64               `json:"revision"`
	ClosureBase   OwnershipClosureBase `json:"closureBase"`
	ClosureFacts  RunClosureFacts      `json:"closureFacts"`
	TerminalRun   Run                  `json:"terminalRun"`
}
