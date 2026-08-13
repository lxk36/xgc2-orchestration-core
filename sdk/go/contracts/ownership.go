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

type OwnershipGraph struct {
	Run         Run                     `json:"run"`
	Revision    uint64                  `json:"revision"`
	Invocations []InvocationLedger      `json:"invocations"`
	ChildRuns   []Run                   `json:"childRuns"`
	Effects     []EffectRecord          `json:"effects"`
	Runtimes    []RuntimeBinding        `json:"runtimes"`
	Resources   []ResourceOwnershipFact `json:"resources"`
}
