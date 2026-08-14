package contracts

type EdgeKind string

const (
	EdgeControl EdgeKind = "control"
	EdgeData    EdgeKind = "data"
)

func (kind EdgeKind) Valid() bool {
	return kind == EdgeControl || kind == EdgeData
}

// EdgeCondition selects the terminal source outcome which may activate an
// edge. An omitted condition is normalized to success by the v2 compiler; the
// compiled plan never retains the empty form.
type EdgeCondition string

const (
	EdgeSuccess EdgeCondition = "success"
	EdgeFailure EdgeCondition = "failure"
	EdgeAlways  EdgeCondition = "always"
)

func (condition EdgeCondition) Valid() bool {
	return condition == EdgeSuccess || condition == EdgeFailure || condition == EdgeAlways
}

// NodeRetryPolicy is an authored, immutable execution policy. MaxAttempts
// includes the first attempt. Delays double from InitialBackoffMillis and are
// capped by MaxBackoffMillis; the kernel never adds jitter or product policy.
type NodeRetryPolicy struct {
	MaxAttempts          uint32 `json:"maxAttempts"`
	InitialBackoffMillis uint64 `json:"initialBackoffMillis"`
	MaxBackoffMillis     uint64 `json:"maxBackoffMillis"`
}

type WorkflowDefinition struct {
	SchemaVersion  string                    `json:"schemaVersion"`
	WorkflowID     string                    `json:"workflowId"`
	Version        string                    `json:"version"`
	InputSchema    Schema                    `json:"inputSchema"`
	ResultSchema   Schema                    `json:"resultSchema"`
	TriggerSchema  Schema                    `json:"triggerSchema"`
	ScopeSchema    Schema                    `json:"scopeSchema"`
	Secrets        []string                  `json:"secrets,omitempty"`
	Entrypoints    map[string]string         `json:"entrypoints"`
	Nodes          []WorkflowNodeDefinition  `json:"nodes"`
	Edges          []WorkflowEdge            `json:"edges"`
	ResultBindings map[string][]ValueBinding `json:"resultBindings,omitempty"`
}

type WorkflowNodeDefinition struct {
	NodeID           string           `json:"nodeId"`
	TypeRef          string           `json:"typeRef"`
	DescriptorDigest string           `json:"descriptorDigest"`
	InputSchema      Schema           `json:"inputSchema"`
	OutputSchema     Schema           `json:"outputSchema"`
	FixedInputs      map[string]any   `json:"fixedInputs,omitempty"`
	Bindings         []ValueBinding   `json:"bindings,omitempty"`
	CallAction       *CallAction      `json:"callAction,omitempty"`
	Retry            *NodeRetryPolicy `json:"retry,omitempty"`
}

type WorkflowEdge struct {
	From       string        `json:"from"`
	To         string        `json:"to"`
	Kind       EdgeKind      `json:"kind"`
	Condition  EdgeCondition `json:"condition,omitempty"`
	Route      string        `json:"route,omitempty"`
	SourcePort string        `json:"sourcePort,omitempty"`
}

type CallAction struct {
	TargetActionRef ActionRef       `json:"targetActionRef"`
	InputSchema     Schema          `json:"inputSchema"`
	TriggerSchema   Schema          `json:"triggerSchema"`
	ScopeSchema     Schema          `json:"scopeSchema"`
	ResultSchema    Schema          `json:"resultSchema"`
	InputMap        []ValueBinding  `json:"inputMap"`
	TriggerMap      []ValueBinding  `json:"triggerMap"`
	ScopeMap        []ValueBinding  `json:"scopeMap"`
	ResultMap       []ResultBinding `json:"resultMap,omitempty"`
}

type ResultBinding struct {
	Source string `json:"source"`
	Target string `json:"target"`
}

type CompiledWorkflowPlan struct {
	WorkflowID          string              `json:"workflowId"`
	Version             string              `json:"version"`
	DefinitionDigest    string              `json:"definitionDigest"`
	NodeOrder           []string            `json:"nodeOrder"`
	EntrypointNodeOrder map[string][]string `json:"entrypointNodeOrder"`
	Edges               []WorkflowEdge      `json:"edges"`
	PlanDigest          string              `json:"planDigest"`
}
