package contracts

type EdgeKind string

const (
	EdgeControl EdgeKind = "control"
	EdgeData    EdgeKind = "data"
)

func (kind EdgeKind) Valid() bool {
	return kind == EdgeControl || kind == EdgeData
}

type WorkflowDefinition struct {
	SchemaVersion string                   `json:"schemaVersion"`
	WorkflowID    string                   `json:"workflowId"`
	Version       string                   `json:"version"`
	InputSchema   Schema                   `json:"inputSchema"`
	ResultSchema  Schema                   `json:"resultSchema"`
	TriggerSchema Schema                   `json:"triggerSchema"`
	ScopeSchema   Schema                   `json:"scopeSchema"`
	Entrypoints   map[string]string        `json:"entrypoints"`
	Nodes         []WorkflowNodeDefinition `json:"nodes"`
	Edges         []WorkflowEdge           `json:"edges"`
}

type WorkflowNodeDefinition struct {
	NodeID       string         `json:"nodeId"`
	TypeRef      string         `json:"typeRef"`
	InputSchema  Schema         `json:"inputSchema"`
	OutputSchema Schema         `json:"outputSchema"`
	FixedInputs  map[string]any `json:"fixedInputs,omitempty"`
	Bindings     []ValueBinding `json:"bindings,omitempty"`
	CallAction   *CallAction    `json:"callAction,omitempty"`
}

type WorkflowEdge struct {
	From string   `json:"from"`
	To   string   `json:"to"`
	Kind EdgeKind `json:"kind"`
}

type CallAction struct {
	TargetActionRef ActionRef      `json:"targetActionRef"`
	InputSchema     Schema         `json:"inputSchema"`
	ResultSchema    Schema         `json:"resultSchema"`
	InputMap        []ValueBinding `json:"inputMap"`
}

type CompiledWorkflowPlan struct {
	WorkflowID       string   `json:"workflowId"`
	Version          string   `json:"version"`
	DefinitionDigest string   `json:"definitionDigest"`
	NodeOrder        []string `json:"nodeOrder"`
	PlanDigest       string   `json:"planDigest"`
}
