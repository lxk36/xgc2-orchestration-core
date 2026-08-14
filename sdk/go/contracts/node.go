package contracts

import "time"

type NodeExecutionMode string

const (
	NodePure      NodeExecutionMode = "pure"
	NodeEffectful NodeExecutionMode = "effectful"
	NodeWaiting   NodeExecutionMode = "waiting"
)

func (mode NodeExecutionMode) Valid() bool {
	return mode == NodePure || mode == NodeEffectful || mode == NodeWaiting
}

type NodeDeterminism string

const (
	NodeDeterministic NodeDeterminism = "deterministic"
	NodeRecorded      NodeDeterminism = "recorded"
)

func (determinism NodeDeterminism) Valid() bool {
	return determinism == NodeDeterministic || determinism == NodeRecorded
}

// NodeSchemaMode defines whether a descriptor owns one exact input/output
// schema pair or is a kernel-recognized control descriptor whose concrete
// result schema is frozen by each CallAction occurrence. Ordinary extension
// nodes always use the default exact mode.
type NodeSchemaMode string

const (
	NodeSchemaExact      NodeSchemaMode = "exact"
	NodeSchemaCallAction NodeSchemaMode = "call-action"
)

func (mode NodeSchemaMode) Valid() bool {
	return mode == "" || mode == NodeSchemaExact || mode == NodeSchemaCallAction
}

type CapabilityRequirement struct {
	CapabilityRef string `json:"capabilityRef"`
	Scope         string `json:"scope"`
	Optional      bool   `json:"optional,omitempty"`
}

type NodeDescriptor struct {
	SchemaVersion        string                  `json:"schemaVersion"`
	TypeRef              string                  `json:"typeRef"`
	DisplayName          string                  `json:"displayName"`
	PackageRef           string                  `json:"packageRef"`
	PackageDigest        string                  `json:"packageDigest"`
	DescriptorDigest     string                  `json:"descriptorDigest"`
	InputSchema          Schema                  `json:"inputSchema"`
	OutputSchema         Schema                  `json:"outputSchema"`
	Mode                 NodeExecutionMode       `json:"mode"`
	Determinism          NodeDeterminism         `json:"determinism"`
	SchemaMode           NodeSchemaMode          `json:"schemaMode,omitempty"`
	RequiredCapabilities []CapabilityRequirement `json:"requiredCapabilities,omitempty"`
	AllowedEffectKinds   []string                `json:"allowedEffectKinds,omitempty"`
	CompensationTypeRef  string                  `json:"compensationTypeRef,omitempty"`
	MaxInputBytes        int                     `json:"maxInputBytes"`
	MaxOutputBytes       int                     `json:"maxOutputBytes"`
}

type CapabilityGrant struct {
	CapabilityRef       string    `json:"capabilityRef"`
	Scope               string    `json:"scope"`
	HandleRef           string    `json:"handleRef"`
	AuthorizationDigest string    `json:"authorizationDigest"`
	ExpiresAt           time.Time `json:"expiresAt"`
}

type NodeInvocationRequest struct {
	SchemaVersion    string            `json:"schemaVersion"`
	InvocationID     string            `json:"invocationId"`
	RunID            string            `json:"runId"`
	NodeID           string            `json:"nodeId"`
	TypeRef          string            `json:"typeRef"`
	DescriptorDigest string            `json:"descriptorDigest"`
	AttemptID        string            `json:"attemptId"`
	AttemptOrdinal   uint32            `json:"attemptOrdinal"`
	Input            map[string]any    `json:"input"`
	InputDigest      string            `json:"inputDigest"`
	CapabilityGrants []CapabilityGrant `json:"capabilityGrants,omitempty"`
	Deadline         time.Time         `json:"deadline"`
	RequestedAt      time.Time         `json:"requestedAt"`
}

type EffectProposal struct {
	EffectKey              string             `json:"effectKey"`
	Kind                   string             `json:"kind"`
	TargetRef              string             `json:"targetRef"`
	IntentSchemaDigest     string             `json:"intentSchemaDigest"`
	Intent                 map[string]any     `json:"intent"`
	IntentDigest           string             `json:"intentDigest"`
	IntentArtifactRef      string             `json:"intentArtifactRef,omitempty"`
	Ownership              EffectOwnership    `json:"ownership"`
	CompensationPolicy     CompensationPolicy `json:"compensationPolicy"`
	RequiredCapabilityRefs []string           `json:"requiredCapabilityRefs,omitempty"`
	PolicyDigest           string             `json:"policyDigest"`
	Deadline               time.Time          `json:"deadline"`
}

type NodeWaitKind string

const (
	NodeWaitTimer    NodeWaitKind = "timer"
	NodeWaitEvent    NodeWaitKind = "event"
	NodeWaitApproval NodeWaitKind = "approval"
	NodeWaitEffect   NodeWaitKind = "effect"
)

func (kind NodeWaitKind) Valid() bool {
	switch kind {
	case NodeWaitTimer, NodeWaitEvent, NodeWaitApproval, NodeWaitEffect:
		return true
	default:
		return false
	}
}

type NodeWait struct {
	Kind            NodeWaitKind `json:"kind"`
	SubjectRef      string       `json:"subjectRef"`
	ConditionDigest string       `json:"conditionDigest"`
	ExpiresAt       *time.Time   `json:"expiresAt,omitempty"`
}

type NodeResultStatus string

const (
	NodeResultSucceeded NodeResultStatus = "succeeded"
	NodeResultWaiting   NodeResultStatus = "waiting"
	NodeResultFailed    NodeResultStatus = "failed"
)

func (status NodeResultStatus) Valid() bool {
	return status == NodeResultSucceeded || status == NodeResultWaiting || status == NodeResultFailed
}

type NodeResult struct {
	SchemaVersion     string             `json:"schemaVersion"`
	Status            NodeResultStatus   `json:"status"`
	Output            map[string]any     `json:"output,omitempty"`
	OutputDigest      string             `json:"outputDigest,omitempty"`
	OutputArtifactRef string             `json:"outputArtifactRef,omitempty"`
	Route             string             `json:"route,omitempty"`
	SourcePorts       []string           `json:"sourcePorts,omitempty"`
	Effects           []EffectProposal   `json:"effects,omitempty"`
	Wait              *NodeWait          `json:"wait,omitempty"`
	Failure           *StructuredFailure `json:"failure,omitempty"`
	EvidenceDigest    string             `json:"evidenceDigest"`
}

type NodeWaitResolutionStatus string

const (
	NodeWaitResolvedSucceeded NodeWaitResolutionStatus = "succeeded"
	NodeWaitResolvedFailed    NodeWaitResolutionStatus = "failed"
	NodeWaitResolvedCanceled  NodeWaitResolutionStatus = "canceled"
)

func (status NodeWaitResolutionStatus) Valid() bool {
	return status == NodeWaitResolvedSucceeded || status == NodeWaitResolvedFailed || status == NodeWaitResolvedCanceled
}

// NodeWaitResolution is public durable data. Provider credentials and raw
// external payloads are never exposed to a node resumer.
type NodeWaitResolution struct {
	Kind               NodeWaitKind             `json:"kind"`
	SubjectRef         string                   `json:"subjectRef"`
	ConditionDigest    string                   `json:"conditionDigest"`
	Status             NodeWaitResolutionStatus `json:"status"`
	Payload            map[string]any           `json:"payload,omitempty"`
	PayloadDigest      string                   `json:"payloadDigest,omitempty"`
	PayloadArtifactRef string                   `json:"payloadArtifactRef,omitempty"`
	Failure            *StructuredFailure       `json:"failure,omitempty"`
	ObservedAt         time.Time                `json:"observedAt"`
}

// NodeResumeRequest lets an optional pure Resumer fold a persisted wait
// resolution into a normal node result. It deliberately carries no grants,
// provider handles, store access, or mutable orchestration context.
type NodeResumeRequest struct {
	InvocationID     string             `json:"invocationId"`
	RunID            string             `json:"runId"`
	NodeID           string             `json:"nodeId"`
	TypeRef          string             `json:"typeRef"`
	DescriptorDigest string             `json:"descriptorDigest"`
	AttemptID        string             `json:"attemptId"`
	AttemptOrdinal   uint32             `json:"attemptOrdinal"`
	Input            map[string]any     `json:"input"`
	InputDigest      string             `json:"inputDigest"`
	Wait             NodeWait           `json:"wait"`
	Resolution       NodeWaitResolution `json:"resolution"`
	RequestedAt      time.Time          `json:"requestedAt"`
}
