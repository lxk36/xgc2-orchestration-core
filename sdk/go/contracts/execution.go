package contracts

import "time"

type FailureClass string

const (
	FailureTransient FailureClass = "transient"
	FailurePermanent FailureClass = "permanent"
	FailureCanceled  FailureClass = "canceled"
	FailureUncertain FailureClass = "uncertain"
)

func (class FailureClass) Valid() bool {
	switch class {
	case FailureTransient, FailurePermanent, FailureCanceled, FailureUncertain:
		return true
	default:
		return false
	}
}

type StructuredFailure struct {
	Class       FailureClass `json:"class"`
	Code        string       `json:"code"`
	Message     string       `json:"message"`
	EvidenceRef string       `json:"evidenceRef,omitempty"`
}

type RunStatus string

const (
	RunAccepted  RunStatus = "accepted"
	RunQueued    RunStatus = "queued"
	RunRunning   RunStatus = "running"
	RunWaiting   RunStatus = "waiting"
	RunStopping  RunStatus = "stopping"
	RunSucceeded RunStatus = "succeeded"
	RunFailed    RunStatus = "failed"
	RunCanceled  RunStatus = "canceled"
	RunStopped   RunStatus = "stopped"
	RunRejected  RunStatus = "rejected"
)

func (status RunStatus) Valid() bool {
	switch status {
	case RunAccepted, RunQueued, RunRunning, RunWaiting, RunStopping,
		RunSucceeded, RunFailed, RunCanceled, RunStopped, RunRejected:
		return true
	default:
		return false
	}
}

func (status RunStatus) Terminal() bool {
	switch status {
	case RunSucceeded, RunFailed, RunCanceled, RunStopped, RunRejected:
		return true
	default:
		return false
	}
}

type TerminationKind string

const (
	TerminationCompleted TerminationKind = "completed"
	TerminationFailed    TerminationKind = "failed"
	TerminationCanceled  TerminationKind = "canceled"
	TerminationStopped   TerminationKind = "stopped"
	TerminationRejected  TerminationKind = "rejected"
)

func (kind TerminationKind) Valid() bool {
	switch kind {
	case TerminationCompleted, TerminationFailed, TerminationCanceled, TerminationStopped, TerminationRejected:
		return true
	default:
		return false
	}
}

func (kind TerminationKind) RequiresStopping() bool {
	return kind == TerminationFailed || kind == TerminationCanceled || kind == TerminationStopped
}

type TerminationIntent struct {
	Kind           TerminationKind    `json:"kind"`
	RequestedBy    string             `json:"requestedBy"`
	ReasonCode     string             `json:"reasonCode"`
	Reason         string             `json:"reason,omitempty"`
	PrimaryFailure *StructuredFailure `json:"primaryFailure,omitempty"`
	CommandID      string             `json:"commandId"`
	RequestedAt    time.Time          `json:"requestedAt"`
}

type ParentRunLink struct {
	ParentRunID        string `json:"parentRunId"`
	ParentInvocationID string `json:"parentInvocationId"`
	CallNodeID         string `json:"callNodeId"`
	MappingDigest      string `json:"mappingDigest"`
}

type Run struct {
	RunID                 string              `json:"runId"`
	NamespaceID           string              `json:"namespaceId"`
	ActionRef             ActionRef           `json:"actionRef"`
	ExecutionPlanRef      string              `json:"executionPlanRef"`
	PlanDigest            string              `json:"planDigest"`
	TriggerRef            string              `json:"triggerRef"`
	TriggerDigest         string              `json:"triggerDigest"`
	InputDigest           string              `json:"inputDigest"`
	ProvenanceArtifactRef string              `json:"provenanceArtifactRef,omitempty"`
	ScopeRef              string              `json:"scopeRef,omitempty"`
	Parent                *ParentRunLink      `json:"parent,omitempty"`
	RootRunID             string              `json:"rootRunId"`
	ActorRef              string              `json:"actorRef"`
	SourceRef             string              `json:"sourceRef"`
	CorrelationRef        string              `json:"correlationRef,omitempty"`
	Status                RunStatus           `json:"status"`
	Termination           *TerminationIntent  `json:"termination,omitempty"`
	TerminationKind       TerminationKind     `json:"terminationKind,omitempty"`
	ResultRef             string              `json:"resultRef,omitempty"`
	PrimaryFailure        *StructuredFailure  `json:"primaryFailure,omitempty"`
	CleanupFailures       []StructuredFailure `json:"cleanupFailures,omitempty"`
	AcceptedAt            time.Time           `json:"acceptedAt"`
	StartedAt             *time.Time          `json:"startedAt,omitempty"`
	UpdatedAt             time.Time           `json:"updatedAt"`
	FinishedAt            *time.Time          `json:"finishedAt,omitempty"`
	Revision              uint64              `json:"revision"`
}

type RunClosureFacts struct {
	RunRevision                 uint64 `json:"runRevision"`
	OwnershipGraphRevision      uint64 `json:"ownershipGraphRevision"`
	ActiveInvocationCount       uint64 `json:"activeInvocationCount"`
	LiveAttemptCount            uint64 `json:"liveAttemptCount"`
	OpenWaitCount               uint64 `json:"openWaitCount"`
	OpenChildCount              uint64 `json:"openChildCount"`
	OpenEffectCount             uint64 `json:"openEffectCount"`
	OpenEffectCompensationCount uint64 `json:"openEffectCompensationCount"`
	OpenOwnedRuntimeCount       uint64 `json:"openOwnedRuntimeBindingCount"`
	OpenOwnedResourceCount      uint64 `json:"openOwnedResourceLeaseCount"`
}

func (facts RunClosureFacts) Satisfied() bool {
	return facts.ActiveInvocationCount == 0 && facts.LiveAttemptCount == 0 && facts.OpenWaitCount == 0 &&
		facts.OpenChildCount == 0 && facts.OpenEffectCount == 0 && facts.OpenEffectCompensationCount == 0 &&
		facts.OpenOwnedRuntimeCount == 0 && facts.OpenOwnedResourceCount == 0
}

type InvocationStatus string

const (
	InvocationReady     InvocationStatus = "ready"
	InvocationRunning   InvocationStatus = "running"
	InvocationWaiting   InvocationStatus = "waiting"
	InvocationRetryWait InvocationStatus = "retry-wait"
	InvocationSucceeded InvocationStatus = "succeeded"
	InvocationFailed    InvocationStatus = "failed"
	InvocationCanceled  InvocationStatus = "canceled"
	InvocationSkipped   InvocationStatus = "skipped"
)

func (status InvocationStatus) Valid() bool {
	switch status {
	case InvocationReady, InvocationRunning, InvocationWaiting, InvocationRetryWait,
		InvocationSucceeded, InvocationFailed, InvocationCanceled, InvocationSkipped:
		return true
	default:
		return false
	}
}

func (status InvocationStatus) Terminal() bool {
	switch status {
	case InvocationSucceeded, InvocationFailed, InvocationCanceled, InvocationSkipped:
		return true
	default:
		return false
	}
}

type CompensationStatus string

const (
	CompensationNotRequired CompensationStatus = "not-required"
	CompensationUnscheduled CompensationStatus = "unscheduled"
	CompensationReady       CompensationStatus = "ready"
	CompensationRunning     CompensationStatus = "running"
	CompensationRetryWait   CompensationStatus = "retry-wait"
	CompensationSucceeded   CompensationStatus = "succeeded"
	CompensationFailed      CompensationStatus = "failed"
	CompensationCanceled    CompensationStatus = "canceled"
)

func (status CompensationStatus) Valid() bool {
	switch status {
	case CompensationNotRequired, CompensationUnscheduled, CompensationReady, CompensationRunning,
		CompensationRetryWait, CompensationSucceeded, CompensationFailed, CompensationCanceled:
		return true
	default:
		return false
	}
}

func (status CompensationStatus) Terminal() bool {
	return status == CompensationNotRequired || status == CompensationSucceeded ||
		status == CompensationFailed || status == CompensationCanceled
}

type Invocation struct {
	InvocationID                string             `json:"invocationId"`
	NamespaceID                 string             `json:"namespaceId"`
	RunID                       string             `json:"runId"`
	NodeID                      string             `json:"nodeId"`
	TypeRef                     string             `json:"typeRef"`
	DescriptorDigest            string             `json:"descriptorDigest"`
	ResolvedInputDigest         string             `json:"resolvedInputDigest"`
	InputRefsDigest             string             `json:"inputRefsDigest"`
	Status                      InvocationStatus   `json:"status"`
	ActiveAttemptID             string             `json:"activeAttemptId,omitempty"`
	ActiveCompensationAttemptID string             `json:"activeCompensationAttemptId,omitempty"`
	ExecutionAttemptCount       uint32             `json:"executionAttemptCount"`
	WaitGeneration              uint32             `json:"waitGeneration,omitempty"`
	CurrentWaitRef              string             `json:"currentWaitRef,omitempty"`
	NextAttemptAt               *time.Time         `json:"nextAttemptAt,omitempty"`
	OutputRefsDigest            string             `json:"outputRefsDigest,omitempty"`
	PrimaryFailure              *StructuredFailure `json:"primaryFailure,omitempty"`
	CompensationStatus          CompensationStatus `json:"compensationStatus"`
	CompensationAttemptCount    uint32             `json:"compensationAttemptCount"`
	CompensationFailure         *StructuredFailure `json:"compensationFailure,omitempty"`
	CompensationNextAt          *time.Time         `json:"compensationNextAttemptAt,omitempty"`
	CreatedAt                   time.Time          `json:"createdAt"`
	StartedAt                   *time.Time         `json:"startedAt,omitempty"`
	UpdatedAt                   time.Time          `json:"updatedAt"`
	FinishedAt                  *time.Time         `json:"finishedAt,omitempty"`
	Revision                    uint64             `json:"revision"`
}

type AttemptPhase string

const (
	AttemptExecution    AttemptPhase = "execution"
	AttemptCompensation AttemptPhase = "compensation"
)

func (phase AttemptPhase) Valid() bool {
	return phase == AttemptExecution || phase == AttemptCompensation
}

type AttemptStatus string

const (
	AttemptRunning   AttemptStatus = "running"
	AttemptWaiting   AttemptStatus = "waiting"
	AttemptSucceeded AttemptStatus = "succeeded"
	AttemptFailed    AttemptStatus = "failed"
	AttemptCanceled  AttemptStatus = "canceled"
	AttemptAbandoned AttemptStatus = "abandoned"
)

func (status AttemptStatus) Valid() bool {
	switch status {
	case AttemptRunning, AttemptWaiting, AttemptSucceeded, AttemptFailed, AttemptCanceled, AttemptAbandoned:
		return true
	default:
		return false
	}
}

func (status AttemptStatus) Terminal() bool {
	return status == AttemptSucceeded || status == AttemptFailed || status == AttemptCanceled || status == AttemptAbandoned
}

type Attempt struct {
	AttemptID           string             `json:"attemptId"`
	NamespaceID         string             `json:"namespaceId"`
	RunID               string             `json:"runId"`
	InvocationID        string             `json:"invocationId"`
	Phase               AttemptPhase       `json:"phase"`
	Ordinal             uint32             `json:"ordinal"`
	Status              AttemptStatus      `json:"status"`
	ResolvedInputDigest string             `json:"resolvedInputDigest"`
	OwnerRef            string             `json:"ownerRef"`
	LeaseTokenHash      string             `json:"leaseTokenHash"`
	LeaseExpiresAt      time.Time          `json:"leaseExpiresAt"`
	AdoptionCount       uint32             `json:"adoptionCount"`
	CheckpointRef       string             `json:"checkpointRef,omitempty"`
	CheckpointDigest    string             `json:"checkpointDigest,omitempty"`
	Failure             *StructuredFailure `json:"failure,omitempty"`
	CreatedAt           time.Time          `json:"createdAt"`
	StartedAt           time.Time          `json:"startedAt"`
	UpdatedAt           time.Time          `json:"updatedAt"`
	FinishedAt          *time.Time         `json:"finishedAt,omitempty"`
	Revision            uint64             `json:"revision"`
}

type InvocationLedger struct {
	Invocation Invocation `json:"invocation"`
	Attempts   []Attempt  `json:"attempts"`
}

type DomainEvent struct {
	EventID             string         `json:"eventId"`
	AggregateType       string         `json:"aggregateType"`
	AggregateID         string         `json:"aggregateId"`
	AggregateRevision   uint64         `json:"aggregateRevision"`
	Type                string         `json:"type"`
	CommandID           string         `json:"commandId"`
	CausationID         string         `json:"causationId,omitempty"`
	CorrelationID       string         `json:"correlationId,omitempty"`
	PayloadSchemaDigest string         `json:"payloadSchemaDigest"`
	PayloadDigest       string         `json:"payloadDigest"`
	Payload             map[string]any `json:"payload"`
	OccurredAt          time.Time      `json:"occurredAt"`
}

type DurableIntentKind string

const (
	IntentOutbox         DurableIntentKind = "outbox"
	IntentReconcile      DurableIntentKind = "reconcile"
	IntentCleanup        DurableIntentKind = "cleanup"
	IntentWaitResolution DurableIntentKind = "wait-resolution"
)

type DurableIntent struct {
	Kind          DurableIntentKind `json:"kind"`
	Identity      string            `json:"identity"`
	AggregateID   string            `json:"aggregateId"`
	PayloadDigest string            `json:"payloadDigest"`
	Payload       map[string]any    `json:"payload"`
}
