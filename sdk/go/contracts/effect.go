package contracts

import "time"

type EffectOwnership string

const (
	EffectOwned    EffectOwnership = "owned"
	EffectAttached EffectOwnership = "attached"
	EffectShared   EffectOwnership = "shared"
	// EffectDetached transfers lifecycle responsibility to an explicit
	// provider/product resource owner. The producing Run retains the immutable
	// Receipt and external identity but must not claim it will compensate the
	// long-lived resource during Run closure.
	EffectDetached EffectOwnership = "detached"
)

func (ownership EffectOwnership) Valid() bool {
	return ownership == EffectOwned || ownership == EffectAttached || ownership == EffectShared || ownership == EffectDetached
}

type CompensationPolicy string

const (
	CompensationNone       CompensationPolicy = "none"
	CompensationRequired   CompensationPolicy = "required"
	CompensationBestEffort CompensationPolicy = "best-effort"
)

func (policy CompensationPolicy) Valid() bool {
	return policy == CompensationNone || policy == CompensationRequired || policy == CompensationBestEffort
}

type EffectState string

const (
	EffectPrepared  EffectState = "prepared"
	EffectApplying  EffectState = "applying"
	EffectApplied   EffectState = "applied"
	EffectFailed    EffectState = "failed"
	EffectUncertain EffectState = "uncertain"
	// EffectCanceled is a prepared intent that was durably canceled before a
	// provider command entered the outbox. It is terminal and therefore cannot
	// be confused with an applying command whose outcome requires observation.
	EffectCanceled EffectState = "canceled"
)

func (state EffectState) Valid() bool {
	switch state {
	case EffectPrepared, EffectApplying, EffectApplied, EffectFailed, EffectUncertain, EffectCanceled:
		return true
	default:
		return false
	}
}

func (state EffectState) Terminal() bool {
	return state == EffectApplied || state == EffectFailed || state == EffectUncertain || state == EffectCanceled
}

type EffectCompensationState string

const (
	EffectCompensationNotRequired EffectCompensationState = "not-required"
	EffectCompensationUnscheduled EffectCompensationState = "unscheduled"
	EffectCompensationPending     EffectCompensationState = "pending"
	EffectCompensationRunning     EffectCompensationState = "running"
	EffectCompensationRetryWait   EffectCompensationState = "retry-wait"
	EffectCompensationSucceeded   EffectCompensationState = "succeeded"
	EffectCompensationFailed      EffectCompensationState = "failed"
	EffectCompensationCanceled    EffectCompensationState = "canceled"
)

func (state EffectCompensationState) Valid() bool {
	switch state {
	case EffectCompensationNotRequired, EffectCompensationUnscheduled, EffectCompensationPending,
		EffectCompensationRunning, EffectCompensationRetryWait, EffectCompensationSucceeded,
		EffectCompensationFailed, EffectCompensationCanceled:
		return true
	default:
		return false
	}
}

func (state EffectCompensationState) Terminal() bool {
	return state == EffectCompensationNotRequired || state == EffectCompensationSucceeded ||
		state == EffectCompensationFailed || state == EffectCompensationCanceled
}

type EffectIntent struct {
	EffectID               string             `json:"effectId"`
	NamespaceID            string             `json:"namespaceId"`
	RunID                  string             `json:"runId"`
	InvocationID           string             `json:"invocationId"`
	PreparedAttemptID      string             `json:"preparedAttemptId"`
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
	DescriptorDigest       string             `json:"descriptorDigest"`
	Deadline               time.Time          `json:"deadline"`
}

type EffectRecord struct {
	EffectID                 string                  `json:"effectId"`
	Intent                   EffectIntent            `json:"intent"`
	PreparationDigest        string                  `json:"preparationDigest"`
	State                    EffectState             `json:"state"`
	CommandID                string                  `json:"commandId"`
	CommandIdentityDigest    string                  `json:"commandIdentityDigest"`
	ExternalIdentity         string                  `json:"externalIdentity,omitempty"`
	ResultDigest             string                  `json:"resultDigest,omitempty"`
	ResultArtifactRef        string                  `json:"resultArtifactRef,omitempty"`
	PrimaryFailure           *StructuredFailure      `json:"primaryFailure,omitempty"`
	CompensationState        EffectCompensationState `json:"compensationState"`
	CompensationAttemptCount uint32                  `json:"compensationAttemptCount"`
	CompensationCommandID    string                  `json:"compensationCommandId,omitempty"`
	CompensationFailure      *StructuredFailure      `json:"compensationFailure,omitempty"`
	CompensationNextAt       *time.Time              `json:"compensationNextAttemptAt,omitempty"`
	PreparedAt               time.Time               `json:"preparedAt"`
	ApplyingAt               *time.Time              `json:"applyingAt,omitempty"`
	PrimaryTerminalAt        *time.Time              `json:"primaryTerminalAt,omitempty"`
	// PrimaryTerminalRevision pins the Effect revision that emitted its wait
	// resolution intent. Zero remains readable for older compatible v1 records.
	PrimaryTerminalRevision uint64     `json:"primaryTerminalRevision,omitempty"`
	CompensationStartedAt   *time.Time `json:"compensationStartedAt,omitempty"`
	CompensationFinishedAt  *time.Time `json:"compensationFinishedAt,omitempty"`
	UpdatedAt               time.Time  `json:"updatedAt"`
	Revision                uint64     `json:"revision"`
}

type FenceKind string

const (
	FenceRevision         FenceKind = "revision"
	FenceGeneration       FenceKind = "generation"
	FenceIdempotentCreate FenceKind = "idempotent-create"
)

type RevisionFence struct {
	TargetID         string `json:"targetId"`
	ExpectedRevision uint64 `json:"expectedRevision"`
}

type GenerationFence struct {
	BindingID    string `json:"bindingId"`
	Generation   uint64 `json:"generation"`
	FencingToken uint64 `json:"fencingToken"`
}

type IdempotentCreateFence struct {
	TargetNamespace string `json:"targetNamespace"`
	IdentityDigest  string `json:"identityDigest"`
}

// TargetFence is a closed union. Exactly one pointer must be set and Kind must
// agree with it.
type TargetFence struct {
	Kind             FenceKind              `json:"kind"`
	Revision         *RevisionFence         `json:"revision,omitempty"`
	Generation       *GenerationFence       `json:"generation,omitempty"`
	IdempotentCreate *IdempotentCreateFence `json:"idempotentCreate,omitempty"`
}

type CommandRisk string

const (
	RiskLow      CommandRisk = "low"
	RiskModerate CommandRisk = "moderate"
	RiskHigh     CommandRisk = "high"
)

func (risk CommandRisk) Valid() bool {
	return risk == RiskLow || risk == RiskModerate || risk == RiskHigh
}

type CommandEnvelope struct {
	CommandID              string      `json:"commandId"`
	RequestID              string      `json:"requestId,omitempty"`
	EffectID               string      `json:"effectId"`
	IdempotencyKey         string      `json:"-"`
	IdempotencyKeyHash     string      `json:"idempotencyKeyHash"`
	IdentityDigest         string      `json:"identityDigest"`
	NamespaceID            string      `json:"namespaceId"`
	TargetRef              string      `json:"targetRef"`
	Action                 string      `json:"action"`
	ActorRef               string      `json:"actorRef"`
	SourceRef              string      `json:"sourceRef"`
	ReasonCode             string      `json:"reasonCode"`
	Reason                 string      `json:"reason,omitempty"`
	Risk                   CommandRisk `json:"risk"`
	Fence                  TargetFence `json:"fence"`
	PayloadDigest          string      `json:"payloadDigest"`
	PayloadArtifactRef     string      `json:"payloadArtifactRef,omitempty"`
	PolicyDigest           string      `json:"policyDigest"`
	DescriptorDigest       string      `json:"descriptorDigest"`
	ManifestDigest         string      `json:"manifestDigest,omitempty"`
	Deadline               time.Time   `json:"deadline"`
	CancellationID         string      `json:"cancellationId"`
	RequiredCapabilityRefs []string    `json:"requiredCapabilityRefs,omitempty"`
	CapabilityTokenHash    string      `json:"capabilityTokenHash,omitempty"`
	CapabilityToken        string      `json:"-"`
}

type ReceiptStatus string

const (
	ReceiptAccepted  ReceiptStatus = "accepted"
	ReceiptRejected  ReceiptStatus = "rejected"
	ReceiptSucceeded ReceiptStatus = "succeeded"
	ReceiptFailed    ReceiptStatus = "failed"
	ReceiptUncertain ReceiptStatus = "uncertain"
)

func (status ReceiptStatus) Valid() bool {
	switch status {
	case ReceiptAccepted, ReceiptRejected, ReceiptSucceeded, ReceiptFailed, ReceiptUncertain:
		return true
	default:
		return false
	}
}

func (status ReceiptStatus) Terminal() bool {
	return status == ReceiptRejected || status == ReceiptSucceeded || status == ReceiptFailed || status == ReceiptUncertain
}

type CommandReceipt struct {
	ReceiptID           string             `json:"receiptId"`
	CommandID           string             `json:"commandId"`
	Sequence            uint32             `json:"sequence"`
	Status              ReceiptStatus      `json:"status"`
	IdentityDigest      string             `json:"identityDigest"`
	FenceDigest         string             `json:"fenceDigest"`
	ProviderRef         string             `json:"providerRef"`
	ProviderDigest      string             `json:"providerDigest"`
	PolicyDigest        string             `json:"policyDigest"`
	AuthorizationDigest string             `json:"authorizationDigest"`
	ResultDigest        string             `json:"resultDigest,omitempty"`
	ResultArtifactRef   string             `json:"resultArtifactRef,omitempty"`
	ExternalIdentity    string             `json:"externalIdentity,omitempty"`
	Failure             *StructuredFailure `json:"failure,omitempty"`
	ObservedAt          time.Time          `json:"observedAt"`
}

type CommandLedger struct {
	Envelope CommandEnvelope  `json:"envelope"`
	Receipts []CommandReceipt `json:"receipts"`
}
