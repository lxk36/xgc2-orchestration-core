package contracts

import "time"

type TriggerKind string

const (
	TriggerManual         TriggerKind = "trigger.manual/v1"
	TriggerSchedule       TriggerKind = "trigger.schedule/v1"
	TriggerWebhook        TriggerKind = "trigger.webhook/v1"
	TriggerAPI            TriggerKind = "trigger.api/v1"
	TriggerPanel          TriggerKind = "trigger.panel/v1"
	TriggerProductBuilder TriggerKind = "trigger.product-builder/v1"
	TriggerActionCall     TriggerKind = "trigger.action-call/v1"
)

func (kind TriggerKind) Valid() bool {
	switch kind {
	case TriggerManual, TriggerSchedule, TriggerWebhook, TriggerAPI, TriggerPanel,
		TriggerProductBuilder, TriggerActionCall:
		return true
	default:
		return false
	}
}

type ActionRef struct {
	ActionID string `json:"actionId"`
	Version  string `json:"version"`
	Digest   string `json:"digest"`
}

func (ref ActionRef) Equal(other ActionRef) bool {
	return ref.ActionID == other.ActionID && ref.Version == other.Version && ref.Digest == other.Digest
}

type ActionVersion struct {
	ActionID             string        `json:"actionId"`
	Version              string        `json:"version"`
	DefinitionDigest     string        `json:"definitionDigest"`
	Entrypoint           string        `json:"entrypoint"`
	InputSchema          Schema        `json:"inputSchema"`
	ResultSchema         Schema        `json:"resultSchema"`
	AcceptedTriggerKinds []TriggerKind `json:"acceptedTriggerKinds"`
	RequiredCapabilities []string      `json:"requiredCapabilities,omitempty"`
	InputSizeLimit       int           `json:"inputSizeLimit,omitempty"`
}

func (version ActionVersion) Ref() ActionRef {
	return ActionRef{ActionID: version.ActionID, Version: version.Version, Digest: version.DefinitionDigest}
}

type TriggerEvent struct {
	EventID                string         `json:"eventId"`
	Kind                   TriggerKind    `json:"kind"`
	Version                string         `json:"version"`
	OccurredAt             time.Time      `json:"occurredAt"`
	ReceivedAt             time.Time      `json:"receivedAt"`
	SourceRef              string         `json:"sourceRef"`
	ActorRef               string         `json:"actorRef"`
	SubjectRef             string         `json:"subjectRef,omitempty"`
	PayloadSchemaDigest    string         `json:"payloadSchemaDigest"`
	Payload                map[string]any `json:"payload"`
	RawArtifactRef         string         `json:"rawArtifactRef,omitempty"`
	VerificationReceiptRef string         `json:"verificationReceiptRef,omitempty"`
}

type ActionPresetVersion struct {
	PresetID         string         `json:"presetId"`
	Version          string         `json:"version"`
	Digest           string         `json:"digest"`
	ActionRef        ActionRef      `json:"actionRef"`
	Values           map[string]any `json:"values"`
	OverridablePaths []string       `json:"overridablePaths,omitempty"`
	PolicyRef        string         `json:"policyRef,omitempty"`
}

type InputOriginKind string

const (
	OriginSchemaDefault  InputOriginKind = "schema-default"
	OriginPreset         InputOriginKind = "preset"
	OriginCaller         InputOriginKind = "caller"
	OriginTriggerMap     InputOriginKind = "trigger-map"
	OriginParentMap      InputOriginKind = "parent-map"
	OriginProductBuilder InputOriginKind = "product-builder"
)

type InputFieldProvenance struct {
	TargetPointer string          `json:"targetPointer"`
	OriginKind    InputOriginKind `json:"originKind"`
	SourceRef     string          `json:"sourceRef"`
	SourcePointer string          `json:"sourcePointer,omitempty"`
	SourceDigest  string          `json:"sourceDigest,omitempty"`
	MappingDigest string          `json:"mappingDigest,omitempty"`
}
