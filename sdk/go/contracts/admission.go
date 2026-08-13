package contracts

import "time"

// ActiveOwnerKey is the product-neutral, canonical identity of one
// single-active-run admission slot. Identity values are opaque product facts;
// the kernel canonicalizes the complete map, so values such as a branch are
// part of the fence instead of mutable lookup context.
type ActiveOwnerKey struct {
	NamespaceID string            `json:"namespaceId"`
	Kind        string            `json:"kind"`
	Identity    map[string]string `json:"identity"`
}

type ActiveRunOwnerState string

const (
	ActiveRunOwnerActive   ActiveRunOwnerState = "active"
	ActiveRunOwnerReleased ActiveRunOwnerState = "released"
)

func (state ActiveRunOwnerState) Valid() bool {
	return state == ActiveRunOwnerActive || state == ActiveRunOwnerReleased
}

const ActiveRunOwnerSchemaVersion = "xgc.active-run-owner/v1"

// ActiveRunOwner is the durable compare-and-swap fence for one ActiveOwnerKey.
// OwnerRef is stable across generations; RunID changes only on a new acquire
// after the prior generation has been released.
type ActiveRunOwner struct {
	SchemaVersion    string              `json:"schemaVersion"`
	OwnerRef         string              `json:"ownerRef"`
	KeyDigest        string              `json:"keyDigest"`
	Key              ActiveOwnerKey      `json:"key"`
	PolicyRef        string              `json:"policyRef"`
	PolicyDigest     string              `json:"policyDigest"`
	State            ActiveRunOwnerState `json:"state"`
	RunID            string              `json:"runId"`
	Generation       uint64              `json:"generation"`
	AcquiredAt       time.Time           `json:"acquiredAt"`
	ReleasedAt       *time.Time          `json:"releasedAt,omitempty"`
	TerminalStatus   RunStatus           `json:"terminalStatus,omitempty"`
	TerminalRevision uint64              `json:"terminalRevision,omitempty"`
	Revision         uint64              `json:"revision"`
}
