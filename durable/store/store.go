// Package store defines product-neutral durability ports for orchestration
// aggregate decisions, immutable events, durable intents, and inbox dedupe.
package store

import (
	"context"
	"encoding/json"
	"errors"
	"time"

	"github.com/lxk36/xgc2-orchestration-core/sdk/go/contracts"
)

var (
	ErrNotFound         = errors.New("durable record not found")
	ErrRevisionConflict = errors.New("durable aggregate revision conflict")
	ErrIdentityConflict = errors.New("durable identity conflict")
	ErrLeaseConflict    = errors.New("durable intent lease conflict")
	ErrCorrupt          = errors.New("durable store is corrupt")
	ErrLocked           = errors.New("durable store is already open")
)

type AggregateKey struct {
	Type string `json:"type"`
	ID   string `json:"id"`
}

type ExpectedRevision struct {
	Key      AggregateKey `json:"key"`
	Revision uint64       `json:"revision"`
}

// AggregateRecord is an opaque canonical snapshot. Domain validators own its
// type-specific meaning; the store owns identity, revision, and digest CAS.
type AggregateRecord struct {
	Key           AggregateKey    `json:"key"`
	Revision      uint64          `json:"revision"`
	PayloadDigest string          `json:"payloadDigest"`
	Payload       json.RawMessage `json:"payload"`
}

type Transaction struct {
	CommandID      string                  `json:"commandId"`
	IdentityDigest string                  `json:"identityDigest"`
	Expected       []ExpectedRevision      `json:"expected"`
	Mutations      []AggregateRecord       `json:"mutations"`
	Events         []contracts.DomainEvent `json:"events"`
	Intents        []IntentSeed            `json:"intents"`
	Outcome        json.RawMessage         `json:"outcome"`
	At             time.Time               `json:"at"`
}

type CommitResult struct {
	Replay        bool            `json:"replay"`
	OutcomeDigest string          `json:"outcomeDigest"`
	Outcome       json.RawMessage `json:"outcome"`
}

type IntentSeed struct {
	Intent      contracts.DurableIntent `json:"intent"`
	AvailableAt time.Time               `json:"availableAt"`
}

type IntentStatus string

const (
	IntentPending   IntentStatus = "pending"
	IntentLeased    IntentStatus = "leased"
	IntentCompleted IntentStatus = "completed"
	IntentDead      IntentStatus = "dead"
)

type IntentRecord struct {
	Intent         contracts.DurableIntent      `json:"intent"`
	Status         IntentStatus                 `json:"status"`
	AvailableAt    time.Time                    `json:"availableAt"`
	AttemptCount   uint32                       `json:"attemptCount"`
	LeaseOwner     string                       `json:"leaseOwner,omitempty"`
	LeaseTokenHash string                       `json:"leaseTokenHash,omitempty"`
	LeaseExpiresAt *time.Time                   `json:"leaseExpiresAt,omitempty"`
	LastFailure    *contracts.StructuredFailure `json:"lastFailure,omitempty"`
	CompletedAt    *time.Time                   `json:"completedAt,omitempty"`
	Revision       uint64                       `json:"revision"`
}

type ClaimRequest struct {
	Kinds          []contracts.DurableIntentKind
	OwnerRef       string
	LeaseToken     string
	Now            time.Time
	LeaseExpiresAt time.Time
	Limit          int
}

type ClaimedIntent struct {
	Record     IntentRecord `json:"record"`
	LeaseToken string       `json:"-"`
}

type IntentFence struct {
	IntentID         string
	ExpectedRevision uint64
	OwnerRef         string
	LeaseToken       string
	At               time.Time
}

type IntentFailure struct {
	Fence       IntentFence
	Failure     contracts.StructuredFailure
	AvailableAt *time.Time
	Dead        bool
}

type InboxRecord struct {
	SourceRef     string    `json:"sourceRef"`
	MessageID     string    `json:"messageId"`
	PayloadDigest string    `json:"payloadDigest"`
	ObservedAt    time.Time `json:"observedAt"`
}

type Store interface {
	Commit(context.Context, Transaction) (CommitResult, error)
	GetAggregate(context.Context, AggregateKey) (AggregateRecord, error)
	ListAggregates(context.Context, string, string, int) ([]AggregateRecord, error)
	EventsAfter(context.Context, AggregateKey, uint64, int) ([]contracts.DomainEvent, error)
	ClaimIntents(context.Context, ClaimRequest) ([]ClaimedIntent, error)
	CompleteIntent(context.Context, IntentFence) (IntentRecord, error)
	FailIntent(context.Context, IntentFailure) (IntentRecord, error)
	GetIntent(context.Context, string) (IntentRecord, error)
	RecordInbox(context.Context, InboxRecord) (bool, error)
	Close() error
}
