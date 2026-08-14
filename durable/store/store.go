// Package store defines product-neutral durability ports for orchestration
// aggregate decisions, immutable events, durable intents, and inbox dedupe.
package store

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
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

const CommandScopeSchemaVersion = "xgc.command-scope/v3"

// CommandScope is the mandatory semantic partition for one durable command.
// HTTP and node callers never provide it directly: composition/controller
// code derives it from the authorized operation and frozen resource identity.
// AuthorityRef and AuthorityDigest are an inseparable pair used for sealed
// ingress domains (for example, a registered reserved-ingress policy).
type CommandScope struct {
	SchemaVersion   string `json:"schemaVersion"`
	Operation       string `json:"operation"`
	NamespaceID     string `json:"namespaceId"`
	ResourceType    string `json:"resourceType"`
	ResourceID      string `json:"resourceId"`
	AuthorityRef    string `json:"authorityRef,omitempty"`
	AuthorityDigest string `json:"authorityDigest,omitempty"`
}

func (scope CommandScope) Validate() error {
	if scope.SchemaVersion != CommandScopeSchemaVersion ||
		!contracts.ValidIdentifier(scope.Operation) || !contracts.ValidIdentifier(scope.NamespaceID) ||
		!contracts.ValidIdentifier(scope.ResourceType) || !contracts.ValidIdentifier(scope.ResourceID) {
		return errors.New("command scope identity is invalid")
	}
	if (scope.AuthorityRef == "") != (scope.AuthorityDigest == "") ||
		(scope.AuthorityRef != "" && (!contracts.ValidIdentifier(scope.AuthorityRef) || !contracts.ValidDigest(scope.AuthorityDigest))) {
		return errors.New("command scope authority is invalid")
	}
	return nil
}

// CommandIdentityKey is the bounded, collision-resistant persistent ledger
// key for a semantic scope and caller command ID. Its v3 schema marker makes
// this deliberately incompatible with the former global commandId ledger.
func CommandIdentityKey(scope CommandScope, commandID string) (string, error) {
	if err := scope.Validate(); err != nil {
		return "", err
	}
	if !contracts.ValidIdentifier(commandID) {
		return "", errors.New("command id is invalid")
	}
	payload, err := json.Marshal(struct {
		Scope     CommandScope `json:"scope"`
		CommandID string       `json:"commandId"`
	}{Scope: scope, CommandID: commandID})
	if err != nil {
		return "", fmt.Errorf("marshal command identity: %w", err)
	}
	digest := sha256.Sum256(payload)
	return "command-v3-" + hex.EncodeToString(digest[:]), nil
}

type Transaction struct {
	CommandScope   CommandScope            `json:"commandScope"`
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
