// Package actioncatalog persists exact Action versions and their immutable
// Workflow definitions. It contains no authoring head, branch, alias, or
// fallback lookup: callers must resolve the complete ActionRef they froze into
// a parent Workflow.
package actioncatalog

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"time"

	"github.com/lxk36/xgc2-orchestration-core/durable/store"
	"github.com/lxk36/xgc2-orchestration-core/kernel/action"
	"github.com/lxk36/xgc2-orchestration-core/kernel/canonicaljson"
	"github.com/lxk36/xgc2-orchestration-core/kernel/execution"
	workflowkernel "github.com/lxk36/xgc2-orchestration-core/kernel/workflow"
	"github.com/lxk36/xgc2-orchestration-core/sdk/go/contracts"
)

const aggregateType = "action-version"

const eventSchemaDigest = "sha256:c0fd20e0354dd7882efd2366be0515388cd38386974808fb053147173ba7f86b"

var ErrActionConflict = errors.New("exact Action catalog identity conflict")

type Catalog struct{ store store.Store }

type Record struct {
	NamespaceID string                       `json:"namespaceId"`
	Action      contracts.ActionVersion      `json:"action"`
	Definition  contracts.WorkflowDefinition `json:"definition"`
	InstalledAt time.Time                    `json:"installedAt"`
	Revision    uint64                       `json:"revision"`
}

type InstallRequest struct {
	NamespaceID string
	Action      contracts.ActionVersion
	Definition  contracts.WorkflowDefinition
	CommandID   string
	At          time.Time
}

type InstallResult struct {
	Record Record `json:"record"`
	Replay bool   `json:"replay"`
}

func New(durable store.Store) (*Catalog, error) {
	if durable == nil {
		return nil, errors.New("Action catalog durable store is required")
	}
	return &Catalog{store: durable}, nil
}

func (catalog *Catalog) Install(ctx context.Context, request InstallRequest) (InstallResult, error) {
	if catalog == nil || catalog.store == nil || ctx == nil {
		return InstallResult{}, errors.New("Action catalog is unavailable")
	}
	if !contracts.ValidIdentifier(request.NamespaceID) || !contracts.ValidIdentifier(request.CommandID) || request.At.IsZero() {
		return InstallResult{}, errors.New("Action catalog namespace, command, and time are required")
	}
	if err := validateExactAction(request.Action, request.Definition); err != nil {
		return InstallResult{}, err
	}
	key, err := recordKey(request.NamespaceID, request.Action.Ref())
	if err != nil {
		return InstallResult{}, err
	}
	record := Record{
		NamespaceID: request.NamespaceID, Action: request.Action, Definition: request.Definition,
		InstalledAt: request.At.UTC(), Revision: 1,
	}
	payload, err := canonicaljson.Marshal(record)
	if err != nil {
		return InstallResult{}, err
	}
	payloadDigest, err := canonicaljson.Digest(payload)
	if err != nil {
		return InstallResult{}, err
	}
	if existing, getErr := catalog.store.GetAggregate(ctx, key); getErr == nil {
		current, decodeErr := decodeRecord(existing, request.NamespaceID, request.Action.Ref())
		if decodeErr != nil {
			return InstallResult{}, decodeErr
		}
		currentDigest, digestErr := canonicaljson.DigestValue(struct {
			NamespaceID string                       `json:"namespaceId"`
			Action      contracts.ActionVersion      `json:"action"`
			Definition  contracts.WorkflowDefinition `json:"definition"`
		}{current.NamespaceID, current.Action, current.Definition})
		requestedDigest, requestDigestErr := canonicaljson.DigestValue(struct {
			NamespaceID string                       `json:"namespaceId"`
			Action      contracts.ActionVersion      `json:"action"`
			Definition  contracts.WorkflowDefinition `json:"definition"`
		}{record.NamespaceID, record.Action, record.Definition})
		if digestErr != nil || requestDigestErr != nil || currentDigest != requestedDigest {
			return InstallResult{}, ErrActionConflict
		}
		return InstallResult{Record: current, Replay: true}, nil
	} else if !errors.Is(getErr, store.ErrNotFound) {
		return InstallResult{}, getErr
	}
	outcome, err := canonicaljson.Marshal(InstallResult{Record: record})
	if err != nil {
		return InstallResult{}, err
	}
	identityDigest, err := canonicaljson.DigestValue(map[string]any{
		"operation": "action.install", "namespaceId": request.NamespaceID,
		"actionRef": request.Action.Ref(), "payloadDigest": payloadDigest,
	})
	if err != nil {
		return InstallResult{}, err
	}
	eventPayload := map[string]any{
		"namespaceId": request.NamespaceID, "actionRef": request.Action.Ref(),
		"definitionDigest": request.Action.DefinitionDigest,
	}
	eventPayloadDigest, err := canonicaljson.DigestValue(eventPayload)
	if err != nil {
		return InstallResult{}, err
	}
	eventID, err := execution.StableEventID(key.Type, key.ID, 1)
	if err != nil {
		return InstallResult{}, err
	}
	event := contracts.DomainEvent{
		EventID: eventID, AggregateType: key.Type, AggregateID: key.ID, AggregateRevision: 1,
		Type: "action-version.installed", CommandID: request.CommandID,
		PayloadSchemaDigest: eventSchemaDigest, PayloadDigest: eventPayloadDigest,
		Payload: eventPayload, OccurredAt: request.At.UTC(),
	}
	committed, err := catalog.store.Commit(ctx, store.Transaction{
		CommandID: request.CommandID, IdentityDigest: identityDigest,
		Expected: []store.ExpectedRevision{{Key: key, Revision: 0}},
		Mutations: []store.AggregateRecord{{
			Key: key, Revision: 1, PayloadDigest: payloadDigest, Payload: payload,
		}},
		Events: []contracts.DomainEvent{event}, Outcome: outcome, At: request.At.UTC(),
	})
	if err != nil {
		if errors.Is(err, store.ErrRevisionConflict) {
			return InstallResult{}, ErrActionConflict
		}
		return InstallResult{}, err
	}
	result := InstallResult{Record: record, Replay: committed.Replay}
	if committed.Replay {
		if err := canonicaljson.UnmarshalStrict(committed.Outcome, &result); err != nil {
			return InstallResult{}, err
		}
		result.Replay = true
	}
	return result, nil
}

// ResolveAction implements controller.ActionResolver.
func (catalog *Catalog) ResolveAction(ctx context.Context, namespaceID string, ref contracts.ActionRef) (contracts.ActionVersion, contracts.WorkflowDefinition, error) {
	if catalog == nil || catalog.store == nil || ctx == nil {
		return contracts.ActionVersion{}, contracts.WorkflowDefinition{}, errors.New("Action catalog is unavailable")
	}
	key, err := recordKey(namespaceID, ref)
	if err != nil {
		return contracts.ActionVersion{}, contracts.WorkflowDefinition{}, err
	}
	stored, err := catalog.store.GetAggregate(ctx, key)
	if err != nil {
		return contracts.ActionVersion{}, contracts.WorkflowDefinition{}, err
	}
	record, err := decodeRecord(stored, namespaceID, ref)
	if err != nil {
		return contracts.ActionVersion{}, contracts.WorkflowDefinition{}, err
	}
	return record.Action, record.Definition, nil
}

func validateExactAction(version contracts.ActionVersion, definition contracts.WorkflowDefinition) error {
	if err := action.ValidateVersion(version); err != nil {
		return fmt.Errorf("Action version: %w", err)
	}
	plan, err := workflowkernel.Compile(definition)
	if err != nil {
		return fmt.Errorf("Action Workflow: %w", err)
	}
	if version.ActionID != definition.WorkflowID || version.Version != definition.Version ||
		version.DefinitionDigest != plan.DefinitionDigest {
		return errors.New("Action does not pin its exact compiled Workflow")
	}
	if _, exists := definition.Entrypoints[version.Entrypoint]; !exists {
		return errors.New("Action entrypoint is absent from its Workflow")
	}
	inputDigest, inputErr := canonicaljson.DigestValue(version.InputSchema)
	definitionInputDigest, definitionInputErr := canonicaljson.DigestValue(definition.InputSchema)
	resultDigest, resultErr := canonicaljson.DigestValue(version.ResultSchema)
	definitionResultDigest, definitionResultErr := canonicaljson.DigestValue(definition.ResultSchema)
	if inputErr != nil || definitionInputErr != nil || resultErr != nil || definitionResultErr != nil ||
		inputDigest != definitionInputDigest || resultDigest != definitionResultDigest {
		return errors.New("Action input or result schema differs from its Workflow")
	}
	return nil
}

func recordKey(namespaceID string, ref contracts.ActionRef) (store.AggregateKey, error) {
	if !contracts.ValidIdentifier(namespaceID) || !contracts.ValidIdentifier(ref.ActionID) ||
		!contracts.ValidIdentifier(ref.Version) || !contracts.ValidDigest(ref.Digest) {
		return store.AggregateKey{}, errors.New("exact Action namespace or ref is invalid")
	}
	sum := sha256.Sum256([]byte(namespaceID + "\x00" + ref.ActionID + "\x00" + ref.Version + "\x00" + ref.Digest))
	return store.AggregateKey{Type: aggregateType, ID: "act-" + hex.EncodeToString(sum[:])}, nil
}

func decodeRecord(stored store.AggregateRecord, namespaceID string, ref contracts.ActionRef) (Record, error) {
	var record Record
	if stored.Key.Type != aggregateType || stored.Revision == 0 || canonicaljson.UnmarshalStrict(stored.Payload, &record) != nil {
		return Record{}, errors.New("durable exact Action record is invalid")
	}
	expected, err := recordKey(namespaceID, ref)
	if err != nil || expected != stored.Key || record.NamespaceID != namespaceID || !record.Action.Ref().Equal(ref) ||
		record.Revision != stored.Revision || record.InstalledAt.IsZero() {
		return Record{}, errors.New("durable exact Action identity or revision is invalid")
	}
	if err := validateExactAction(record.Action, record.Definition); err != nil {
		return Record{}, fmt.Errorf("durable exact Action: %w", err)
	}
	return record, nil
}
