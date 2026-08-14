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
	"github.com/lxk36/xgc2-orchestration-core/kernel/ingress"
	workflowkernel "github.com/lxk36/xgc2-orchestration-core/kernel/workflow"
	"github.com/lxk36/xgc2-orchestration-core/sdk/go/contracts"
)

const aggregateType = "action-version"

const eventSchemaDigest = "sha256:c4724a26a4e3afa089e6eedaff1f90614fd8ce01da78f036ad26bc26f55c4719"

var (
	ErrActionConflict          = errors.New("exact Action catalog identity conflict")
	ErrReservedNamespaceDenied = errors.New("reserved Action namespace denied")
)

type Catalog struct {
	store    store.Store
	reserved map[string][]reservedPolicy
}

type reservedPolicy struct {
	policy *ingress.ReservedIngressPolicy
	spec   ingress.ReservedIngressPolicySpec
}

type Config struct {
	Store                   store.Store
	ReservedIngressPolicies []*ingress.ReservedIngressPolicy
}

type Record struct {
	NamespaceID           string                       `json:"namespaceId"`
	Action                contracts.ActionVersion      `json:"action"`
	Definition            contracts.WorkflowDefinition `json:"definition"`
	AdmissionPolicyRef    string                       `json:"admissionPolicyRef,omitempty"`
	AdmissionPolicyDigest string                       `json:"admissionPolicyDigest,omitempty"`
	InstalledAt           time.Time                    `json:"installedAt"`
	Revision              uint64                       `json:"revision"`
}

type InstallRequest struct {
	NamespaceID   string
	Action        contracts.ActionVersion
	Definition    contracts.WorkflowDefinition
	IngressPermit *ingress.IngressPermit
	CommandID     string
	At            time.Time
}

type InstallResult struct {
	Record Record `json:"record"`
	Replay bool   `json:"replay"`
}

// Metadata is the bounded discovery projection used by editors and agents.
// The immutable Workflow definition is returned only by Get so listing a
// catalog never scales with the combined size of every installed graph.
type Metadata struct {
	NamespaceID           string                  `json:"namespaceId"`
	Action                contracts.ActionVersion `json:"action"`
	AdmissionPolicyRef    string                  `json:"admissionPolicyRef,omitempty"`
	AdmissionPolicyDigest string                  `json:"admissionPolicyDigest,omitempty"`
	InstalledAt           time.Time               `json:"installedAt"`
	Revision              uint64                  `json:"revision"`
}

type ListRequest struct {
	NamespaceID string
	After       string
	Limit       int
}

type Page struct {
	Items      []Metadata `json:"items"`
	NextCursor string     `json:"nextCursor,omitempty"`
}

func New(durable store.Store) (*Catalog, error) {
	return NewWithConfig(Config{Store: durable})
}

func NewWithConfig(config Config) (*Catalog, error) {
	if config.Store == nil {
		return nil, errors.New("Action catalog durable store is required")
	}
	reserved := make(map[string][]reservedPolicy)
	seenRefs := make(map[string]struct{}, len(config.ReservedIngressPolicies))
	seenPolicies := make(map[*ingress.ReservedIngressPolicy]struct{}, len(config.ReservedIngressPolicies))
	for _, policy := range config.ReservedIngressPolicies {
		spec, valid := policy.Spec()
		if !valid || !contracts.ValidDigest(policy.Digest()) {
			return nil, errors.New("Action catalog reserved ingress policy is invalid")
		}
		if _, duplicate := seenPolicies[policy]; duplicate {
			return nil, errors.New("Action catalog reserved ingress capability is duplicated")
		}
		if _, duplicate := seenRefs[spec.PolicyRef]; duplicate {
			return nil, errors.New("Action catalog reserved ingress policy ref is duplicated")
		}
		reserved[spec.NamespaceID] = append(reserved[spec.NamespaceID], reservedPolicy{policy: policy, spec: spec})
		seenPolicies[policy] = struct{}{}
		seenRefs[spec.PolicyRef] = struct{}{}
	}
	return &Catalog{store: config.Store, reserved: reserved}, nil
}

func (catalog *Catalog) Install(ctx context.Context, request InstallRequest) (InstallResult, error) {
	if catalog == nil || catalog.store == nil || ctx == nil {
		return InstallResult{}, errors.New("Action catalog is unavailable")
	}
	if !contracts.ValidIdentifier(request.NamespaceID) || !contracts.ValidIdentifier(request.CommandID) || request.At.IsZero() {
		return InstallResult{}, errors.New("Action catalog namespace, command, and time are required")
	}
	policyRef, policyDigest, err := catalog.authorizeInstall(request.NamespaceID, request.Action, request.IngressPermit)
	if err != nil {
		return InstallResult{}, err
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
		AdmissionPolicyRef: policyRef, AdmissionPolicyDigest: policyDigest,
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
		current, decodeErr := catalog.decodeCurrentRecord(existing, request.NamespaceID, request.Action.Ref())
		if decodeErr != nil {
			return InstallResult{}, decodeErr
		}
		currentDigest, digestErr := recordContentDigest(current)
		requestedDigest, requestDigestErr := recordContentDigest(record)
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
		"admissionPolicyRef": policyRef, "admissionPolicyDigest": policyDigest,
	})
	if err != nil {
		return InstallResult{}, err
	}
	eventPayload := map[string]any{
		"namespaceId": request.NamespaceID, "actionRef": request.Action.Ref(),
		"definitionDigest":   request.Action.DefinitionDigest,
		"admissionPolicyRef": policyRef, "admissionPolicyDigest": policyDigest,
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
	commandScope := store.CommandScope{
		SchemaVersion: store.CommandScopeSchemaVersion, Operation: "action.install",
		NamespaceID: request.NamespaceID, ResourceType: key.Type, ResourceID: key.ID,
		AuthorityRef: policyRef, AuthorityDigest: policyDigest,
	}
	if err := commandScope.Validate(); err != nil {
		return InstallResult{}, err
	}
	committed, err := catalog.store.Commit(ctx, store.Transaction{
		CommandScope: commandScope, CommandID: request.CommandID, IdentityDigest: identityDigest,
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
	record, err := catalog.decodeCurrentRecord(stored, namespaceID, ref)
	if err != nil {
		return contracts.ActionVersion{}, contracts.WorkflowDefinition{}, err
	}
	return record.Action, record.Definition, nil
}

// Get returns one exact immutable catalog record. There are deliberately no
// latest-version, branch, alias, or partial-identity lookup semantics.
func (catalog *Catalog) Get(ctx context.Context, namespaceID string, ref contracts.ActionRef) (Record, error) {
	if catalog == nil || catalog.store == nil || ctx == nil {
		return Record{}, errors.New("Action catalog is unavailable")
	}
	key, err := recordKey(namespaceID, ref)
	if err != nil {
		return Record{}, err
	}
	stored, err := catalog.store.GetAggregate(ctx, key)
	if err != nil {
		return Record{}, err
	}
	return catalog.decodeCurrentRecord(stored, namespaceID, ref)
}

// List exposes a stable, bounded namespace projection. The durable cursor is
// opaque to callers and advances across every scanned catalog aggregate, not
// merely returned matches, so interleaved namespaces cannot duplicate or skip
// records between pages.
func (catalog *Catalog) List(ctx context.Context, request ListRequest) (Page, error) {
	if catalog == nil || catalog.store == nil || ctx == nil {
		return Page{}, errors.New("Action catalog is unavailable")
	}
	if !contracts.ValidIdentifier(request.NamespaceID) || request.Limit <= 0 || request.Limit > 1000 ||
		(request.After != "" && !contracts.ValidIdentifier(request.After)) {
		return Page{}, errors.New("Action catalog namespace, cursor, or limit is invalid")
	}
	page := Page{Items: make([]Metadata, 0, request.Limit)}
	cursor := request.After
	for len(page.Items) < request.Limit {
		remaining := request.Limit - len(page.Items)
		scanLimit := remaining * 4
		if scanLimit < 100 {
			scanLimit = 100
		}
		if scanLimit > 1000 {
			scanLimit = 1000
		}
		records, err := catalog.store.ListAggregates(ctx, aggregateType, cursor, scanLimit)
		if err != nil {
			return Page{}, err
		}
		if len(records) == 0 {
			page.NextCursor = ""
			break
		}
		for _, stored := range records {
			cursor = stored.Key.ID
			var candidate Record
			if canonicaljson.UnmarshalStrict(stored.Payload, &candidate) != nil {
				return Page{}, errors.New("durable exact Action record is invalid")
			}
			if candidate.NamespaceID != request.NamespaceID {
				continue
			}
			decoded, decodeErr := catalog.decodeCurrentRecord(stored, candidate.NamespaceID, candidate.Action.Ref())
			if decodeErr != nil {
				return Page{}, decodeErr
			}
			page.Items = append(page.Items, Metadata{
				NamespaceID: decoded.NamespaceID, Action: decoded.Action,
				AdmissionPolicyRef: decoded.AdmissionPolicyRef, AdmissionPolicyDigest: decoded.AdmissionPolicyDigest,
				InstalledAt: decoded.InstalledAt, Revision: decoded.Revision,
			})
			if len(page.Items) == request.Limit {
				page.NextCursor = cursor
				return page, nil
			}
		}
		if len(records) < scanLimit {
			page.NextCursor = ""
			break
		}
	}
	return page, nil
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

func (catalog *Catalog) authorizeInstall(
	namespaceID string, version contracts.ActionVersion, permit *ingress.IngressPermit,
) (string, string, error) {
	policies := catalog.reserved[namespaceID]
	if len(policies) == 0 {
		if permit != nil {
			return "", "", fmt.Errorf("%w: permit does not target this namespace", ErrReservedNamespaceDenied)
		}
		return "", "", nil
	}
	for _, registered := range policies {
		if !registered.policy.Authorizes(permit) {
			continue
		}
		accepted := false
		for _, kind := range version.AcceptedTriggerKinds {
			if kind == registered.spec.TriggerKind {
				accepted = true
				break
			}
		}
		if !accepted {
			return "", "", fmt.Errorf("%w: Action does not accept the policy trigger", ErrReservedNamespaceDenied)
		}
		return registered.spec.PolicyRef, registered.policy.Digest(), nil
	}
	return "", "", fmt.Errorf("%w: a registered permit is required", ErrReservedNamespaceDenied)
}

func recordContentDigest(record Record) (string, error) {
	return canonicaljson.DigestValue(struct {
		NamespaceID           string                       `json:"namespaceId"`
		Action                contracts.ActionVersion      `json:"action"`
		Definition            contracts.WorkflowDefinition `json:"definition"`
		AdmissionPolicyRef    string                       `json:"admissionPolicyRef,omitempty"`
		AdmissionPolicyDigest string                       `json:"admissionPolicyDigest,omitempty"`
	}{
		NamespaceID: record.NamespaceID, Action: record.Action, Definition: record.Definition,
		AdmissionPolicyRef: record.AdmissionPolicyRef, AdmissionPolicyDigest: record.AdmissionPolicyDigest,
	})
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
	if (record.AdmissionPolicyRef == "") != (record.AdmissionPolicyDigest == "") ||
		(record.AdmissionPolicyRef != "" && (!contracts.ValidIdentifier(record.AdmissionPolicyRef) || !contracts.ValidDigest(record.AdmissionPolicyDigest))) {
		return Record{}, errors.New("durable exact Action admission policy is invalid")
	}
	return record, nil
}

// decodeCurrentRecord binds every read to the Catalog's currently registered
// reserved-ingress policy seal. A generic record that predates reservation, or
// a record admitted under an older ref/digest, is deliberately not readable.
func (catalog *Catalog) decodeCurrentRecord(stored store.AggregateRecord, namespaceID string, ref contracts.ActionRef) (Record, error) {
	record, err := decodeRecord(stored, namespaceID, ref)
	if err != nil {
		return Record{}, err
	}
	policies := catalog.reserved[namespaceID]
	if len(policies) == 0 {
		if record.AdmissionPolicyRef != "" || record.AdmissionPolicyDigest != "" {
			return Record{}, fmt.Errorf("%w: Action record carries a policy for an unreserved namespace", ErrReservedNamespaceDenied)
		}
		return record, nil
	}
	for _, registered := range policies {
		if record.AdmissionPolicyRef != registered.spec.PolicyRef || record.AdmissionPolicyDigest != registered.policy.Digest() {
			continue
		}
		for _, kind := range record.Action.AcceptedTriggerKinds {
			if kind == registered.spec.TriggerKind {
				return record, nil
			}
		}
		return Record{}, fmt.Errorf("%w: durable Action no longer accepts its sealed policy trigger", ErrReservedNamespaceDenied)
	}
	return Record{}, fmt.Errorf("%w: durable Action policy ref or digest is not current", ErrReservedNamespaceDenied)
}
