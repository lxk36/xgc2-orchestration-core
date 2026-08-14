package controller

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/lxk36/xgc2-orchestration-core/durable/store"
	"github.com/lxk36/xgc2-orchestration-core/kernel/canonicaljson"
	"github.com/lxk36/xgc2-orchestration-core/kernel/execution"
	sharedingress "github.com/lxk36/xgc2-orchestration-core/kernel/ingress"
	"github.com/lxk36/xgc2-orchestration-core/sdk/go/contracts"
)

const (
	runAdmissionReceiptType   = "run-admission-receipt"
	runAdmissionReceiptSchema = "xgc.run-admission-receipt/v1"
)

var (
	ErrReservedIngressDenied = errors.New("reserved ingress denied")
	ErrActiveOwnerConflict   = errors.New("active run owner conflict")
)

type ReservedIngressPolicySpec = sharedingress.ReservedIngressPolicySpec
type ReservedIngressPolicy = sharedingress.ReservedIngressPolicy
type IngressPermit = sharedingress.IngressPermit

// NewReservedIngressPolicy returns the policy to install in Config and the
// sole capability that the trusted ingress adapter passes to Invoke.
func NewReservedIngressPolicy(spec ReservedIngressPolicySpec) (*ReservedIngressPolicy, *IngressPermit, error) {
	return sharedingress.NewReservedIngressPolicy(spec)
}

type ingressAdmission struct {
	policyRef    string
	policyDigest string
	ownerKey     *contracts.ActiveOwnerKey
	keyDigest    string
	ownerRef     string
}

func (controller *Controller) admitIngress(request InvokeRequest) (ingressAdmission, error) {
	root := request.Parent == nil
	_, namespaceReserved := controller.reservedNamespaces[request.NamespaceID]
	productBuilderRoot := root && request.Trigger.Kind == contracts.TriggerProductBuilder
	if !root {
		if request.IngressPermit != nil || request.ActiveOwnerKey != nil {
			return ingressAdmission{}, fmt.Errorf("%w: child Action calls cannot carry a root permit", ErrReservedIngressDenied)
		}
		return ingressAdmission{}, nil
	}
	if request.IngressPermit == nil {
		if productBuilderRoot || namespaceReserved {
			return ingressAdmission{}, fmt.Errorf("%w: root ingress requires a registered permit", ErrReservedIngressDenied)
		}
		if request.ActiveOwnerKey != nil {
			return ingressAdmission{}, fmt.Errorf("%w: active owner key requires a registered permit", ErrReservedIngressDenied)
		}
		return ingressAdmission{}, nil
	}
	var registered *registeredIngressPolicy
	for index := range controller.reservedIngress {
		if controller.reservedIngress[index].policy.Authorizes(request.IngressPermit) {
			registered = &controller.reservedIngress[index]
			break
		}
	}
	if registered == nil {
		return ingressAdmission{}, fmt.Errorf("%w: permit is not registered on this controller", ErrReservedIngressDenied)
	}
	policy := registered.spec
	if policy.NamespaceID != request.NamespaceID || policy.TriggerKind != request.Trigger.Kind ||
		policy.TriggerVersion != request.Trigger.Version || policy.SourceRef != request.Trigger.SourceRef ||
		policy.CandidateOrigin != request.CandidateOrigin || (policy.RootOnly && !root) {
		return ingressAdmission{}, fmt.Errorf("%w: request differs from its frozen policy", ErrReservedIngressDenied)
	}
	admission := ingressAdmission{policyRef: policy.PolicyRef, policyDigest: registered.digest}
	if !policy.RequireActiveOwner {
		if request.ActiveOwnerKey != nil {
			return ingressAdmission{}, fmt.Errorf("%w: policy does not authorize active ownership", ErrReservedIngressDenied)
		}
		return admission, nil
	}
	if request.ActiveOwnerKey == nil || request.ActiveOwnerKey.NamespaceID != request.NamespaceID {
		return ingressAdmission{}, fmt.Errorf("%w: policy requires a namespace-matched active owner key", ErrReservedIngressDenied)
	}
	if request.ActiveOwnerKey.Kind != policy.ActiveOwnerKind || len(request.ActiveOwnerKey.Identity) != len(policy.ActiveOwnerIdentityFields) {
		return ingressAdmission{}, fmt.Errorf("%w: active owner key differs from its frozen policy", ErrReservedIngressDenied)
	}
	fields := make([]string, 0, len(request.ActiveOwnerKey.Identity))
	for field := range request.ActiveOwnerKey.Identity {
		fields = append(fields, field)
	}
	sort.Strings(fields)
	for index := range fields {
		if fields[index] != policy.ActiveOwnerIdentityFields[index] {
			return ingressAdmission{}, fmt.Errorf("%w: active owner identity fields differ from policy", ErrReservedIngressDenied)
		}
	}
	key, digest, ownerRef, err := normalizeActiveOwnerKey(*request.ActiveOwnerKey)
	if err != nil {
		return ingressAdmission{}, fmt.Errorf("%w: %v", ErrReservedIngressDenied, err)
	}
	admission.ownerKey, admission.keyDigest, admission.ownerRef = &key, digest, ownerRef
	return admission, nil
}

func normalizeActiveOwnerKey(key contracts.ActiveOwnerKey) (contracts.ActiveOwnerKey, string, string, error) {
	if !contracts.ValidIdentifier(key.NamespaceID) || !contracts.ValidIdentifier(key.Kind) ||
		len(key.Identity) == 0 || len(key.Identity) > 32 {
		return contracts.ActiveOwnerKey{}, "", "", errors.New("active owner key identity is invalid")
	}
	identity := make(map[string]string, len(key.Identity))
	for field, value := range key.Identity {
		if !contracts.ValidIdentifier(field) || value == "" || len(value) > 1024 || !utf8.ValidString(value) ||
			strings.TrimSpace(value) != value {
			return contracts.ActiveOwnerKey{}, "", "", errors.New("active owner key contains an invalid field or value")
		}
		identity[field] = value
	}
	normalized := contracts.ActiveOwnerKey{NamespaceID: key.NamespaceID, Kind: key.Kind, Identity: identity}
	digest, err := canonicaljson.DigestValue(map[string]any{
		"schemaVersion": "xgc.active-owner-key/v1", "key": normalized,
	})
	if err != nil {
		return contracts.ActiveOwnerKey{}, "", "", err
	}
	return normalized, digest, digestRef("active-owner", digest), nil
}

// ActiveOwnerKeyDigest exposes the exact canonical identity used by the CAS
// aggregate without exposing its storage key construction.
func ActiveOwnerKeyDigest(key contracts.ActiveOwnerKey) (string, error) {
	_, digest, _, err := normalizeActiveOwnerKey(key)
	return digest, err
}

// ActiveOwnerConflictError reports the durable winner of a single-active-run
// race while preserving errors.Is(err, ErrActiveOwnerConflict).
type ActiveOwnerConflictError struct{ Owner contracts.ActiveRunOwner }

func (conflict *ActiveOwnerConflictError) Error() string {
	return fmt.Sprintf("%v: %s is held by Run %s", ErrActiveOwnerConflict, conflict.Owner.OwnerRef, conflict.Owner.RunID)
}

func (conflict *ActiveOwnerConflictError) Unwrap() error { return ErrActiveOwnerConflict }

func activeOwnerKey(ownerRef string) store.AggregateKey {
	return store.AggregateKey{Type: activeOwnerAggregateType, ID: ownerRef}
}

type runAdmissionReceipt struct {
	SchemaVersion         string        `json:"schemaVersion"`
	CommandID             string        `json:"commandId"`
	RequestIdentityDigest string        `json:"requestIdentityDigest"`
	AcceptedRun           contracts.Run `json:"acceptedRun"`
	OwnerRef              string        `json:"ownerRef"`
	OwnerGeneration       uint64        `json:"ownerGeneration"`
}

func runAdmissionReceiptKey(commandID string) store.AggregateKey {
	return store.AggregateKey{Type: runAdmissionReceiptType, ID: commandID}
}

func (controller *Controller) getRunAdmissionReceipt(ctx context.Context, commandID string) (runAdmissionReceipt, bool, error) {
	record, err := controller.store.GetAggregate(ctx, runAdmissionReceiptKey(commandID))
	if errors.Is(err, store.ErrNotFound) {
		return runAdmissionReceipt{}, false, nil
	}
	if err != nil {
		return runAdmissionReceipt{}, false, err
	}
	if record.Revision != 1 {
		return runAdmissionReceipt{}, false, store.ErrCorrupt
	}
	var receipt runAdmissionReceipt
	if err := canonicaljson.UnmarshalStrict(record.Payload, &receipt); err != nil {
		return runAdmissionReceipt{}, false, errors.Join(store.ErrCorrupt, err)
	}
	if receipt.SchemaVersion != runAdmissionReceiptSchema || !contracts.ValidIdentifier(receipt.CommandID) ||
		receipt.CommandID != commandID || !contracts.ValidDigest(receipt.RequestIdentityDigest) ||
		!contracts.ValidIdentifier(receipt.OwnerRef) || receipt.OwnerGeneration == 0 ||
		receipt.AcceptedRun.Status != contracts.RunAccepted || receipt.AcceptedRun.Revision != 1 ||
		receipt.AcceptedRun.ActiveOwnerRef != receipt.OwnerRef ||
		receipt.AcceptedRun.ActiveOwnerGeneration != receipt.OwnerGeneration {
		return runAdmissionReceipt{}, false, store.ErrCorrupt
	}
	if err := execution.ValidateRun(receipt.AcceptedRun); err != nil {
		return runAdmissionReceipt{}, false, errors.Join(store.ErrCorrupt, err)
	}
	return receipt, true, nil
}

func decodeActiveRunOwner(record store.AggregateRecord) (contracts.ActiveRunOwner, error) {
	var owner contracts.ActiveRunOwner
	if err := canonicaljson.UnmarshalStrict(record.Payload, &owner); err != nil {
		return contracts.ActiveRunOwner{}, errors.Join(store.ErrCorrupt, err)
	}
	if record.Key != activeOwnerKey(owner.OwnerRef) || record.Revision != owner.Revision {
		return contracts.ActiveRunOwner{}, store.ErrCorrupt
	}
	if err := validateActiveRunOwner(owner); err != nil {
		return contracts.ActiveRunOwner{}, errors.Join(store.ErrCorrupt, err)
	}
	return owner, nil
}

func validateActiveRunOwner(owner contracts.ActiveRunOwner) error {
	if owner.SchemaVersion != contracts.ActiveRunOwnerSchemaVersion || !contracts.ValidIdentifier(owner.OwnerRef) ||
		!contracts.ValidDigest(owner.KeyDigest) || !contracts.ValidIdentifier(owner.PolicyRef) ||
		!contracts.ValidDigest(owner.PolicyDigest) || !owner.State.Valid() || !contracts.ValidIdentifier(owner.RunID) ||
		owner.Generation == 0 || owner.Revision == 0 || owner.AcquiredAt.IsZero() {
		return errors.New("active run owner envelope is invalid")
	}
	_, digest, ownerRef, err := normalizeActiveOwnerKey(owner.Key)
	if err != nil || digest != owner.KeyDigest || ownerRef != owner.OwnerRef {
		return errors.New("active run owner canonical key is invalid")
	}
	switch owner.State {
	case contracts.ActiveRunOwnerActive:
		if owner.Revision != 2*owner.Generation-1 || owner.ReleasedAt != nil || owner.TerminalStatus != "" || owner.TerminalRevision != 0 {
			return errors.New("active owner generation or release facts are invalid")
		}
	case contracts.ActiveRunOwnerReleased:
		if owner.Revision != 2*owner.Generation || owner.ReleasedAt == nil || owner.ReleasedAt.Before(owner.AcquiredAt) ||
			!owner.TerminalStatus.Terminal() || owner.TerminalRevision == 0 {
			return errors.New("released owner terminal facts are invalid")
		}
	}
	return nil
}

func (controller *Controller) prepareActiveOwnerAcquire(
	ctx context.Context, ingress ingressAdmission, runID string, at time.Time,
) (contracts.ActiveRunOwner, uint64, error) {
	if ingress.ownerKey == nil {
		return contracts.ActiveRunOwner{}, 0, errors.New("active owner admission has no key")
	}
	record, err := controller.store.GetAggregate(ctx, activeOwnerKey(ingress.ownerRef))
	if errors.Is(err, store.ErrNotFound) {
		return contracts.ActiveRunOwner{
			SchemaVersion: contracts.ActiveRunOwnerSchemaVersion, OwnerRef: ingress.ownerRef,
			KeyDigest: ingress.keyDigest, Key: *ingress.ownerKey,
			PolicyRef: ingress.policyRef, PolicyDigest: ingress.policyDigest,
			State: contracts.ActiveRunOwnerActive, RunID: runID, Generation: 1,
			AcquiredAt: at, Revision: 1,
		}, 0, nil
	}
	if err != nil {
		return contracts.ActiveRunOwner{}, 0, err
	}
	current, err := decodeActiveRunOwner(record)
	if err != nil {
		return contracts.ActiveRunOwner{}, 0, err
	}
	if current.KeyDigest != ingress.keyDigest || current.OwnerRef != ingress.ownerRef {
		return contracts.ActiveRunOwner{}, 0, store.ErrCorrupt
	}
	if current.State == contracts.ActiveRunOwnerActive {
		// The deliberately stale create CAS reaches Store.Commit. Its command
		// ledger may still replay an old exact Start before aggregate CAS is
		// evaluated; a genuinely new command receives a revision conflict.
		return contracts.ActiveRunOwner{
			SchemaVersion: contracts.ActiveRunOwnerSchemaVersion, OwnerRef: ingress.ownerRef,
			KeyDigest: ingress.keyDigest, Key: *ingress.ownerKey,
			PolicyRef: ingress.policyRef, PolicyDigest: ingress.policyDigest,
			State: contracts.ActiveRunOwnerActive, RunID: runID, Generation: 1,
			AcquiredAt: at, Revision: 1,
		}, 0, nil
	}
	return contracts.ActiveRunOwner{
		SchemaVersion: contracts.ActiveRunOwnerSchemaVersion, OwnerRef: ingress.ownerRef,
		KeyDigest: ingress.keyDigest, Key: *ingress.ownerKey,
		PolicyRef: ingress.policyRef, PolicyDigest: ingress.policyDigest,
		State: contracts.ActiveRunOwnerActive, RunID: runID, Generation: current.Generation + 1,
		AcquiredAt: at, Revision: current.Revision + 1,
	}, current.Revision, nil
}

func (controller *Controller) releaseActiveOwner(
	ctx context.Context, currentRun, terminalRun contracts.Run, commandID string, at time.Time,
) (store.ExpectedRevision, store.AggregateRecord, contracts.DomainEvent, bool, error) {
	if currentRun.ActiveOwnerRef == "" {
		return store.ExpectedRevision{}, store.AggregateRecord{}, contracts.DomainEvent{}, false, nil
	}
	if !terminalRun.Status.Terminal() || terminalRun.RunID != currentRun.RunID ||
		terminalRun.ActiveOwnerRef != currentRun.ActiveOwnerRef ||
		terminalRun.ActiveOwnerGeneration != currentRun.ActiveOwnerGeneration ||
		terminalRun.AdmissionPolicyRef != currentRun.AdmissionPolicyRef ||
		terminalRun.AdmissionPolicyDigest != currentRun.AdmissionPolicyDigest {
		return store.ExpectedRevision{}, store.AggregateRecord{}, contracts.DomainEvent{}, false,
			errors.New("terminal Run changed its active owner admission fence")
	}
	record, err := controller.store.GetAggregate(ctx, activeOwnerKey(currentRun.ActiveOwnerRef))
	if err != nil {
		return store.ExpectedRevision{}, store.AggregateRecord{}, contracts.DomainEvent{}, false, err
	}
	owner, err := decodeActiveRunOwner(record)
	if err != nil {
		return store.ExpectedRevision{}, store.AggregateRecord{}, contracts.DomainEvent{}, false, err
	}
	if owner.State != contracts.ActiveRunOwnerActive || owner.RunID != currentRun.RunID ||
		owner.Key.NamespaceID != currentRun.NamespaceID ||
		owner.Generation != currentRun.ActiveOwnerGeneration || owner.PolicyRef != currentRun.AdmissionPolicyRef ||
		owner.PolicyDigest != currentRun.AdmissionPolicyDigest {
		return store.ExpectedRevision{}, store.AggregateRecord{}, contracts.DomainEvent{}, false,
			errors.New("active owner does not match the Run terminal fence")
	}
	releasedAt := at.UTC()
	owner.State = contracts.ActiveRunOwnerReleased
	owner.ReleasedAt = &releasedAt
	owner.TerminalStatus = terminalRun.Status
	owner.TerminalRevision = terminalRun.Revision
	owner.Revision++
	if err := validateActiveRunOwner(owner); err != nil {
		return store.ExpectedRevision{}, store.AggregateRecord{}, contracts.DomainEvent{}, false, err
	}
	mutation, err := aggregateMutation(activeOwnerKey(owner.OwnerRef), record.Revision, owner)
	if err != nil {
		return store.ExpectedRevision{}, store.AggregateRecord{}, contracts.DomainEvent{}, false, err
	}
	event, err := aggregateEvent(mutation, "active-run-owner.released", commandID, at, map[string]any{
		"runId": terminalRun.RunID, "ownerRef": owner.OwnerRef, "generation": owner.Generation,
		"terminalStatus": terminalRun.Status, "terminalRevision": terminalRun.Revision,
	})
	if err != nil {
		return store.ExpectedRevision{}, store.AggregateRecord{}, contracts.DomainEvent{}, false, err
	}
	return store.ExpectedRevision{Key: mutation.Key, Revision: record.Revision}, mutation, event, true, nil
}

// GetActiveRunOwner reads the complete durable owner generation, active or
// released, for an exact canonical key.
func (controller *Controller) GetActiveRunOwner(ctx context.Context, key contracts.ActiveOwnerKey) (contracts.ActiveRunOwner, error) {
	if controller == nil || ctx == nil {
		return contracts.ActiveRunOwner{}, errors.New("controller and context are required")
	}
	_, _, ownerRef, err := normalizeActiveOwnerKey(key)
	if err != nil {
		return contracts.ActiveRunOwner{}, err
	}
	record, err := controller.store.GetAggregate(ctx, activeOwnerKey(ownerRef))
	if err != nil {
		return contracts.ActiveRunOwner{}, err
	}
	return decodeActiveRunOwner(record)
}

// ValidateRunActiveOwnerKey proves that runID was admitted under the exact
// canonical owner key. It validates the Run's frozen historical fence only;
// the owner generation may already be released or a newer generation may now
// be active. This makes it suitable for authorization before terminal reads
// and exact command replay without turning a mutable owner head into history.
func (controller *Controller) ValidateRunActiveOwnerKey(
	ctx context.Context, key contracts.ActiveOwnerKey, runID string,
) (contracts.Run, error) {
	if controller == nil || ctx == nil || !contracts.ValidIdentifier(runID) {
		return contracts.Run{}, errors.New("controller, context, and target Run are required")
	}
	normalized, _, ownerRef, err := normalizeActiveOwnerKey(key)
	if err != nil {
		return contracts.Run{}, err
	}
	run, err := controller.GetRun(ctx, runID)
	if err != nil {
		return contracts.Run{}, err
	}
	if run.NamespaceID != normalized.NamespaceID || run.ActiveOwnerRef != ownerRef ||
		run.ActiveOwnerGeneration == 0 || !contracts.ValidIdentifier(run.AdmissionPolicyRef) ||
		!contracts.ValidDigest(run.AdmissionPolicyDigest) {
		return contracts.Run{}, fmt.Errorf("%w: key and Run frozen ownership differ", ErrReservedIngressDenied)
	}
	return run, nil
}

// ResolveActiveRun returns the Run currently protected by key. A released
// owner deliberately resolves as store.ErrNotFound rather than a stale Run.
func (controller *Controller) ResolveActiveRun(ctx context.Context, key contracts.ActiveOwnerKey) (contracts.Run, error) {
	owner, err := controller.GetActiveRunOwner(ctx, key)
	if err != nil {
		return contracts.Run{}, err
	}
	if owner.State != contracts.ActiveRunOwnerActive {
		return contracts.Run{}, store.ErrNotFound
	}
	run, err := controller.GetRun(ctx, owner.RunID)
	if err != nil {
		return contracts.Run{}, err
	}
	if run.NamespaceID != owner.Key.NamespaceID || run.ActiveOwnerRef != owner.OwnerRef ||
		run.ActiveOwnerGeneration != owner.Generation || run.AdmissionPolicyRef != owner.PolicyRef ||
		run.AdmissionPolicyDigest != owner.PolicyDigest || run.Status.Terminal() {
		return contracts.Run{}, errors.New("active owner and Run aggregate disagree")
	}
	return run, nil
}
