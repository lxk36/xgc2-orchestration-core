package runtime

import (
	"errors"
	"fmt"
	"time"

	"github.com/lxk36/xgc2-orchestration-core/kernel/canonicaljson"
	"github.com/lxk36/xgc2-orchestration-core/kernel/execution"
	"github.com/lxk36/xgc2-orchestration-core/sdk/go/contracts"
)

func ValidateBinding(binding contracts.RuntimeBinding) error {
	for label, value := range map[string]string{
		"binding id": binding.BindingID, "namespace id": binding.NamespaceID, "run id": binding.RunID,
		"invocation id": binding.InvocationID, "node id": binding.NodeID, "runtime key": binding.RuntimeKey,
		"provider ref": binding.ProviderRef,
	} {
		if err := validateIdentity(value, label); err != nil {
			return err
		}
	}
	expectedID, err := StableBindingID(binding.InvocationID, binding.RuntimeKey)
	if err != nil || expectedID != binding.BindingID {
		return errors.New("runtime binding stable identity is invalid")
	}
	if !contracts.ValidTypeRef(binding.Kind) || !contracts.ValidDigest(binding.SpecDigest) || !contracts.ValidDigest(binding.ProviderDigest) {
		return errors.New("runtime kind, spec digest, or provider digest is invalid")
	}
	if !binding.Ownership.Valid() || !binding.CleanupPolicy.Valid() || !binding.State.Valid() || !binding.ObservedState.Valid() || !binding.Health.Valid() {
		return errors.New("runtime ownership, cleanup, state, observation, or health is invalid")
	}
	if binding.Ownership == contracts.EffectOwned {
		if binding.CleanupPolicy == contracts.RuntimeCleanupNone {
			return errors.New("owned runtime requires cleanup policy")
		}
	} else if binding.CleanupPolicy != contracts.RuntimeCleanupNone {
		return errors.New("attached or shared runtime cannot be destroyed by cleanup")
	}
	if binding.Generation == 0 || binding.FencingToken == 0 || binding.Revision == 0 || binding.CreatedAt.IsZero() || binding.UpdatedAt.Before(binding.CreatedAt) {
		return errors.New("runtime generation, fencing token, revision, or timestamps are invalid")
	}
	if binding.State.Terminal() {
		if binding.FinishedAt == nil || binding.LeaseOwner != "" || binding.LeaseTokenHash != "" || !binding.LeaseExpiresAt.IsZero() {
			return errors.New("terminal runtime binding retains lease or lacks finish time")
		}
	} else {
		if err := validateIdentity(binding.LeaseOwner, "runtime lease owner"); err != nil {
			return err
		}
		if !contracts.ValidDigest(binding.LeaseTokenHash) || !binding.LeaseExpiresAt.After(binding.UpdatedAt) || binding.FinishedAt != nil {
			return errors.New("live runtime binding requires unexpired hashed lease and no finish time")
		}
	}
	if binding.State == contracts.RuntimeBindingActive && (binding.ActivatedAt == nil || binding.ExternalIdentity == "" || binding.ObservedState != contracts.RuntimeObservedRunning) {
		return errors.New("active runtime requires running external identity and activation time")
	}
	if binding.State == contracts.RuntimeBindingStopping && (binding.Ownership != contracts.EffectOwned || binding.ReleaseStartedAt == nil) {
		return errors.New("stopping runtime requires owned release lifecycle")
	}
	if binding.State == contracts.RuntimeBindingReleased && binding.Ownership == contracts.EffectOwned && binding.ObservedState != contracts.RuntimeObservedStopped {
		return errors.New("released owned runtime lacks stopped observation")
	}
	if binding.State == contracts.RuntimeBindingLost && (binding.ObservedState != contracts.RuntimeObservedLost || binding.Health != contracts.RuntimeHealthLost || binding.PrimaryFailure == nil) {
		return errors.New("lost runtime requires lost observation, health, and failure")
	}
	if binding.ObservationDigest == "" {
		if binding.ObservationAt != nil {
			return errors.New("runtime observation time exists without digest")
		}
	} else if !contracts.ValidDigest(binding.ObservationDigest) || binding.ObservationAt == nil || binding.ObservationAt.After(binding.UpdatedAt) {
		return errors.New("runtime observation digest or time is invalid")
	}
	if binding.ExternalIdentity != "" {
		if err := validateIdentity(binding.ExternalIdentity, "runtime external identity"); err != nil {
			return err
		}
	}
	if binding.PrimaryFailure != nil {
		if err := validateFailure(binding.PrimaryFailure); err != nil {
			return err
		}
		if binding.ObservedState != contracts.RuntimeObservedFailed && binding.ObservedState != contracts.RuntimeObservedLost {
			return errors.New("runtime failure exists outside failed or lost observation")
		}
	}
	return nil
}

func validatePrepare(command PrepareBindingCommand) error {
	for label, value := range map[string]string{
		"binding id": command.BindingID, "namespace id": command.NamespaceID, "run id": command.RunID,
		"invocation id": command.InvocationID, "node id": command.NodeID, "runtime key": command.RuntimeKey,
		"provider ref": command.ProviderRef, "lease owner": command.LeaseOwner, "command id": command.CommandID,
	} {
		if err := validateIdentity(value, label); err != nil {
			return err
		}
	}
	if !contracts.ValidTypeRef(command.Kind) || !contracts.ValidDigest(command.SpecDigest) || !contracts.ValidDigest(command.ProviderDigest) {
		return errors.New("runtime kind, spec digest, or provider digest is invalid")
	}
	if !command.Ownership.Valid() || !command.CleanupPolicy.Valid() || command.Generation != 1 || command.FencingToken == 0 ||
		command.At.IsZero() || !command.LeaseExpiresAt.After(command.At) {
		return errors.New("runtime initial ownership, cleanup, generation, fence, or time is invalid")
	}
	if command.Ownership == contracts.EffectOwned && command.CleanupPolicy == contracts.RuntimeCleanupNone {
		return errors.New("owned runtime requires cleanup policy")
	}
	if command.Ownership != contracts.EffectOwned && command.CleanupPolicy != contracts.RuntimeCleanupNone {
		return errors.New("non-owned runtime must use no cleanup")
	}
	_, err := execution.PrivateTokenDigest(command.LeaseToken)
	return err
}

func validateFence(binding contracts.RuntimeBinding, fence BindingFence) error {
	if fence.BindingID != binding.BindingID || fence.ExpectedRevision != binding.Revision {
		return ErrRevisionConflict
	}
	if fence.Generation != binding.Generation || fence.FencingToken != binding.FencingToken {
		return ErrFenceConflict
	}
	if fence.LeaseOwner != binding.LeaseOwner {
		return ErrLeaseConflict
	}
	hash, err := execution.PrivateTokenDigest(fence.LeaseToken)
	if err != nil || hash != binding.LeaseTokenHash || fence.At.IsZero() || fence.At.Before(binding.UpdatedAt) || !fence.At.Before(binding.LeaseExpiresAt) {
		return ErrLeaseConflict
	}
	return nil
}

func validateObservationFailure(state contracts.RuntimeObservedState, failure *contracts.StructuredFailure) error {
	required := state == contracts.RuntimeObservedFailed || state == contracts.RuntimeObservedLost
	if required != (failure != nil) {
		return errors.New("runtime failed/lost observation and failure disagree")
	}
	if failure != nil {
		return validateFailure(failure)
	}
	return nil
}

func validateFailure(failure *contracts.StructuredFailure) error {
	if failure == nil || !failure.Class.Valid() || !contracts.ValidIdentifier(failure.Code) || failure.Message == "" || len(failure.Message) > 4096 {
		return errors.New("runtime structured failure is invalid")
	}
	if failure.EvidenceRef != "" && !contracts.ValidIdentifier(failure.EvidenceRef) {
		return errors.New("runtime failure evidence ref is invalid")
	}
	return nil
}

func validateIdentity(value, label string) error {
	if !contracts.ValidIdentifier(value) {
		return fmt.Errorf("%s must be a portable identifier", label)
	}
	return nil
}

func newEvent(binding contracts.RuntimeBinding, eventType, commandID string, at time.Time, payload map[string]any) (contracts.DomainEvent, error) {
	digest, err := canonicaljson.DigestValue(payload)
	if err != nil {
		return contracts.DomainEvent{}, err
	}
	id, err := execution.StableEventID("runtime", binding.BindingID, binding.Revision)
	if err != nil {
		return contracts.DomainEvent{}, err
	}
	return contracts.DomainEvent{
		EventID: id, AggregateType: "runtime", AggregateID: binding.BindingID, AggregateRevision: binding.Revision,
		Type: eventType, CommandID: commandID,
		PayloadSchemaDigest: "sha256:82ef96cebaf5fbe16269fd18b0240d78f5b9b90a4155a17eb797115b09148ecf",
		PayloadDigest:       digest, Payload: payload, OccurredAt: at.UTC(),
	}, nil
}

func newIntent(kind contracts.DurableIntentKind, binding contracts.RuntimeBinding, payload map[string]any) (contracts.DurableIntent, error) {
	digest, err := canonicaljson.DigestValue(payload)
	if err != nil {
		return contracts.DurableIntent{}, err
	}
	id, err := execution.StableIntentID(kind, binding.BindingID, binding.Revision)
	if err != nil {
		return contracts.DurableIntent{}, err
	}
	return contracts.DurableIntent{Kind: kind, Identity: id, AggregateID: binding.BindingID, PayloadDigest: digest, Payload: payload}, nil
}

func clearLease(binding *contracts.RuntimeBinding) {
	binding.LeaseOwner = ""
	binding.LeaseTokenHash = ""
	binding.LeaseExpiresAt = time.Time{}
}

func cloneBinding(binding contracts.RuntimeBinding) contracts.RuntimeBinding {
	binding.ObservationAt = cloneTime(binding.ObservationAt)
	binding.PrimaryFailure = cloneFailure(binding.PrimaryFailure)
	binding.ActivatedAt = cloneTime(binding.ActivatedAt)
	binding.ReleaseStartedAt = cloneTime(binding.ReleaseStartedAt)
	binding.FinishedAt = cloneTime(binding.FinishedAt)
	return binding
}

func cloneTime(value *time.Time) *time.Time {
	if value == nil {
		return nil
	}
	copy := *value
	return &copy
}

func cloneFailure(value *contracts.StructuredFailure) *contracts.StructuredFailure {
	if value == nil {
		return nil
	}
	copy := *value
	return &copy
}
