// Package runtime implements product-neutral ownership and generation fencing
// for managed processes and other long-lived runtime objects.
package runtime

import (
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"time"

	"github.com/lxk36/xgc2-orchestration-core/kernel/canonicaljson"
	"github.com/lxk36/xgc2-orchestration-core/kernel/execution"
	"github.com/lxk36/xgc2-orchestration-core/sdk/go/contracts"
)

var (
	ErrRevisionConflict  = errors.New("runtime revision conflict")
	ErrFenceConflict     = errors.New("runtime generation fence conflict")
	ErrLeaseConflict     = errors.New("runtime lease conflict")
	ErrInvalidTransition = errors.New("invalid runtime transition")
)

type PrepareBindingCommand struct {
	BindingID      string
	NamespaceID    string
	RunID          string
	InvocationID   string
	NodeID         string
	RuntimeKey     string
	Kind           string
	SpecDigest     string
	ProviderRef    string
	ProviderDigest string
	Ownership      contracts.EffectOwnership
	CleanupPolicy  contracts.RuntimeCleanupPolicy
	Generation     uint64
	FencingToken   uint64
	LeaseOwner     string
	LeaseToken     string
	LeaseExpiresAt time.Time
	CommandID      string
	At             time.Time
}

type BindingFence struct {
	BindingID        string
	ExpectedRevision uint64
	Generation       uint64
	FencingToken     uint64
	LeaseOwner       string
	LeaseToken       string
	At               time.Time
}

type ObserveCommand struct {
	Fence             BindingFence
	ExternalIdentity  string
	ObservedState     contracts.RuntimeObservedState
	Health            contracts.RuntimeHealth
	ObservationDigest string
	Failure           *contracts.StructuredFailure
	CommandID         string
}

type HeartbeatCommand struct {
	Fence          BindingFence
	LeaseExpiresAt time.Time
	CommandID      string
}

type TakeoverCommand struct {
	BindingID        string
	ExpectedRevision uint64
	Generation       uint64
	FencingToken     uint64
	LeaseOwner       string
	LeaseToken       string
	LeaseExpiresAt   time.Time
	CommandID        string
	At               time.Time
}

type ReleaseCommand struct {
	Fence     BindingFence
	CommandID string
}

type Decision struct {
	Binding contracts.RuntimeBinding
	Events  []contracts.DomainEvent
	Intents []contracts.DurableIntent
}

func StableBindingID(invocationID, runtimeKey string) (string, error) {
	if !contracts.ValidIdentifier(invocationID) || !contracts.ValidIdentifier(runtimeKey) {
		return "", errors.New("runtime binding identity parts are invalid")
	}
	canonical, err := canonicaljson.Marshal([]any{"xgc.runtime-binding/v1", invocationID, runtimeKey})
	if err != nil {
		return "", err
	}
	sum := sha256.Sum256(canonical)
	return "rtb-" + hex.EncodeToString(sum[:]), nil
}

func Prepare(command PrepareBindingCommand) (Decision, error) {
	if err := validatePrepare(command); err != nil {
		return Decision{}, err
	}
	expectedID, _ := StableBindingID(command.InvocationID, command.RuntimeKey)
	if command.BindingID != expectedID {
		return Decision{}, errors.New("runtime binding stable identity is invalid")
	}
	leaseHash, err := execution.PrivateTokenDigest(command.LeaseToken)
	if err != nil {
		return Decision{}, err
	}
	binding := contracts.RuntimeBinding{
		BindingID: command.BindingID, NamespaceID: command.NamespaceID, RunID: command.RunID,
		InvocationID: command.InvocationID, NodeID: command.NodeID, RuntimeKey: command.RuntimeKey,
		Kind: command.Kind, SpecDigest: command.SpecDigest, ProviderRef: command.ProviderRef,
		ProviderDigest: command.ProviderDigest, Ownership: command.Ownership, CleanupPolicy: command.CleanupPolicy,
		State: contracts.RuntimeBindingPreparing, ObservedState: contracts.RuntimeObservedUnknown, Health: contracts.RuntimeHealthUnknown,
		Generation: command.Generation, FencingToken: command.FencingToken, LeaseOwner: command.LeaseOwner,
		LeaseTokenHash: leaseHash, LeaseExpiresAt: command.LeaseExpiresAt.UTC(),
		CreatedAt: command.At.UTC(), UpdatedAt: command.At.UTC(), Revision: 1,
	}
	if err := ValidateBinding(binding); err != nil {
		return Decision{}, err
	}
	event, err := newEvent(binding, "runtime.preparing", command.CommandID, command.At, map[string]any{
		"generation": binding.Generation, "fencingToken": binding.FencingToken, "specDigest": binding.SpecDigest,
	})
	if err != nil {
		return Decision{}, err
	}
	return Decision{Binding: binding, Events: []contracts.DomainEvent{event}}, nil
}

func Observe(current contracts.RuntimeBinding, command ObserveCommand) (Decision, error) {
	if err := ValidateBinding(current); err != nil {
		return Decision{}, err
	}
	if err := validateFence(current, command.Fence); err != nil {
		return Decision{}, err
	}
	if err := validateIdentity(command.CommandID, "runtime observation command id"); err != nil {
		return Decision{}, err
	}
	if !command.ObservedState.Valid() || !command.Health.Valid() || !contracts.ValidDigest(command.ObservationDigest) {
		return Decision{}, errors.New("runtime observation state, health, or digest is invalid")
	}
	if current.State.Terminal() {
		return Decision{}, fmt.Errorf("%w: terminal runtime cannot observe", ErrInvalidTransition)
	}
	if command.ExternalIdentity == "" && command.ObservedState != contracts.RuntimeObservedUnknown && command.ObservedState != contracts.RuntimeObservedLost {
		return Decision{}, errors.New("concrete runtime observation requires external identity")
	}
	if command.ExternalIdentity != "" {
		if err := validateIdentity(command.ExternalIdentity, "runtime external identity"); err != nil {
			return Decision{}, err
		}
	}
	expectedDigest, err := ObservationDigest(current.BindingID, current.Generation, current.FencingToken, command.ExternalIdentity, command.ObservedState, command.Health, command.Failure)
	if err != nil || expectedDigest != command.ObservationDigest {
		return Decision{}, errors.New("runtime observation digest is invalid")
	}
	if err := validateObservationFailure(command.ObservedState, command.Failure); err != nil {
		return Decision{}, err
	}
	next := cloneBinding(current)
	next.ExternalIdentity = command.ExternalIdentity
	next.ObservedState = command.ObservedState
	next.Health = command.Health
	next.ObservationDigest = command.ObservationDigest
	observedAt := command.Fence.At.UTC()
	next.ObservationAt = &observedAt
	next.PrimaryFailure = cloneFailure(command.Failure)
	next.UpdatedAt = observedAt
	next.Revision++
	if current.State == contracts.RuntimeBindingPreparing && command.ObservedState == contracts.RuntimeObservedRunning {
		next.State = contracts.RuntimeBindingActive
		activated := observedAt
		next.ActivatedAt = &activated
	}
	if command.ObservedState == contracts.RuntimeObservedLost {
		next.State = contracts.RuntimeBindingLost
		next.Health = contracts.RuntimeHealthLost
		finished := observedAt
		next.FinishedAt = &finished
		clearLease(&next)
	}
	if err := ValidateBinding(next); err != nil {
		return Decision{}, err
	}
	event, err := newEvent(next, "runtime.observed."+string(command.ObservedState), command.CommandID, command.Fence.At, map[string]any{
		"generation": next.Generation, "fencingToken": next.FencingToken, "observationDigest": next.ObservationDigest,
	})
	if err != nil {
		return Decision{}, err
	}
	return Decision{Binding: next, Events: []contracts.DomainEvent{event}}, nil
}

func Heartbeat(current contracts.RuntimeBinding, command HeartbeatCommand) (Decision, error) {
	if err := ValidateBinding(current); err != nil {
		return Decision{}, err
	}
	if err := validateFence(current, command.Fence); err != nil {
		return Decision{}, err
	}
	if !command.LeaseExpiresAt.After(current.LeaseExpiresAt) {
		return Decision{}, ErrLeaseConflict
	}
	if err := validateIdentity(command.CommandID, "runtime heartbeat command id"); err != nil {
		return Decision{}, err
	}
	next := cloneBinding(current)
	next.LeaseExpiresAt = command.LeaseExpiresAt.UTC()
	next.UpdatedAt = command.Fence.At.UTC()
	next.Revision++
	event, err := newEvent(next, "runtime.heartbeat", command.CommandID, command.Fence.At, map[string]any{
		"generation": next.Generation, "fencingToken": next.FencingToken, "leaseExpiresAt": next.LeaseExpiresAt,
	})
	if err != nil {
		return Decision{}, err
	}
	return Decision{Binding: next, Events: []contracts.DomainEvent{event}}, nil
}

func Takeover(current contracts.RuntimeBinding, command TakeoverCommand) (Decision, error) {
	if err := ValidateBinding(current); err != nil {
		return Decision{}, err
	}
	if command.BindingID != current.BindingID || command.ExpectedRevision != current.Revision {
		return Decision{}, ErrRevisionConflict
	}
	if current.State.Terminal() || command.At.IsZero() || command.At.Before(current.LeaseExpiresAt) ||
		command.Generation != current.Generation+1 || command.FencingToken <= current.FencingToken || !command.LeaseExpiresAt.After(command.At) {
		return Decision{}, ErrFenceConflict
	}
	if err := validateIdentity(command.LeaseOwner, "takeover lease owner"); err != nil {
		return Decision{}, err
	}
	if err := validateIdentity(command.CommandID, "takeover command id"); err != nil {
		return Decision{}, err
	}
	leaseHash, err := execution.PrivateTokenDigest(command.LeaseToken)
	if err != nil {
		return Decision{}, err
	}
	next := cloneBinding(current)
	next.Generation = command.Generation
	next.FencingToken = command.FencingToken
	next.LeaseOwner = command.LeaseOwner
	next.LeaseTokenHash = leaseHash
	next.LeaseExpiresAt = command.LeaseExpiresAt.UTC()
	next.ObservedState = contracts.RuntimeObservedUnknown
	next.Health = contracts.RuntimeHealthUnknown
	next.ObservationDigest = ""
	next.ObservationAt = nil
	next.PrimaryFailure = nil
	if next.State == contracts.RuntimeBindingActive {
		next.State = contracts.RuntimeBindingPreparing
	}
	next.UpdatedAt = command.At.UTC()
	next.Revision++
	payload := map[string]any{
		"bindingId": next.BindingID, "generation": next.Generation, "fencingToken": next.FencingToken,
		"externalIdentity": next.ExternalIdentity,
	}
	intent, err := newIntent(contracts.IntentReconcile, next, payload)
	if err != nil {
		return Decision{}, err
	}
	event, err := newEvent(next, "runtime.taken-over", command.CommandID, command.At, payload)
	if err != nil {
		return Decision{}, err
	}
	return Decision{Binding: next, Events: []contracts.DomainEvent{event}, Intents: []contracts.DurableIntent{intent}}, nil
}

func BeginRelease(current contracts.RuntimeBinding, command ReleaseCommand) (Decision, error) {
	if err := ValidateBinding(current); err != nil {
		return Decision{}, err
	}
	if err := validateFence(current, command.Fence); err != nil {
		return Decision{}, err
	}
	if err := validateIdentity(command.CommandID, "runtime release command id"); err != nil {
		return Decision{}, err
	}
	if current.State != contracts.RuntimeBindingActive && current.State != contracts.RuntimeBindingPreparing {
		return Decision{}, fmt.Errorf("%w: runtime %s cannot begin release", ErrInvalidTransition, current.State)
	}
	next := cloneBinding(current)
	next.UpdatedAt = command.Fence.At.UTC()
	next.Revision++
	started := command.Fence.At.UTC()
	next.ReleaseStartedAt = &started
	decision := Decision{}
	if current.Ownership == contracts.EffectOwned {
		next.State = contracts.RuntimeBindingStopping
		payload := map[string]any{
			"bindingId": next.BindingID, "generation": next.Generation, "fencingToken": next.FencingToken,
			"externalIdentity": next.ExternalIdentity,
		}
		intent, err := newIntent(contracts.IntentCleanup, next, payload)
		if err != nil {
			return Decision{}, err
		}
		decision.Intents = []contracts.DurableIntent{intent}
	} else {
		next.State = contracts.RuntimeBindingReleased
		finished := command.Fence.At.UTC()
		next.FinishedAt = &finished
		clearLease(&next)
	}
	if err := ValidateBinding(next); err != nil {
		return Decision{}, err
	}
	event, err := newEvent(next, "runtime."+string(next.State), command.CommandID, command.Fence.At, map[string]any{
		"generation": current.Generation, "fencingToken": current.FencingToken, "ownership": current.Ownership,
	})
	if err != nil {
		return Decision{}, err
	}
	decision.Binding = next
	decision.Events = []contracts.DomainEvent{event}
	return decision, nil
}

func FinishRelease(current contracts.RuntimeBinding, command ReleaseCommand) (Decision, error) {
	if err := ValidateBinding(current); err != nil {
		return Decision{}, err
	}
	if err := validateFence(current, command.Fence); err != nil {
		return Decision{}, err
	}
	if err := validateIdentity(command.CommandID, "runtime release completion command id"); err != nil {
		return Decision{}, err
	}
	if current.State != contracts.RuntimeBindingStopping || current.Ownership != contracts.EffectOwned || current.ObservedState != contracts.RuntimeObservedStopped {
		return Decision{}, fmt.Errorf("%w: owned runtime is not proven stopped", ErrInvalidTransition)
	}
	next := cloneBinding(current)
	next.State = contracts.RuntimeBindingReleased
	next.UpdatedAt = command.Fence.At.UTC()
	next.Revision++
	finished := command.Fence.At.UTC()
	next.FinishedAt = &finished
	clearLease(&next)
	if err := ValidateBinding(next); err != nil {
		return Decision{}, err
	}
	event, err := newEvent(next, "runtime.released", command.CommandID, command.Fence.At, map[string]any{
		"generation": current.Generation, "fencingToken": current.FencingToken, "observationDigest": current.ObservationDigest,
	})
	if err != nil {
		return Decision{}, err
	}
	return Decision{Binding: next, Events: []contracts.DomainEvent{event}}, nil
}

func ObservationDigest(bindingID string, generation, fencingToken uint64, externalIdentity string, state contracts.RuntimeObservedState, health contracts.RuntimeHealth, failure *contracts.StructuredFailure) (string, error) {
	return canonicaljson.DigestValue(map[string]any{
		"bindingId": bindingID, "generation": generation, "fencingToken": fencingToken,
		"externalIdentity": externalIdentity, "state": state, "health": health, "failure": failure,
	})
}
