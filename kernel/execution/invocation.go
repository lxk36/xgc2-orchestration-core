package execution

import (
	"errors"
	"fmt"
	"time"

	"github.com/lxk36/xgc2-orchestration-core/sdk/go/contracts"
)

var ErrLeaseConflict = errors.New("attempt lease conflict")

type ActivateInvocationCommand struct {
	InvocationID        string
	NamespaceID         string
	RunID               string
	NodeID              string
	TypeRef             string
	DescriptorDigest    string
	ResolvedInputDigest string
	InputRefsDigest     string
	Compensatable       bool
	CommandID           string
	At                  time.Time
}

type ClaimInvocationCommand struct {
	InvocationID     string
	ExpectedRevision uint64
	Phase            contracts.AttemptPhase
	OwnerRef         string
	LeaseToken       string
	LeaseExpiresAt   time.Time
	CommandID        string
	At               time.Time
}

type AttemptFence struct {
	InvocationID       string
	InvocationRevision uint64
	AttemptID          string
	AttemptRevision    uint64
	LeaseToken         string
	At                 time.Time
}

type TransitionInvocationCommand struct {
	Fence            AttemptFence
	To               contracts.InvocationStatus
	AttemptTo        contracts.AttemptStatus
	OutputRefsDigest string
	Failure          *contracts.StructuredFailure
	RetryAt          *time.Time
	WaitRef          string
	WaitGeneration   uint32
	CommandID        string
}

type HeartbeatAttemptCommand struct {
	Fence          AttemptFence
	OwnerRef       string
	LeaseExpiresAt time.Time
	CommandID      string
}

type ResolveUnleasedInvocationCommand struct {
	InvocationID     string
	ExpectedRevision uint64
	To               contracts.InvocationStatus
	Failure          *contracts.StructuredFailure
	CommandID        string
	At               time.Time
}

type ScheduleInvocationCompensationCommand struct {
	InvocationID     string
	ExpectedRevision uint64
	CommandID        string
	At               time.Time
}

type ClaimCompensationCommand struct {
	InvocationID     string
	ExpectedRevision uint64
	OwnerRef         string
	LeaseToken       string
	LeaseExpiresAt   time.Time
	CommandID        string
	At               time.Time
}

type TransitionInvocationCompensationCommand struct {
	Fence     AttemptFence
	To        contracts.CompensationStatus
	AttemptTo contracts.AttemptStatus
	Failure   *contracts.StructuredFailure
	RetryAt   *time.Time
	CommandID string
}

type InvocationDecision struct {
	Ledger  contracts.InvocationLedger
	Events  []contracts.DomainEvent
	Intents []contracts.DurableIntent
}

func ActivateInvocation(command ActivateInvocationCommand) (InvocationDecision, error) {
	if err := validateActivateInvocation(command); err != nil {
		return InvocationDecision{}, err
	}
	expectedID, _ := StableInvocationID(command.RunID, command.NodeID)
	if command.InvocationID != expectedID {
		return InvocationDecision{}, errors.New("invocation identity does not match run and node")
	}
	compensation := contracts.CompensationNotRequired
	if command.Compensatable {
		compensation = contracts.CompensationUnscheduled
	}
	invocation := contracts.Invocation{
		InvocationID: command.InvocationID, NamespaceID: command.NamespaceID, RunID: command.RunID,
		NodeID: command.NodeID, TypeRef: command.TypeRef, DescriptorDigest: command.DescriptorDigest,
		ResolvedInputDigest: command.ResolvedInputDigest, InputRefsDigest: command.InputRefsDigest,
		Status: contracts.InvocationReady, CompensationStatus: compensation,
		CreatedAt: command.At.UTC(), UpdatedAt: command.At.UTC(), Revision: 1,
	}
	ledger := contracts.InvocationLedger{Invocation: invocation, Attempts: []contracts.Attempt{}}
	if err := ValidateInvocationLedger(ledger); err != nil {
		return InvocationDecision{}, err
	}
	event, err := newEvent("invocation", invocation.InvocationID, 1, "invocation.ready", command.CommandID, command.At, map[string]any{
		"runId": invocation.RunID, "nodeId": invocation.NodeID, "descriptorDigest": invocation.DescriptorDigest,
		"resolvedInputDigest": invocation.ResolvedInputDigest,
	})
	if err != nil {
		return InvocationDecision{}, err
	}
	return InvocationDecision{Ledger: ledger, Events: []contracts.DomainEvent{event}}, nil
}

func ClaimInvocation(current contracts.InvocationLedger, command ClaimInvocationCommand) (InvocationDecision, error) {
	if err := ValidateInvocationLedger(current); err != nil {
		return InvocationDecision{}, fmt.Errorf("current invocation ledger: %w", err)
	}
	invocation := current.Invocation
	if command.InvocationID != invocation.InvocationID {
		return InvocationDecision{}, errors.New("claim targets another invocation")
	}
	if command.ExpectedRevision != invocation.Revision {
		return InvocationDecision{}, ErrRevisionConflict
	}
	if command.At.IsZero() || command.At.Before(invocation.UpdatedAt) || !command.LeaseExpiresAt.After(command.At) {
		return InvocationDecision{}, errors.New("claim times are invalid")
	}
	if err := validateIdentity(command.OwnerRef, "attempt owner ref"); err != nil {
		return InvocationDecision{}, err
	}
	if err := validateCanonicalText(command.LeaseToken, "attempt lease token", 512, true); err != nil {
		return InvocationDecision{}, err
	}
	if err := validateIdentity(command.CommandID, "claim command id"); err != nil {
		return InvocationDecision{}, err
	}
	if command.Phase != contracts.AttemptExecution {
		return InvocationDecision{}, errors.New("execution claim requires execution attempt phase")
	}
	if invocation.Status != contracts.InvocationReady && invocation.Status != contracts.InvocationRetryWait {
		return InvocationDecision{}, fmt.Errorf("%w: invocation %s cannot be claimed", ErrInvalidTransition, invocation.Status)
	}
	if invocation.Status == contracts.InvocationRetryWait && (invocation.NextAttemptAt == nil || command.At.Before(*invocation.NextAttemptAt)) {
		return InvocationDecision{}, fmt.Errorf("%w: retry is not due", ErrInvalidTransition)
	}
	ordinal := invocation.ExecutionAttemptCount + 1
	attemptID, _ := StableAttemptID(invocation.InvocationID, command.Phase, ordinal)
	leaseHash, err := PrivateTokenDigest(command.LeaseToken)
	if err != nil {
		return InvocationDecision{}, err
	}
	attempt := contracts.Attempt{
		AttemptID: attemptID, NamespaceID: invocation.NamespaceID, RunID: invocation.RunID,
		InvocationID: invocation.InvocationID, Phase: command.Phase, Ordinal: ordinal,
		Status: contracts.AttemptRunning, ResolvedInputDigest: invocation.ResolvedInputDigest,
		OwnerRef: command.OwnerRef, LeaseTokenHash: leaseHash,
		LeaseExpiresAt: command.LeaseExpiresAt.UTC(), CreatedAt: command.At.UTC(), StartedAt: command.At.UTC(),
		UpdatedAt: command.At.UTC(), Revision: 1,
	}
	next := cloneLedger(current)
	next.Attempts = append(next.Attempts, attempt)
	next.Invocation.Status = contracts.InvocationRunning
	next.Invocation.ActiveAttemptID = attemptID
	next.Invocation.ExecutionAttemptCount = ordinal
	next.Invocation.NextAttemptAt = nil
	next.Invocation.PrimaryFailure = nil
	next.Invocation.Revision++
	next.Invocation.UpdatedAt = command.At.UTC()
	if next.Invocation.StartedAt == nil {
		started := command.At.UTC()
		next.Invocation.StartedAt = &started
	}
	if err := ValidateInvocationLedger(next); err != nil {
		return InvocationDecision{}, err
	}
	event, err := newEvent("invocation", invocation.InvocationID, next.Invocation.Revision, "invocation.claimed", command.CommandID, command.At, map[string]any{
		"attemptId": attemptID, "attemptOrdinal": ordinal, "ownerRef": command.OwnerRef,
		"leaseTokenHash": leaseHash, "leaseExpiresAt": command.LeaseExpiresAt.UTC(),
	})
	if err != nil {
		return InvocationDecision{}, err
	}
	return InvocationDecision{Ledger: next, Events: []contracts.DomainEvent{event}}, nil
}

func TransitionInvocation(current contracts.InvocationLedger, command TransitionInvocationCommand) (InvocationDecision, error) {
	if err := ValidateInvocationLedger(current); err != nil {
		return InvocationDecision{}, fmt.Errorf("current invocation ledger: %w", err)
	}
	index, err := validateAttemptFenceForPhase(current, command.Fence, contracts.AttemptExecution)
	if err != nil {
		return InvocationDecision{}, err
	}
	if err := validateIdentity(command.CommandID, "invocation transition command id"); err != nil {
		return InvocationDecision{}, err
	}
	invocation := current.Invocation
	attempt := current.Attempts[index]
	if err := ValidateInvocationTransition(invocation.Status, command.To); err != nil {
		return InvocationDecision{}, err
	}
	if err := ValidateAttemptTransition(attempt.Status, command.AttemptTo); err != nil {
		return InvocationDecision{}, err
	}
	if err := validateAlignedTransition(command); err != nil {
		return InvocationDecision{}, err
	}

	next := cloneLedger(current)
	nextAttempt := &next.Attempts[index]
	nextAttempt.Status = command.AttemptTo
	nextAttempt.Revision++
	nextAttempt.UpdatedAt = command.Fence.At.UTC()
	if command.AttemptTo.Terminal() {
		finished := command.Fence.At.UTC()
		nextAttempt.FinishedAt = &finished
		nextAttempt.LeaseExpiresAt = time.Time{}
	}
	nextAttempt.Failure = cloneFailure(command.Failure)

	nextInvocation := &next.Invocation
	nextInvocation.Status = command.To
	nextInvocation.Revision++
	nextInvocation.UpdatedAt = command.Fence.At.UTC()
	nextInvocation.CurrentWaitRef = ""
	nextInvocation.NextAttemptAt = nil
	if command.To == contracts.InvocationWaiting {
		nextInvocation.CurrentWaitRef = command.WaitRef
		nextInvocation.WaitGeneration = command.WaitGeneration
	}
	if command.To == contracts.InvocationRetryWait {
		nextInvocation.NextAttemptAt = cloneTime(command.RetryAt)
		nextInvocation.ActiveAttemptID = ""
	}
	if command.To == contracts.InvocationSucceeded {
		nextInvocation.OutputRefsDigest = command.OutputRefsDigest
	}
	if command.To == contracts.InvocationFailed || command.To == contracts.InvocationRetryWait {
		nextInvocation.PrimaryFailure = cloneFailure(command.Failure)
	}
	if command.To.Terminal() {
		nextInvocation.ActiveAttemptID = ""
		finished := command.Fence.At.UTC()
		nextInvocation.FinishedAt = &finished
	}
	if err := ValidateInvocationLedger(next); err != nil {
		return InvocationDecision{}, err
	}
	event, err := newEvent("invocation", invocation.InvocationID, nextInvocation.Revision, "invocation."+string(command.To), command.CommandID, command.Fence.At, map[string]any{
		"from": invocation.Status, "to": command.To, "attemptId": attempt.AttemptID,
		"attemptFrom": attempt.Status, "attemptTo": command.AttemptTo,
	})
	if err != nil {
		return InvocationDecision{}, err
	}
	return InvocationDecision{Ledger: next, Events: []contracts.DomainEvent{event}}, nil
}

func HeartbeatAttempt(current contracts.InvocationLedger, command HeartbeatAttemptCommand) (InvocationDecision, error) {
	if err := ValidateInvocationLedger(current); err != nil {
		return InvocationDecision{}, err
	}
	index, err := validateAttemptFenceAny(current, command.Fence)
	if err != nil {
		return InvocationDecision{}, err
	}
	if current.Attempts[index].Status != contracts.AttemptRunning && current.Attempts[index].Status != contracts.AttemptWaiting {
		return InvocationDecision{}, ErrLeaseConflict
	}
	if command.OwnerRef != current.Attempts[index].OwnerRef || !command.LeaseExpiresAt.After(current.Attempts[index].LeaseExpiresAt) {
		return InvocationDecision{}, ErrLeaseConflict
	}
	if err := validateIdentity(command.CommandID, "heartbeat command id"); err != nil {
		return InvocationDecision{}, err
	}
	next := cloneLedger(current)
	next.Attempts[index].LeaseExpiresAt = command.LeaseExpiresAt.UTC()
	next.Attempts[index].UpdatedAt = command.Fence.At.UTC()
	next.Attempts[index].Revision++
	next.Invocation.UpdatedAt = command.Fence.At.UTC()
	next.Invocation.Revision++
	event, err := newEvent("invocation", next.Invocation.InvocationID, next.Invocation.Revision, "attempt.heartbeat", command.CommandID, command.Fence.At, map[string]any{
		"attemptId": next.Attempts[index].AttemptID, "attemptRevision": next.Attempts[index].Revision,
		"leaseExpiresAt": next.Attempts[index].LeaseExpiresAt,
	})
	if err != nil {
		return InvocationDecision{}, err
	}
	return InvocationDecision{Ledger: next, Events: []contracts.DomainEvent{event}}, nil
}

// ResolveUnleasedInvocation closes states that deliberately have no active
// Attempt. Leased running/waiting state must use TransitionInvocation.
func ResolveUnleasedInvocation(current contracts.InvocationLedger, command ResolveUnleasedInvocationCommand) (InvocationDecision, error) {
	if err := ValidateInvocationLedger(current); err != nil {
		return InvocationDecision{}, err
	}
	invocation := current.Invocation
	if command.InvocationID != invocation.InvocationID || command.ExpectedRevision != invocation.Revision {
		return InvocationDecision{}, ErrRevisionConflict
	}
	if command.At.IsZero() || command.At.Before(invocation.UpdatedAt) {
		return InvocationDecision{}, errors.New("unleased resolution time is missing or moves backwards")
	}
	if err := validateIdentity(command.CommandID, "unleased resolution command id"); err != nil {
		return InvocationDecision{}, err
	}
	allowed := (invocation.Status == contracts.InvocationReady && (command.To == contracts.InvocationSkipped || command.To == contracts.InvocationCanceled)) ||
		(invocation.Status == contracts.InvocationRetryWait && (command.To == contracts.InvocationFailed || command.To == contracts.InvocationCanceled))
	if !allowed {
		return InvocationDecision{}, fmt.Errorf("%w: unleased invocation %s -> %s", ErrInvalidTransition, invocation.Status, command.To)
	}
	if err := validateFailure(command.Failure, command.To == contracts.InvocationFailed, "unleased invocation failure"); err != nil {
		return InvocationDecision{}, err
	}
	if command.To != contracts.InvocationFailed && command.Failure != nil {
		return InvocationDecision{}, errors.New("only failed unleased invocation can carry failure")
	}
	next := cloneLedger(current)
	next.Invocation.Status = command.To
	next.Invocation.ActiveAttemptID = ""
	next.Invocation.NextAttemptAt = nil
	next.Invocation.PrimaryFailure = cloneFailure(command.Failure)
	if command.To == contracts.InvocationSkipped && next.Invocation.CompensationStatus == contracts.CompensationUnscheduled {
		next.Invocation.CompensationStatus = contracts.CompensationNotRequired
	}
	next.Invocation.UpdatedAt = command.At.UTC()
	next.Invocation.Revision++
	finished := command.At.UTC()
	next.Invocation.FinishedAt = &finished
	if err := ValidateInvocationLedger(next); err != nil {
		return InvocationDecision{}, err
	}
	event, err := newEvent("invocation", next.Invocation.InvocationID, next.Invocation.Revision, "invocation."+string(command.To), command.CommandID, command.At, map[string]any{
		"from": invocation.Status, "to": command.To, "leased": false,
	})
	if err != nil {
		return InvocationDecision{}, err
	}
	return InvocationDecision{Ledger: next, Events: []contracts.DomainEvent{event}}, nil
}

func ScheduleInvocationCompensation(current contracts.InvocationLedger, command ScheduleInvocationCompensationCommand) (InvocationDecision, error) {
	if err := ValidateInvocationLedger(current); err != nil {
		return InvocationDecision{}, err
	}
	invocation := current.Invocation
	if command.InvocationID != invocation.InvocationID || command.ExpectedRevision != invocation.Revision {
		return InvocationDecision{}, ErrRevisionConflict
	}
	if !invocation.Status.Terminal() || invocation.Status == contracts.InvocationSkipped || invocation.CompensationStatus != contracts.CompensationUnscheduled {
		return InvocationDecision{}, fmt.Errorf("%w: invocation cannot schedule compensation", ErrInvalidTransition)
	}
	if command.At.IsZero() || command.At.Before(invocation.UpdatedAt) {
		return InvocationDecision{}, errors.New("compensation scheduling time is missing or moves backwards")
	}
	if err := validateIdentity(command.CommandID, "compensation scheduling command id"); err != nil {
		return InvocationDecision{}, err
	}
	next := cloneLedger(current)
	next.Invocation.CompensationStatus = contracts.CompensationReady
	next.Invocation.UpdatedAt = command.At.UTC()
	next.Invocation.Revision++
	payload := map[string]any{"invocationId": next.Invocation.InvocationID, "invocationRevision": next.Invocation.Revision}
	intent, err := newDurableIntent(contracts.IntentCleanup, next.Invocation.InvocationID, next.Invocation.Revision, payload)
	if err != nil {
		return InvocationDecision{}, err
	}
	event, err := newEvent("invocation", next.Invocation.InvocationID, next.Invocation.Revision, "invocation.compensation.ready", command.CommandID, command.At, payload)
	if err != nil {
		return InvocationDecision{}, err
	}
	return InvocationDecision{Ledger: next, Events: []contracts.DomainEvent{event}, Intents: []contracts.DurableIntent{intent}}, nil
}

func ClaimCompensation(current contracts.InvocationLedger, command ClaimCompensationCommand) (InvocationDecision, error) {
	if err := ValidateInvocationLedger(current); err != nil {
		return InvocationDecision{}, err
	}
	invocation := current.Invocation
	if command.InvocationID != invocation.InvocationID || command.ExpectedRevision != invocation.Revision {
		return InvocationDecision{}, ErrRevisionConflict
	}
	if invocation.CompensationStatus != contracts.CompensationReady && invocation.CompensationStatus != contracts.CompensationRetryWait {
		return InvocationDecision{}, fmt.Errorf("%w: compensation %s cannot be claimed", ErrInvalidTransition, invocation.CompensationStatus)
	}
	if invocation.CompensationStatus == contracts.CompensationRetryWait && (invocation.CompensationNextAt == nil || command.At.Before(*invocation.CompensationNextAt)) {
		return InvocationDecision{}, fmt.Errorf("%w: compensation retry is not due", ErrInvalidTransition)
	}
	if command.At.IsZero() || command.At.Before(invocation.UpdatedAt) || !command.LeaseExpiresAt.After(command.At) {
		return InvocationDecision{}, errors.New("compensation claim times are invalid")
	}
	if err := validateIdentity(command.OwnerRef, "compensation owner ref"); err != nil {
		return InvocationDecision{}, err
	}
	if err := validateIdentity(command.CommandID, "compensation claim command id"); err != nil {
		return InvocationDecision{}, err
	}
	leaseHash, err := PrivateTokenDigest(command.LeaseToken)
	if err != nil {
		return InvocationDecision{}, err
	}
	ordinal := invocation.CompensationAttemptCount + 1
	attemptID, _ := StableAttemptID(invocation.InvocationID, contracts.AttemptCompensation, ordinal)
	attempt := contracts.Attempt{
		AttemptID: attemptID, NamespaceID: invocation.NamespaceID, RunID: invocation.RunID,
		InvocationID: invocation.InvocationID, Phase: contracts.AttemptCompensation, Ordinal: ordinal,
		Status: contracts.AttemptRunning, ResolvedInputDigest: invocation.ResolvedInputDigest,
		OwnerRef: command.OwnerRef, LeaseTokenHash: leaseHash, LeaseExpiresAt: command.LeaseExpiresAt.UTC(),
		CreatedAt: command.At.UTC(), StartedAt: command.At.UTC(), UpdatedAt: command.At.UTC(), Revision: 1,
	}
	next := cloneLedger(current)
	next.Attempts = append(next.Attempts, attempt)
	next.Invocation.CompensationStatus = contracts.CompensationRunning
	next.Invocation.ActiveCompensationAttemptID = attemptID
	next.Invocation.CompensationAttemptCount = ordinal
	next.Invocation.CompensationFailure = nil
	next.Invocation.CompensationNextAt = nil
	next.Invocation.UpdatedAt = command.At.UTC()
	next.Invocation.Revision++
	if err := ValidateInvocationLedger(next); err != nil {
		return InvocationDecision{}, err
	}
	event, err := newEvent("invocation", next.Invocation.InvocationID, next.Invocation.Revision, "invocation.compensation.claimed", command.CommandID, command.At, map[string]any{
		"attemptId": attemptID, "attemptOrdinal": ordinal, "ownerRef": command.OwnerRef,
		"leaseTokenHash": leaseHash, "leaseExpiresAt": command.LeaseExpiresAt.UTC(),
	})
	if err != nil {
		return InvocationDecision{}, err
	}
	return InvocationDecision{Ledger: next, Events: []contracts.DomainEvent{event}}, nil
}

func TransitionInvocationCompensation(current contracts.InvocationLedger, command TransitionInvocationCompensationCommand) (InvocationDecision, error) {
	if err := ValidateInvocationLedger(current); err != nil {
		return InvocationDecision{}, err
	}
	index, err := validateAttemptFenceForPhase(current, command.Fence, contracts.AttemptCompensation)
	if err != nil {
		return InvocationDecision{}, err
	}
	if err := validateIdentity(command.CommandID, "compensation transition command id"); err != nil {
		return InvocationDecision{}, err
	}
	if err := validateInvocationCompensationTransition(current.Invocation.CompensationStatus, current.Attempts[index].Status, command); err != nil {
		return InvocationDecision{}, err
	}
	next := cloneLedger(current)
	attempt := &next.Attempts[index]
	attempt.Status = command.AttemptTo
	attempt.Failure = cloneFailure(command.Failure)
	attempt.UpdatedAt = command.Fence.At.UTC()
	attempt.Revision++
	finished := command.Fence.At.UTC()
	attempt.FinishedAt = &finished
	attempt.LeaseExpiresAt = time.Time{}
	next.Invocation.CompensationStatus = command.To
	next.Invocation.ActiveCompensationAttemptID = ""
	next.Invocation.CompensationFailure = cloneFailure(command.Failure)
	next.Invocation.CompensationNextAt = cloneTime(command.RetryAt)
	next.Invocation.UpdatedAt = command.Fence.At.UTC()
	next.Invocation.Revision++
	if err := ValidateInvocationLedger(next); err != nil {
		return InvocationDecision{}, err
	}
	event, err := newEvent("invocation", next.Invocation.InvocationID, next.Invocation.Revision, "invocation.compensation."+string(command.To), command.CommandID, command.Fence.At, map[string]any{
		"attemptId": attempt.AttemptID, "attemptStatus": attempt.Status, "compensationStatus": command.To,
	})
	if err != nil {
		return InvocationDecision{}, err
	}
	decision := InvocationDecision{Ledger: next, Events: []contracts.DomainEvent{event}}
	if command.To == contracts.CompensationRetryWait {
		payload := map[string]any{"invocationId": next.Invocation.InvocationID, "retryAt": command.RetryAt.UTC(), "invocationRevision": next.Invocation.Revision}
		intent, err := newDurableIntent(contracts.IntentCleanup, next.Invocation.InvocationID, next.Invocation.Revision, payload)
		if err != nil {
			return InvocationDecision{}, err
		}
		decision.Intents = []contracts.DurableIntent{intent}
	}
	return decision, nil
}

func ValidateInvocationTransition(from, to contracts.InvocationStatus) error {
	if !from.Valid() || !to.Valid() || from == to {
		return fmt.Errorf("%w: invocation %q -> %q", ErrInvalidTransition, from, to)
	}
	allowed := map[contracts.InvocationStatus]map[contracts.InvocationStatus]bool{
		contracts.InvocationReady:     {contracts.InvocationRunning: true, contracts.InvocationSkipped: true, contracts.InvocationCanceled: true},
		contracts.InvocationRunning:   {contracts.InvocationWaiting: true, contracts.InvocationRetryWait: true, contracts.InvocationSucceeded: true, contracts.InvocationFailed: true, contracts.InvocationCanceled: true},
		contracts.InvocationWaiting:   {contracts.InvocationRunning: true, contracts.InvocationRetryWait: true, contracts.InvocationSucceeded: true, contracts.InvocationFailed: true, contracts.InvocationCanceled: true},
		contracts.InvocationRetryWait: {contracts.InvocationRunning: true, contracts.InvocationFailed: true, contracts.InvocationCanceled: true},
	}
	if !allowed[from][to] {
		return fmt.Errorf("%w: invocation %q -> %q", ErrInvalidTransition, from, to)
	}
	return nil
}

func ValidateAttemptTransition(from, to contracts.AttemptStatus) error {
	if !from.Valid() || !to.Valid() || from == to {
		return fmt.Errorf("%w: attempt %q -> %q", ErrInvalidTransition, from, to)
	}
	allowed := map[contracts.AttemptStatus]map[contracts.AttemptStatus]bool{
		contracts.AttemptRunning: {contracts.AttemptWaiting: true, contracts.AttemptSucceeded: true, contracts.AttemptFailed: true, contracts.AttemptCanceled: true, contracts.AttemptAbandoned: true},
		contracts.AttemptWaiting: {contracts.AttemptRunning: true, contracts.AttemptSucceeded: true, contracts.AttemptFailed: true, contracts.AttemptCanceled: true, contracts.AttemptAbandoned: true},
	}
	if !allowed[from][to] {
		return fmt.Errorf("%w: attempt %q -> %q", ErrInvalidTransition, from, to)
	}
	return nil
}

func ValidateInvocationLedger(ledger contracts.InvocationLedger) error {
	invocation := ledger.Invocation
	for label, value := range map[string]string{
		"invocation id": invocation.InvocationID, "namespace id": invocation.NamespaceID,
		"run id": invocation.RunID, "node id": invocation.NodeID,
	} {
		if err := validateIdentity(value, label); err != nil {
			return err
		}
	}
	expectedID, err := StableInvocationID(invocation.RunID, invocation.NodeID)
	if err != nil || expectedID != invocation.InvocationID {
		return errors.New("invocation stable identity is invalid")
	}
	if !contracts.ValidTypeRef(invocation.TypeRef) || !contracts.ValidDigest(invocation.DescriptorDigest) ||
		!contracts.ValidDigest(invocation.ResolvedInputDigest) || !contracts.ValidDigest(invocation.InputRefsDigest) {
		return errors.New("invocation type or frozen digest is invalid")
	}
	if !invocation.Status.Valid() || !invocation.CompensationStatus.Valid() || invocation.Revision == 0 ||
		invocation.CreatedAt.IsZero() || invocation.UpdatedAt.Before(invocation.CreatedAt) {
		return errors.New("invocation lifecycle is invalid")
	}
	if invocation.Status.Terminal() != (invocation.FinishedAt != nil) {
		return errors.New("invocation terminal state and finishedAt disagree")
	}
	if (invocation.Status == contracts.InvocationRunning || invocation.Status == contracts.InvocationWaiting) && invocation.ActiveAttemptID == "" {
		return errors.New("active invocation requires an active attempt")
	}
	if invocation.Status != contracts.InvocationRunning && invocation.Status != contracts.InvocationWaiting && invocation.ActiveAttemptID != "" {
		return errors.New("inactive invocation retains an execution attempt pointer")
	}
	if invocation.Status == contracts.InvocationRetryWait && (invocation.ActiveAttemptID != "" || invocation.NextAttemptAt == nil) {
		return errors.New("retry-wait invocation has invalid active attempt or time")
	}
	if invocation.Status == contracts.InvocationWaiting && (invocation.CurrentWaitRef == "" || invocation.WaitGeneration == 0) {
		return errors.New("waiting invocation requires a durable wait generation")
	}
	if invocation.Status == contracts.InvocationSucceeded && !contracts.ValidDigest(invocation.OutputRefsDigest) {
		return errors.New("succeeded invocation requires output refs digest")
	}
	if err := validateFailure(invocation.PrimaryFailure, invocation.Status == contracts.InvocationFailed || invocation.Status == contracts.InvocationRetryWait, "invocation primary failure"); err != nil {
		return err
	}
	if invocation.Status != contracts.InvocationFailed && invocation.Status != contracts.InvocationRetryWait && invocation.PrimaryFailure != nil {
		return errors.New("invocation primary failure exists outside failed or retry-wait")
	}
	if invocation.ExecutionAttemptCount != uint32(countPhase(ledger.Attempts, contracts.AttemptExecution)) {
		return errors.New("invocation execution attempt count disagrees with ledger")
	}
	if invocation.CompensationAttemptCount != uint32(countPhase(ledger.Attempts, contracts.AttemptCompensation)) {
		return errors.New("invocation compensation attempt count disagrees with ledger")
	}
	if err := validateInvocationCompensation(invocation); err != nil {
		return err
	}
	seen := make(map[string]bool, len(ledger.Attempts))
	ordinals := map[contracts.AttemptPhase]uint32{contracts.AttemptExecution: 0, contracts.AttemptCompensation: 0}
	for index := range ledger.Attempts {
		attempt := ledger.Attempts[index]
		if err := validateAttempt(attempt, invocation); err != nil {
			return fmt.Errorf("attempt %d: %w", index, err)
		}
		if seen[attempt.AttemptID] {
			return errors.New("attempt identity is duplicated")
		}
		ordinals[attempt.Phase]++
		if attempt.Ordinal != ordinals[attempt.Phase] {
			return errors.New("attempt ordinals are not contiguous in ledger order")
		}
		seen[attempt.AttemptID] = true
	}
	if invocation.ActiveAttemptID != "" {
		active, exists := findAttempt(ledger.Attempts, invocation.ActiveAttemptID)
		if !exists || active.Phase != contracts.AttemptExecution || active.Status.Terminal() {
			return errors.New("active attempt pointer is missing or terminal")
		}
	}
	if invocation.ActiveCompensationAttemptID != "" {
		active, exists := findAttempt(ledger.Attempts, invocation.ActiveCompensationAttemptID)
		if !exists || active.Phase != contracts.AttemptCompensation || active.Status.Terminal() {
			return errors.New("active compensation attempt pointer is missing or terminal")
		}
	}
	return nil
}

func validateInvocationCompensation(invocation contracts.Invocation) error {
	status := invocation.CompensationStatus
	active := invocation.ActiveCompensationAttemptID
	failure := invocation.CompensationFailure
	nextAt := invocation.CompensationNextAt
	if status == contracts.CompensationNotRequired {
		if invocation.CompensationAttemptCount != 0 || active != "" || failure != nil || nextAt != nil {
			return errors.New("not-required compensation contains lifecycle state")
		}
		return nil
	}
	if status != contracts.CompensationUnscheduled && !invocation.Status.Terminal() {
		return errors.New("compensation cannot start before primary invocation is terminal")
	}
	if status == contracts.CompensationRunning {
		if active == "" || failure != nil || nextAt != nil {
			return errors.New("running compensation has invalid active attempt, failure, or retry time")
		}
		return nil
	}
	if active != "" {
		return errors.New("inactive compensation retains an attempt pointer")
	}
	switch status {
	case contracts.CompensationRetryWait:
		if nextAt == nil || failure == nil || failure.Class != contracts.FailureTransient {
			return errors.New("compensation retry-wait requires transient failure and next time")
		}
	case contracts.CompensationFailed:
		if nextAt != nil || failure == nil {
			return errors.New("failed compensation requires failure and no retry time")
		}
	case contracts.CompensationUnscheduled, contracts.CompensationReady, contracts.CompensationSucceeded, contracts.CompensationCanceled:
		if nextAt != nil || failure != nil {
			return errors.New("compensation state retains failure or retry time")
		}
	default:
		return errors.New("compensation state is invalid")
	}
	return nil
}

func validateAttempt(attempt contracts.Attempt, invocation contracts.Invocation) error {
	if attempt.InvocationID != invocation.InvocationID || attempt.RunID != invocation.RunID || attempt.NamespaceID != invocation.NamespaceID {
		return errors.New("attempt belongs to another invocation")
	}
	if !attempt.Phase.Valid() || !attempt.Status.Valid() || attempt.Ordinal == 0 || attempt.Revision == 0 {
		return errors.New("attempt phase, status, ordinal, or revision is invalid")
	}
	expectedID, _ := StableAttemptID(invocation.InvocationID, attempt.Phase, attempt.Ordinal)
	if attempt.AttemptID != expectedID || attempt.ResolvedInputDigest != invocation.ResolvedInputDigest {
		return errors.New("attempt stable identity or resolved input digest is invalid")
	}
	if !contracts.ValidDigest(attempt.LeaseTokenHash) || attempt.CreatedAt.IsZero() || attempt.StartedAt.IsZero() || attempt.UpdatedAt.Before(attempt.CreatedAt) {
		return errors.New("attempt lease hash or timestamps are invalid")
	}
	if attempt.Status.Terminal() != (attempt.FinishedAt != nil) {
		return errors.New("attempt terminal state and finishedAt disagree")
	}
	if attempt.Status.Terminal() {
		if !attempt.LeaseExpiresAt.IsZero() {
			return errors.New("terminal attempt retains a live lease")
		}
	} else if !attempt.LeaseExpiresAt.After(attempt.UpdatedAt) {
		return errors.New("live attempt requires an unexpired lease")
	}
	return validateFailure(attempt.Failure, attempt.Status == contracts.AttemptFailed || attempt.Status == contracts.AttemptAbandoned, "attempt failure")
}

func validateActivateInvocation(command ActivateInvocationCommand) error {
	for label, value := range map[string]string{
		"invocation id": command.InvocationID, "namespace id": command.NamespaceID,
		"run id": command.RunID, "node id": command.NodeID, "command id": command.CommandID,
	} {
		if err := validateIdentity(value, label); err != nil {
			return err
		}
	}
	if !contracts.ValidTypeRef(command.TypeRef) || !contracts.ValidDigest(command.DescriptorDigest) ||
		!contracts.ValidDigest(command.ResolvedInputDigest) || !contracts.ValidDigest(command.InputRefsDigest) {
		return errors.New("invocation activation contains invalid type or digest")
	}
	if command.At.IsZero() {
		return errors.New("invocation activation time is required")
	}
	return nil
}

func validateAttemptFenceForPhase(ledger contracts.InvocationLedger, fence AttemptFence, phase contracts.AttemptPhase) (int, error) {
	index, err := validateAttemptFenceAny(ledger, fence)
	if err != nil {
		return -1, err
	}
	if ledger.Attempts[index].Phase != phase {
		return -1, ErrLeaseConflict
	}
	return index, nil
}

func validateAttemptFenceAny(ledger contracts.InvocationLedger, fence AttemptFence) (int, error) {
	if fence.InvocationID != ledger.Invocation.InvocationID || fence.InvocationRevision != ledger.Invocation.Revision {
		return -1, ErrRevisionConflict
	}
	if fence.At.IsZero() || fence.At.Before(ledger.Invocation.UpdatedAt) {
		return -1, ErrLeaseConflict
	}
	for index := range ledger.Attempts {
		attempt := ledger.Attempts[index]
		if attempt.AttemptID != fence.AttemptID {
			continue
		}
		leaseHash, err := PrivateTokenDigest(fence.LeaseToken)
		if err != nil || attempt.Revision != fence.AttemptRevision || attempt.LeaseTokenHash != leaseHash ||
			!fence.At.Before(attempt.LeaseExpiresAt) {
			return -1, ErrLeaseConflict
		}
		activeID := ledger.Invocation.ActiveAttemptID
		if attempt.Phase == contracts.AttemptCompensation {
			activeID = ledger.Invocation.ActiveCompensationAttemptID
		}
		if activeID != attempt.AttemptID {
			return -1, ErrLeaseConflict
		}
		return index, nil
	}
	return -1, ErrLeaseConflict
}

func validateAlignedTransition(command TransitionInvocationCommand) error {
	switch command.To {
	case contracts.InvocationWaiting:
		if command.AttemptTo != contracts.AttemptWaiting || command.WaitRef == "" || command.WaitGeneration == 0 {
			return errors.New("waiting transition requires waiting attempt and durable wait generation")
		}
	case contracts.InvocationRunning:
		if command.AttemptTo != contracts.AttemptRunning {
			return errors.New("running invocation requires running attempt")
		}
	case contracts.InvocationSucceeded:
		if command.AttemptTo != contracts.AttemptSucceeded || !contracts.ValidDigest(command.OutputRefsDigest) || command.Failure != nil {
			return errors.New("success transition requires succeeded attempt and output refs digest")
		}
	case contracts.InvocationRetryWait:
		if command.AttemptTo != contracts.AttemptFailed || command.RetryAt == nil || !command.RetryAt.After(command.Fence.At) {
			return errors.New("retry transition requires failed attempt and future retry time")
		}
		if err := validateFailure(command.Failure, true, "retry failure"); err != nil {
			return err
		}
	case contracts.InvocationFailed:
		if command.AttemptTo != contracts.AttemptFailed && command.AttemptTo != contracts.AttemptAbandoned {
			return errors.New("failed invocation requires failed or abandoned attempt")
		}
		if err := validateFailure(command.Failure, true, "invocation failure"); err != nil {
			return err
		}
	case contracts.InvocationCanceled:
		if command.AttemptTo != contracts.AttemptCanceled && command.AttemptTo != contracts.AttemptAbandoned {
			return errors.New("canceled invocation requires canceled or abandoned attempt")
		}
	default:
		return errors.New("leased attempt cannot perform this invocation transition")
	}
	if command.To != contracts.InvocationWaiting && (command.WaitRef != "" || command.WaitGeneration != 0) {
		return errors.New("wait identity is only valid for waiting transition")
	}
	if command.To != contracts.InvocationRetryWait && command.RetryAt != nil {
		return errors.New("retry time is only valid for retry-wait transition")
	}
	return nil
}

func validateInvocationCompensationTransition(from contracts.CompensationStatus, attemptFrom contracts.AttemptStatus, command TransitionInvocationCompensationCommand) error {
	if from != contracts.CompensationRunning {
		return fmt.Errorf("%w: compensation %s cannot transition", ErrInvalidTransition, from)
	}
	if err := ValidateAttemptTransition(attemptFrom, command.AttemptTo); err != nil {
		return err
	}
	switch command.To {
	case contracts.CompensationSucceeded:
		if command.AttemptTo != contracts.AttemptSucceeded || command.Failure != nil || command.RetryAt != nil {
			return errors.New("successful compensation requires a succeeded attempt")
		}
	case contracts.CompensationRetryWait:
		if command.AttemptTo != contracts.AttemptFailed || command.RetryAt == nil || !command.RetryAt.After(command.Fence.At) {
			return errors.New("compensation retry requires failed attempt and future retry time")
		}
		if err := validateFailure(command.Failure, true, "compensation retry failure"); err != nil || command.Failure.Class != contracts.FailureTransient {
			return errors.New("compensation retry requires a transient failure")
		}
	case contracts.CompensationFailed:
		if command.AttemptTo != contracts.AttemptFailed && command.AttemptTo != contracts.AttemptAbandoned {
			return errors.New("failed compensation requires failed or abandoned attempt")
		}
		if err := validateFailure(command.Failure, true, "compensation failure"); err != nil {
			return err
		}
		if command.RetryAt != nil {
			return errors.New("terminal compensation failure cannot carry retry time")
		}
	case contracts.CompensationCanceled:
		if command.AttemptTo != contracts.AttemptCanceled && command.AttemptTo != contracts.AttemptAbandoned {
			return errors.New("canceled compensation requires canceled or abandoned attempt")
		}
		if command.Failure != nil || command.RetryAt != nil {
			return errors.New("canceled compensation cannot carry failure or retry time")
		}
	default:
		return fmt.Errorf("%w: compensation running -> %s", ErrInvalidTransition, command.To)
	}
	return nil
}

func countPhase(attempts []contracts.Attempt, phase contracts.AttemptPhase) int {
	count := 0
	for _, attempt := range attempts {
		if attempt.Phase == phase {
			count++
		}
	}
	return count
}

func findAttempt(attempts []contracts.Attempt, id string) (contracts.Attempt, bool) {
	for _, attempt := range attempts {
		if attempt.AttemptID == id {
			return attempt, true
		}
	}
	return contracts.Attempt{}, false
}

func cloneLedger(ledger contracts.InvocationLedger) contracts.InvocationLedger {
	copy := ledger
	copy.Invocation.PrimaryFailure = cloneFailure(ledger.Invocation.PrimaryFailure)
	copy.Invocation.CompensationFailure = cloneFailure(ledger.Invocation.CompensationFailure)
	copy.Invocation.NextAttemptAt = cloneTime(ledger.Invocation.NextAttemptAt)
	copy.Invocation.CompensationNextAt = cloneTime(ledger.Invocation.CompensationNextAt)
	copy.Invocation.StartedAt = cloneTime(ledger.Invocation.StartedAt)
	copy.Invocation.FinishedAt = cloneTime(ledger.Invocation.FinishedAt)
	copy.Attempts = append([]contracts.Attempt(nil), ledger.Attempts...)
	for index := range copy.Attempts {
		copy.Attempts[index].Failure = cloneFailure(copy.Attempts[index].Failure)
		copy.Attempts[index].FinishedAt = cloneTime(copy.Attempts[index].FinishedAt)
	}
	return copy
}
