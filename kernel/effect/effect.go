// Package effect implements the deterministic external-effect protocol. An
// EffectRecord must be durably prepared before a command can enter an outbox.
// Adapters report immutable receipts; they never mutate orchestration state.
package effect

import (
	"errors"
	"fmt"
	"sort"
	"time"

	"github.com/lxk36/xgc2-orchestration-core/kernel/canonicaljson"
	"github.com/lxk36/xgc2-orchestration-core/kernel/execution"
	"github.com/lxk36/xgc2-orchestration-core/sdk/go/contracts"
)

var (
	ErrRevisionConflict  = errors.New("effect revision conflict")
	ErrInvalidTransition = errors.New("invalid effect transition")
	ErrCommandConflict   = errors.New("command identity conflict")
	ErrReceiptConflict   = errors.New("receipt ledger conflict")
	ErrReconcileRequired = errors.New("uncertain effect requires reconciliation")
	ErrEffectConflict    = errors.New("effect preparation conflict")
)

type PrepareCommand struct {
	Intent    contracts.EffectIntent
	CommandID string
	At        time.Time
}

type BeginCommand struct {
	EffectID         string
	ExpectedRevision uint64
	Envelope         contracts.CommandEnvelope
	CommandID        string
	At               time.Time
}

type ObserveCommand struct {
	EffectID         string
	ExpectedRevision uint64
	Receipt          contracts.CommandReceipt
	CommandID        string
}

type ReconcileCommand struct {
	EffectID         string
	ExpectedRevision uint64
	CommandID        string
	At               time.Time
}

// CancelPreparedCommand closes an Effect that has not crossed the provider
// boundary. Applying Effects deliberately cannot use this transition: their
// outcome must still be observed or reconciled before ownership can close.
type CancelPreparedCommand struct {
	EffectID         string
	ExpectedRevision uint64
	CommandID        string
	At               time.Time
}

type CompensationCommand struct {
	EffectID              string
	ExpectedRevision      uint64
	To                    contracts.EffectCompensationState
	CompensationCommandID string
	Failure               *contracts.StructuredFailure
	RetryAt               *time.Time
	CommandID             string
	At                    time.Time
}

type ObserveCompensationCommand struct {
	EffectID         string
	ExpectedRevision uint64
	Receipt          contracts.CommandReceipt
	CommandID        string
}

type Decision struct {
	Effect  contracts.EffectRecord
	Ledger  *contracts.CommandLedger
	Events  []contracts.DomainEvent
	Intents []contracts.DurableIntent
}

func StableEffectID(invocationID, effectKey string) (string, error) {
	return stableID("eff", "xgc.effect/v1", invocationID, effectKey)
}

func StableReceiptID(commandID string, sequence uint32) (string, error) {
	return stableID("rcp", "xgc.command-receipt/v1", commandID, sequence)
}

func Prepare(command PrepareCommand) (Decision, error) {
	if err := validateIdentity(command.CommandID, "prepare command id"); err != nil {
		return Decision{}, err
	}
	if command.At.IsZero() {
		return Decision{}, errors.New("effect preparation time is required")
	}
	if err := ValidateIntent(command.Intent, command.At); err != nil {
		return Decision{}, err
	}
	expectedID, _ := StableEffectID(command.Intent.InvocationID, command.Intent.EffectKey)
	if command.Intent.EffectID != expectedID {
		return Decision{}, errors.New("effect identity does not match invocation and key")
	}
	preparationDigest, err := canonicaljson.DigestValue(command.Intent)
	if err != nil {
		return Decision{}, fmt.Errorf("effect intent digest: %w", err)
	}
	compensation := contracts.EffectCompensationNotRequired
	if command.Intent.CompensationPolicy != contracts.CompensationNone {
		compensation = contracts.EffectCompensationUnscheduled
	}
	record := contracts.EffectRecord{
		EffectID: command.Intent.EffectID, Intent: cloneIntent(command.Intent),
		PreparationDigest: preparationDigest, State: contracts.EffectPrepared,
		CompensationState: compensation, PreparedAt: command.At.UTC(), UpdatedAt: command.At.UTC(), Revision: 1,
	}
	if err := ValidateRecord(record); err != nil {
		return Decision{}, err
	}
	event, err := newEvent(record, "effect.prepared", command.CommandID, command.At, map[string]any{
		"preparationDigest": preparationDigest, "intentDigest": record.Intent.IntentDigest,
	})
	if err != nil {
		return Decision{}, err
	}
	return Decision{Effect: record, Events: []contracts.DomainEvent{event}}, nil
}

// ReplayPrepare distinguishes an idempotent retry from reusing a stable effect
// identity for different intent bytes. It emits no duplicate event.
func ReplayPrepare(existing contracts.EffectRecord, command PrepareCommand) (Decision, error) {
	if err := ValidateRecord(existing); err != nil {
		return Decision{}, err
	}
	prepared, err := Prepare(command)
	if err != nil {
		return Decision{}, err
	}
	if existing.EffectID != prepared.Effect.EffectID || existing.PreparationDigest != prepared.Effect.PreparationDigest {
		return Decision{}, ErrEffectConflict
	}
	return Decision{Effect: cloneRecord(existing)}, nil
}

func CancelPrepared(current contracts.EffectRecord, command CancelPreparedCommand) (Decision, error) {
	if err := ValidateRecord(current); err != nil {
		return Decision{}, fmt.Errorf("current effect: %w", err)
	}
	if command.EffectID != current.EffectID || command.ExpectedRevision != current.Revision {
		return Decision{}, ErrRevisionConflict
	}
	if current.State != contracts.EffectPrepared {
		return Decision{}, fmt.Errorf("%w: only a prepared effect can be canceled without provider evidence", ErrInvalidTransition)
	}
	if err := validateIdentity(command.CommandID, "effect cancellation command id"); err != nil {
		return Decision{}, err
	}
	if command.At.IsZero() || command.At.Before(current.UpdatedAt) {
		return Decision{}, errors.New("effect cancellation time is missing or moves backwards")
	}
	next := cloneRecord(current)
	next.State = contracts.EffectCanceled
	if next.Intent.CompensationPolicy != contracts.CompensationNone {
		next.CompensationState = contracts.EffectCompensationCanceled
		finished := command.At.UTC()
		next.CompensationFinishedAt = &finished
	}
	terminal := command.At.UTC()
	next.PrimaryTerminalAt = &terminal
	next.UpdatedAt = terminal
	next.Revision++
	next.PrimaryTerminalRevision = next.Revision
	if err := ValidateRecord(next); err != nil {
		return Decision{}, err
	}
	event, err := newEvent(next, "effect.canceled", command.CommandID, command.At, map[string]any{
		"from": current.State, "to": next.State,
	})
	if err != nil {
		return Decision{}, err
	}
	return Decision{Effect: next, Events: []contracts.DomainEvent{event}}, nil
}

// Begin moves a prepared or explicitly retryable failed effect to applying and
// emits its command into a durable outbox. It never invokes an adapter.
func Begin(current contracts.EffectRecord, command BeginCommand) (Decision, error) {
	if err := ValidateRecord(current); err != nil {
		return Decision{}, fmt.Errorf("current effect: %w", err)
	}
	if command.EffectID != current.EffectID || command.ExpectedRevision != current.Revision {
		return Decision{}, ErrRevisionConflict
	}
	if command.CommandID != command.Envelope.CommandID {
		return Decision{}, errors.New("begin command id must equal envelope command id")
	}
	if command.At.IsZero() || command.At.Before(current.UpdatedAt) {
		return Decision{}, errors.New("effect begin time is missing or moves backwards")
	}
	if current.State == contracts.EffectUncertain {
		return Decision{}, ErrReconcileRequired
	}
	if current.State != contracts.EffectPrepared && !(current.State == contracts.EffectFailed && current.PrimaryFailure != nil && current.PrimaryFailure.Class == contracts.FailureTransient) {
		return Decision{}, fmt.Errorf("%w: effect %s cannot begin", ErrInvalidTransition, current.State)
	}
	if current.CommandID != "" && command.Envelope.CommandID == current.CommandID {
		return Decision{}, ErrCommandConflict
	}
	if err := ValidateEnvelopeForIntent(command.Envelope, current.Intent, command.At, true); err != nil {
		return Decision{}, err
	}
	next := cloneRecord(current)
	next.State = contracts.EffectApplying
	next.CommandID = command.Envelope.CommandID
	next.CommandIdentityDigest = command.Envelope.IdentityDigest
	next.ExternalIdentity = ""
	next.ResultDigest = ""
	next.ResultArtifactRef = ""
	next.PrimaryFailure = nil
	next.PrimaryTerminalAt = nil
	next.PrimaryTerminalRevision = 0
	applying := command.At.UTC()
	next.ApplyingAt = &applying
	next.UpdatedAt = applying
	next.Revision++
	if err := ValidateRecord(next); err != nil {
		return Decision{}, err
	}
	ledger := contracts.CommandLedger{Envelope: cloneEnvelope(command.Envelope), Receipts: []contracts.CommandReceipt{}}
	if err := ValidateLedger(ledger); err != nil {
		return Decision{}, err
	}
	payload := map[string]any{"command": ledger.Envelope}
	intent, err := durableIntent(contracts.IntentOutbox, next.EffectID, next.Revision, payload)
	if err != nil {
		return Decision{}, err
	}
	event, err := newEvent(next, "effect.applying", command.CommandID, command.At, map[string]any{
		"commandId": next.CommandID, "commandIdentityDigest": next.CommandIdentityDigest,
	})
	if err != nil {
		return Decision{}, err
	}
	return Decision{Effect: next, Ledger: &ledger, Events: []contracts.DomainEvent{event}, Intents: []contracts.DurableIntent{intent}}, nil
}

// Observe appends one adapter receipt. A terminal receipt folds the primary
// Effect state; a persisted terminal or uncertain observation cannot be
// overwritten by a later adapter response.
func Observe(current contracts.EffectRecord, ledger contracts.CommandLedger, command ObserveCommand) (Decision, error) {
	if err := ValidateRecord(current); err != nil {
		return Decision{}, fmt.Errorf("current effect: %w", err)
	}
	if err := ValidateLedger(ledger); err != nil {
		return Decision{}, fmt.Errorf("current command ledger: %w", err)
	}
	if command.EffectID != current.EffectID || command.ExpectedRevision != current.Revision {
		return Decision{}, ErrRevisionConflict
	}
	if current.State != contracts.EffectApplying || ledger.Envelope.CommandID != current.CommandID || ledger.Envelope.EffectID != current.EffectID {
		return Decision{}, fmt.Errorf("%w: receipt does not target the applying command", ErrInvalidTransition)
	}
	if err := validateIdentity(command.CommandID, "observation command id"); err != nil {
		return Decision{}, err
	}
	receipt := cloneReceipt(command.Receipt)
	if err := validateNextReceipt(ledger, receipt); err != nil {
		return Decision{}, err
	}
	nextLedger := cloneLedger(ledger)
	nextLedger.Receipts = append(nextLedger.Receipts, receipt)
	if err := ValidateLedger(nextLedger); err != nil {
		return Decision{}, err
	}
	next := cloneRecord(current)
	next.UpdatedAt = receipt.ObservedAt.UTC()
	next.Revision++
	switch receipt.Status {
	case contracts.ReceiptAccepted:
		// Applying remains observable while the adapter owns the command.
	case contracts.ReceiptSucceeded:
		next.State = contracts.EffectApplied
		next.ResultDigest = receipt.ResultDigest
		next.ResultArtifactRef = receipt.ResultArtifactRef
		next.ExternalIdentity = receipt.ExternalIdentity
		terminal := receipt.ObservedAt.UTC()
		next.PrimaryTerminalAt = &terminal
	case contracts.ReceiptRejected, contracts.ReceiptFailed:
		next.State = contracts.EffectFailed
		next.PrimaryFailure = cloneFailure(receipt.Failure)
		terminal := receipt.ObservedAt.UTC()
		next.PrimaryTerminalAt = &terminal
	case contracts.ReceiptUncertain:
		next.State = contracts.EffectUncertain
		next.PrimaryFailure = cloneFailure(receipt.Failure)
		next.ExternalIdentity = receipt.ExternalIdentity
		terminal := receipt.ObservedAt.UTC()
		next.PrimaryTerminalAt = &terminal
	default:
		return Decision{}, ErrReceiptConflict
	}
	if receipt.Status.Terminal() {
		next.PrimaryTerminalRevision = next.Revision
	}
	if err := ValidateRecord(next); err != nil {
		return Decision{}, err
	}
	event, err := newEvent(next, "effect.receipt."+string(receipt.Status), command.CommandID, receipt.ObservedAt, map[string]any{
		"commandId": receipt.CommandID, "receiptId": receipt.ReceiptID, "sequence": receipt.Sequence,
	})
	if err != nil {
		return Decision{}, err
	}
	return Decision{Effect: next, Ledger: &nextLedger, Events: []contracts.DomainEvent{event}}, nil
}

// RequestReconciliation is the only automatic follow-up accepted after an
// uncertain receipt. It creates a query/reconcile intent, never a mutation
// retry.
func RequestReconciliation(current contracts.EffectRecord, command ReconcileCommand) (Decision, error) {
	if err := ValidateRecord(current); err != nil {
		return Decision{}, err
	}
	if command.EffectID != current.EffectID || command.ExpectedRevision != current.Revision {
		return Decision{}, ErrRevisionConflict
	}
	if current.State != contracts.EffectUncertain {
		return Decision{}, fmt.Errorf("%w: only uncertain effects can reconcile", ErrInvalidTransition)
	}
	if err := validateIdentity(command.CommandID, "reconcile command id"); err != nil {
		return Decision{}, err
	}
	if command.At.IsZero() || command.At.Before(current.UpdatedAt) {
		return Decision{}, errors.New("reconcile time is missing or moves backwards")
	}
	next := cloneRecord(current)
	next.UpdatedAt = command.At.UTC()
	next.Revision++
	payload := map[string]any{
		"effectId": next.EffectID, "commandId": next.CommandID,
		"commandIdentityDigest": next.CommandIdentityDigest, "externalIdentity": next.ExternalIdentity,
	}
	intent, err := durableIntent(contracts.IntentReconcile, next.EffectID, next.Revision, payload)
	if err != nil {
		return Decision{}, err
	}
	event, err := newEvent(next, "effect.reconciliation-requested", command.CommandID, command.At, payload)
	if err != nil {
		return Decision{}, err
	}
	return Decision{Effect: next, Events: []contracts.DomainEvent{event}, Intents: []contracts.DurableIntent{intent}}, nil
}

func TransitionCompensation(current contracts.EffectRecord, command CompensationCommand) (Decision, error) {
	if err := ValidateRecord(current); err != nil {
		return Decision{}, err
	}
	if command.EffectID != current.EffectID || command.ExpectedRevision != current.Revision {
		return Decision{}, ErrRevisionConflict
	}
	if err := validateIdentity(command.CommandID, "compensation transition command id"); err != nil {
		return Decision{}, err
	}
	if command.At.IsZero() || command.At.Before(current.UpdatedAt) {
		return Decision{}, errors.New("compensation time is missing or moves backwards")
	}
	if current.State != contracts.EffectApplied || current.Intent.CompensationPolicy == contracts.CompensationNone {
		return Decision{}, fmt.Errorf("%w: effect is not compensatable", ErrInvalidTransition)
	}
	if err := validateCompensationTransition(current.CompensationState, command); err != nil {
		return Decision{}, err
	}
	next := cloneRecord(current)
	next.CompensationState = command.To
	next.CompensationFailure = cloneFailure(command.Failure)
	next.CompensationNextAt = cloneTime(command.RetryAt)
	next.UpdatedAt = command.At.UTC()
	next.Revision++
	if command.To == contracts.EffectCompensationRunning {
		next.CompensationAttemptCount++
		next.CompensationCommandID = command.CompensationCommandID
		started := command.At.UTC()
		next.CompensationStartedAt = &started
		next.CompensationFinishedAt = nil
	}
	if command.To == contracts.EffectCompensationSucceeded || command.To == contracts.EffectCompensationFailed || command.To == contracts.EffectCompensationCanceled {
		finished := command.At.UTC()
		next.CompensationFinishedAt = &finished
	}
	if err := ValidateRecord(next); err != nil {
		return Decision{}, err
	}
	event, err := newEvent(next, "effect.compensation."+string(command.To), command.CommandID, command.At, map[string]any{
		"from": current.CompensationState, "to": command.To, "attemptCount": next.CompensationAttemptCount,
	})
	if err != nil {
		return Decision{}, err
	}
	decision := Decision{Effect: next, Events: []contracts.DomainEvent{event}}
	if command.To == contracts.EffectCompensationPending || command.To == contracts.EffectCompensationRetryWait {
		payload := map[string]any{"effectId": next.EffectID, "compensationState": command.To, "effectRevision": next.Revision}
		intent, err := durableIntent(contracts.IntentCleanup, next.EffectID, next.Revision, payload)
		if err != nil {
			return Decision{}, err
		}
		decision.Intents = []contracts.DurableIntent{intent}
	}
	return decision, nil
}

// ObserveCompensation appends one receipt to the independently persisted
// compensation command ledger and folds terminal provider evidence into the
// Effect compensation state. Primary Effect evidence is immutable here.
func ObserveCompensation(current contracts.EffectRecord, ledger contracts.CommandLedger, command ObserveCompensationCommand) (Decision, error) {
	if err := ValidateRecord(current); err != nil {
		return Decision{}, fmt.Errorf("current effect: %w", err)
	}
	if err := ValidateLedger(ledger); err != nil {
		return Decision{}, fmt.Errorf("current compensation ledger: %w", err)
	}
	if command.EffectID != current.EffectID || command.ExpectedRevision != current.Revision {
		return Decision{}, ErrRevisionConflict
	}
	if current.State != contracts.EffectApplied || current.CompensationState != contracts.EffectCompensationRunning ||
		ledger.Envelope.CommandID != current.CompensationCommandID || ledger.Envelope.EffectID != current.EffectID {
		return Decision{}, fmt.Errorf("%w: receipt does not target the running compensation", ErrInvalidTransition)
	}
	if err := validateIdentity(command.CommandID, "compensation observation command id"); err != nil {
		return Decision{}, err
	}
	receipt := cloneReceipt(command.Receipt)
	if err := validateNextReceipt(ledger, receipt); err != nil {
		return Decision{}, err
	}
	nextLedger := cloneLedger(ledger)
	nextLedger.Receipts = append(nextLedger.Receipts, receipt)
	if err := ValidateLedger(nextLedger); err != nil {
		return Decision{}, err
	}
	next := cloneRecord(current)
	next.UpdatedAt = receipt.ObservedAt.UTC()
	next.Revision++
	switch receipt.Status {
	case contracts.ReceiptAccepted:
		// Running remains observable while the compensator owns the command.
	case contracts.ReceiptSucceeded:
		next.CompensationState = contracts.EffectCompensationSucceeded
		finished := receipt.ObservedAt.UTC()
		next.CompensationFinishedAt = &finished
	case contracts.ReceiptRejected, contracts.ReceiptFailed, contracts.ReceiptUncertain:
		next.CompensationState = contracts.EffectCompensationFailed
		next.CompensationFailure = cloneFailure(receipt.Failure)
		finished := receipt.ObservedAt.UTC()
		next.CompensationFinishedAt = &finished
	default:
		return Decision{}, ErrReceiptConflict
	}
	if err := ValidateRecord(next); err != nil {
		return Decision{}, err
	}
	event, err := newEvent(next, "effect.compensation.receipt."+string(receipt.Status), command.CommandID, receipt.ObservedAt, map[string]any{
		"commandId": receipt.CommandID, "receiptId": receipt.ReceiptID, "sequence": receipt.Sequence,
	})
	if err != nil {
		return Decision{}, err
	}
	return Decision{Effect: next, Ledger: &nextLedger, Events: []contracts.DomainEvent{event}}, nil
}

func ValidateIntent(intent contracts.EffectIntent, at time.Time) error {
	for label, value := range map[string]string{
		"effect id": intent.EffectID, "namespace id": intent.NamespaceID, "run id": intent.RunID,
		"invocation id": intent.InvocationID, "prepared attempt id": intent.PreparedAttemptID,
		"effect key": intent.EffectKey, "target ref": intent.TargetRef,
	} {
		if err := validateIdentity(value, label); err != nil {
			return err
		}
	}
	if !contracts.ValidTypeRef(intent.Kind) {
		return errors.New("effect kind must be a versioned type ref")
	}
	for label, digest := range map[string]string{
		"intent schema digest": intent.IntentSchemaDigest, "intent digest": intent.IntentDigest,
		"policy digest": intent.PolicyDigest, "descriptor digest": intent.DescriptorDigest,
	} {
		if !contracts.ValidDigest(digest) {
			return fmt.Errorf("%s is invalid", label)
		}
	}
	if err := validateOptionalIdentity(intent.IntentArtifactRef, "intent artifact ref"); err != nil {
		return err
	}
	if intent.Intent == nil {
		return errors.New("effect intent payload is required")
	}
	intentDigest, err := canonicaljson.DigestValue(intent.Intent)
	if err != nil || intentDigest != intent.IntentDigest {
		return errors.New("effect intent payload digest is invalid")
	}
	if !intent.Ownership.Valid() || !intent.CompensationPolicy.Valid() {
		return errors.New("effect ownership or compensation policy is invalid")
	}
	if intent.Ownership != contracts.EffectOwned && intent.CompensationPolicy != contracts.CompensationNone {
		return errors.New("only owned effects can require compensation")
	}
	if err := validateSortedIdentities(intent.RequiredCapabilityRefs, "required capability refs"); err != nil {
		return err
	}
	if intent.Deadline.IsZero() || (!at.IsZero() && !intent.Deadline.After(at)) {
		return errors.New("effect deadline must be after preparation")
	}
	return nil
}

func ValidateRecord(record contracts.EffectRecord) error {
	if err := ValidateIntent(record.Intent, time.Time{}); err != nil {
		return err
	}
	if record.EffectID != record.Intent.EffectID || !record.State.Valid() || !record.CompensationState.Valid() || record.Revision == 0 {
		return errors.New("effect identity, state, compensation state, or revision is invalid")
	}
	preparationDigest, err := canonicaljson.DigestValue(record.Intent)
	if err != nil || record.PreparationDigest != preparationDigest {
		return errors.New("effect preparation digest is invalid")
	}
	if record.PreparedAt.IsZero() || record.UpdatedAt.Before(record.PreparedAt) {
		return errors.New("effect timestamps are invalid")
	}
	if record.State == contracts.EffectPrepared {
		if record.CommandID != "" || record.CommandIdentityDigest != "" || record.ApplyingAt != nil || record.PrimaryTerminalAt != nil || record.PrimaryTerminalRevision != 0 {
			return errors.New("prepared effect contains application state")
		}
	} else if record.State == contracts.EffectCanceled {
		if record.CommandID != "" || record.CommandIdentityDigest != "" || record.ApplyingAt != nil || record.PrimaryTerminalAt == nil {
			return errors.New("pre-dispatch canceled effect contains provider application state or lacks its terminal time")
		}
	} else {
		if err := validateIdentity(record.CommandID, "effect command id"); err != nil {
			return err
		}
		if !contracts.ValidDigest(record.CommandIdentityDigest) || record.ApplyingAt == nil {
			return errors.New("started effect requires command identity and applying time")
		}
	}
	if record.ApplyingAt != nil && (record.ApplyingAt.Before(record.PreparedAt) || record.ApplyingAt.After(record.UpdatedAt)) {
		return errors.New("effect applying time is outside its lifecycle")
	}
	if record.State.Terminal() != (record.PrimaryTerminalAt != nil) {
		return errors.New("effect terminal observation and terminal time disagree")
	}
	if record.PrimaryTerminalAt != nil && (record.PrimaryTerminalAt.Before(record.PreparedAt) || record.PrimaryTerminalAt.After(record.UpdatedAt)) {
		return errors.New("effect terminal time is outside its lifecycle")
	}
	if record.PrimaryTerminalRevision > record.Revision || (record.PrimaryTerminalRevision != 0 && !record.State.Terminal()) {
		return errors.New("effect primary terminal revision is outside its lifecycle")
	}
	switch record.State {
	case contracts.EffectApplied:
		if !contracts.ValidDigest(record.ResultDigest) || record.PrimaryFailure != nil {
			return errors.New("applied effect requires a result digest and no failure")
		}
	case contracts.EffectFailed:
		if err := validateFailure(record.PrimaryFailure, false); err != nil || record.PrimaryFailure == nil || record.PrimaryFailure.Class == contracts.FailureUncertain {
			return errors.New("failed effect requires a non-uncertain failure")
		}
	case contracts.EffectUncertain:
		if err := validateFailure(record.PrimaryFailure, true); err != nil || record.PrimaryFailure.Class != contracts.FailureUncertain {
			return errors.New("uncertain effect requires an uncertain failure")
		}
	case contracts.EffectCanceled:
		if record.ResultDigest != "" || record.ResultArtifactRef != "" || record.ExternalIdentity != "" || record.PrimaryFailure != nil {
			return errors.New("pre-dispatch canceled effect contains a provider observation")
		}
	default:
		if record.ResultDigest != "" || record.ResultArtifactRef != "" || record.ExternalIdentity != "" || record.PrimaryFailure != nil {
			return errors.New("nonterminal effect contains a terminal observation")
		}
	}
	if err := validateCompensation(record); err != nil {
		return err
	}
	return nil
}

func ValidateEnvelopeForIntent(envelope contracts.CommandEnvelope, intent contracts.EffectIntent, at time.Time, requirePrivate bool) error {
	if err := ValidateEnvelope(envelope); err != nil {
		return err
	}
	if envelope.EffectID != intent.EffectID || envelope.NamespaceID != intent.NamespaceID || envelope.TargetRef != intent.TargetRef ||
		envelope.PolicyDigest != intent.PolicyDigest || envelope.DescriptorDigest != intent.DescriptorDigest ||
		!equalStrings(envelope.RequiredCapabilityRefs, intent.RequiredCapabilityRefs) {
		return ErrCommandConflict
	}
	if envelope.Deadline.After(intent.Deadline) || !envelope.Deadline.After(at) {
		return errors.New("command deadline exceeds the prepared effect deadline or has elapsed")
	}
	if len(intent.RequiredCapabilityRefs) == 0 {
		if envelope.CapabilityToken != "" || envelope.CapabilityTokenHash != "" {
			return errors.New("command without capabilities carries a capability token")
		}
	}
	if requirePrivate {
		return ValidatePrivateEnvelopeTokens(envelope)
	}
	return nil
}

// ValidatePrivateEnvelopeTokens proves that host-side credential rehydration
// matches the immutable public hashes. It is used for both primary dispatch
// and compensation; providers never receive substituted ambient credentials.
func ValidatePrivateEnvelopeTokens(envelope contracts.CommandEnvelope) error {
	if err := ValidateEnvelope(envelope); err != nil {
		return err
	}
	keyHash, err := execution.PrivateTokenDigest(envelope.IdempotencyKey)
	if err != nil || keyHash != envelope.IdempotencyKeyHash {
		return errors.New("command idempotency key does not match its public hash")
	}
	if len(envelope.RequiredCapabilityRefs) == 0 {
		if envelope.CapabilityToken != "" || envelope.CapabilityTokenHash != "" {
			return errors.New("command without capabilities carries a capability token")
		}
		return nil
	}
	tokenHash, err := execution.PrivateTokenDigest(envelope.CapabilityToken)
	if err != nil || tokenHash != envelope.CapabilityTokenHash {
		return errors.New("command capability token does not match its public hash")
	}
	return nil
}

func ValidateEnvelope(envelope contracts.CommandEnvelope) error {
	for label, value := range map[string]string{
		"command id": envelope.CommandID, "effect id": envelope.EffectID, "namespace id": envelope.NamespaceID,
		"target ref": envelope.TargetRef, "action": envelope.Action, "actor ref": envelope.ActorRef,
		"source ref": envelope.SourceRef, "reason code": envelope.ReasonCode, "cancellation id": envelope.CancellationID,
	} {
		if err := validateIdentity(value, label); err != nil {
			return err
		}
	}
	if err := validateOptionalIdentity(envelope.RequestID, "request id"); err != nil {
		return err
	}
	if !envelope.Risk.Valid() || envelope.Deadline.IsZero() {
		return errors.New("command risk or deadline is invalid")
	}
	for label, digest := range map[string]string{
		"idempotency key hash": envelope.IdempotencyKeyHash, "identity digest": envelope.IdentityDigest,
		"payload digest": envelope.PayloadDigest, "policy digest": envelope.PolicyDigest,
		"descriptor digest": envelope.DescriptorDigest,
	} {
		if !contracts.ValidDigest(digest) {
			return fmt.Errorf("command %s is invalid", label)
		}
	}
	if !contracts.ValidOptionalDigest(envelope.ManifestDigest) || !contracts.ValidOptionalDigest(envelope.CapabilityTokenHash) {
		return errors.New("command manifest or capability token hash is invalid")
	}
	if err := validateOptionalIdentity(envelope.PayloadArtifactRef, "payload artifact ref"); err != nil {
		return err
	}
	if err := validateSortedIdentities(envelope.RequiredCapabilityRefs, "required capability refs"); err != nil {
		return err
	}
	if err := validateFence(envelope.Fence); err != nil {
		return err
	}
	identity, err := CommandIdentityDigest(envelope)
	if err != nil || identity != envelope.IdentityDigest {
		return errors.New("command identity digest is invalid")
	}
	return nil
}

func ValidateLedger(ledger contracts.CommandLedger) error {
	if err := ValidateEnvelope(ledger.Envelope); err != nil {
		return err
	}
	terminal := false
	for index := range ledger.Receipts {
		receipt := ledger.Receipts[index]
		if terminal {
			return errors.New("receipt appears after terminal observation")
		}
		if receipt.Sequence != uint32(index+1) {
			return errors.New("receipt sequence is not contiguous")
		}
		if err := validateReceipt(ledger.Envelope, receipt); err != nil {
			return fmt.Errorf("receipt %d: %w", index, err)
		}
		if index > 0 && ledger.Receipts[index-1].ProviderRef != receipt.ProviderRef {
			return errors.New("command receipts disagree on provider")
		}
		if index > 0 && receipt.ObservedAt.Before(ledger.Receipts[index-1].ObservedAt) {
			return errors.New("command receipt time moves backwards")
		}
		terminal = receipt.Status.Terminal()
	}
	return nil
}

func CommandIdentityDigest(envelope contracts.CommandEnvelope) (string, error) {
	identity := map[string]any{
		"commandId": envelope.CommandID, "effectId": envelope.EffectID,
		"idempotencyKeyHash": envelope.IdempotencyKeyHash, "namespaceId": envelope.NamespaceID,
		"targetRef": envelope.TargetRef, "action": envelope.Action, "actorRef": envelope.ActorRef,
		"sourceRef": envelope.SourceRef, "reasonCode": envelope.ReasonCode, "reason": envelope.Reason,
		"risk": envelope.Risk, "fence": cloneFence(envelope.Fence), "payloadDigest": envelope.PayloadDigest,
		"payloadArtifactRef": envelope.PayloadArtifactRef, "policyDigest": envelope.PolicyDigest,
		"descriptorDigest": envelope.DescriptorDigest, "manifestDigest": envelope.ManifestDigest,
		"deadline": envelope.Deadline.UTC(), "cancellationId": envelope.CancellationID,
		"requiredCapabilityRefs": append([]string(nil), envelope.RequiredCapabilityRefs...),
		"capabilityTokenHash":    envelope.CapabilityTokenHash,
	}
	return canonicaljson.DigestValue(identity)
}

func FenceDigest(fence contracts.TargetFence) (string, error) {
	if err := validateFence(fence); err != nil {
		return "", err
	}
	return canonicaljson.DigestValue(fence)
}

func validateNextReceipt(ledger contracts.CommandLedger, receipt contracts.CommandReceipt) error {
	if len(ledger.Receipts) > 0 && ledger.Receipts[len(ledger.Receipts)-1].Status.Terminal() {
		return ErrReceiptConflict
	}
	if receipt.Sequence != uint32(len(ledger.Receipts)+1) {
		return ErrReceiptConflict
	}
	return validateReceipt(ledger.Envelope, receipt)
}

func validateReceipt(envelope contracts.CommandEnvelope, receipt contracts.CommandReceipt) error {
	expectedID, err := StableReceiptID(envelope.CommandID, receipt.Sequence)
	if err != nil || receipt.ReceiptID != expectedID || receipt.CommandID != envelope.CommandID || receipt.IdentityDigest != envelope.IdentityDigest {
		return ErrReceiptConflict
	}
	if !receipt.Status.Valid() || receipt.Sequence == 0 || receipt.ObservedAt.IsZero() {
		return errors.New("receipt status, sequence, or time is invalid")
	}
	expectedFence, err := FenceDigest(envelope.Fence)
	if err != nil || receipt.FenceDigest != expectedFence {
		return errors.New("receipt fence digest is invalid")
	}
	for label, value := range map[string]string{"provider ref": receipt.ProviderRef} {
		if err := validateIdentity(value, label); err != nil {
			return err
		}
	}
	for label, digest := range map[string]string{
		"provider digest": receipt.ProviderDigest, "policy digest": receipt.PolicyDigest,
		"authorization digest": receipt.AuthorizationDigest,
	} {
		if !contracts.ValidDigest(digest) {
			return fmt.Errorf("receipt %s is invalid", label)
		}
	}
	if receipt.PolicyDigest != envelope.PolicyDigest || !contracts.ValidOptionalDigest(receipt.ResultDigest) {
		return errors.New("receipt policy or result digest does not match the command")
	}
	if err := validateOptionalIdentity(receipt.ResultArtifactRef, "receipt result artifact ref"); err != nil {
		return err
	}
	if err := validateOptionalIdentity(receipt.ExternalIdentity, "receipt external identity"); err != nil {
		return err
	}
	switch receipt.Status {
	case contracts.ReceiptAccepted:
		if receipt.Sequence != 1 || receipt.ResultDigest != "" || receipt.ResultArtifactRef != "" || receipt.Failure != nil {
			return errors.New("accepted receipt must be the first nonterminal observation")
		}
	case contracts.ReceiptRejected:
		if receipt.Sequence != 1 || receipt.ResultDigest != "" || receipt.ResultArtifactRef != "" {
			return errors.New("rejected receipt must be the first terminal observation")
		}
		if err := validateFailure(receipt.Failure, false); err != nil || receipt.Failure.Class == contracts.FailureUncertain {
			return errors.New("rejected receipt requires a non-uncertain failure")
		}
	case contracts.ReceiptSucceeded:
		if receipt.Sequence != 2 || !contracts.ValidDigest(receipt.ResultDigest) || receipt.Failure != nil {
			return errors.New("succeeded receipt requires result digest and no failure")
		}
	case contracts.ReceiptFailed:
		if receipt.Sequence != 2 || receipt.ResultDigest != "" || receipt.ResultArtifactRef != "" {
			return errors.New("failed receipt must follow accepted without a result")
		}
		if err := validateFailure(receipt.Failure, false); err != nil || receipt.Failure.Class == contracts.FailureUncertain {
			return errors.New("failed receipt requires a non-uncertain failure")
		}
	case contracts.ReceiptUncertain:
		if receipt.Sequence != 2 || receipt.ResultDigest != "" || receipt.ResultArtifactRef != "" {
			return errors.New("uncertain receipt must follow accepted without a result")
		}
		if err := validateFailure(receipt.Failure, true); err != nil || receipt.Failure.Class != contracts.FailureUncertain {
			return errors.New("uncertain receipt requires an uncertain failure")
		}
	}
	return nil
}

func validateFence(fence contracts.TargetFence) error {
	count := 0
	if fence.Revision != nil {
		count++
	}
	if fence.Generation != nil {
		count++
	}
	if fence.IdempotentCreate != nil {
		count++
	}
	if count != 1 {
		return errors.New("target fence must select exactly one variant")
	}
	switch fence.Kind {
	case contracts.FenceRevision:
		if fence.Revision == nil || fence.Generation != nil || fence.IdempotentCreate != nil || fence.Revision.ExpectedRevision == 0 {
			return errors.New("revision fence union is invalid")
		}
		return validateIdentity(fence.Revision.TargetID, "revision fence target id")
	case contracts.FenceGeneration:
		if fence.Generation == nil || fence.Revision != nil || fence.IdempotentCreate != nil || fence.Generation.Generation == 0 || fence.Generation.FencingToken == 0 {
			return errors.New("generation fence union is invalid")
		}
		return validateIdentity(fence.Generation.BindingID, "generation fence binding id")
	case contracts.FenceIdempotentCreate:
		if fence.IdempotentCreate == nil || fence.Revision != nil || fence.Generation != nil || !contracts.ValidDigest(fence.IdempotentCreate.IdentityDigest) {
			return errors.New("idempotent-create fence union is invalid")
		}
		return validateIdentity(fence.IdempotentCreate.TargetNamespace, "idempotent-create target namespace")
	default:
		return errors.New("target fence kind is invalid")
	}
}

func validateCompensation(record contracts.EffectRecord) error {
	if record.Intent.CompensationPolicy == contracts.CompensationNone {
		if record.CompensationState != contracts.EffectCompensationNotRequired {
			return errors.New("non-compensatable effect has compensation state")
		}
		return nil
	}
	if record.CompensationState == contracts.EffectCompensationNotRequired {
		return errors.New("compensatable effect is marked not-required")
	}
	if record.CompensationAttemptCount == 0 && record.CompensationState == contracts.EffectCompensationRunning {
		return errors.New("running compensation has no attempt")
	}
	if record.CompensationState == contracts.EffectCompensationRetryWait {
		if record.CompensationNextAt == nil || record.CompensationFailure == nil {
			return errors.New("compensation retry-wait requires failure and next time")
		}
	} else if record.CompensationNextAt != nil {
		return errors.New("compensation next time exists outside retry-wait")
	}
	if record.CompensationState == contracts.EffectCompensationFailed {
		if record.CompensationFailure == nil {
			return errors.New("failed compensation requires failure")
		}
	} else if record.CompensationState != contracts.EffectCompensationRetryWait && record.CompensationFailure != nil {
		return errors.New("compensation failure exists outside failed or retry-wait")
	}
	if record.CompensationState == contracts.EffectCompensationRunning {
		if err := validateIdentity(record.CompensationCommandID, "compensation command id"); err != nil {
			return err
		}
	}
	return nil
}

func validateCompensationTransition(from contracts.EffectCompensationState, command CompensationCommand) error {
	allowed := map[contracts.EffectCompensationState]map[contracts.EffectCompensationState]bool{
		contracts.EffectCompensationUnscheduled: {contracts.EffectCompensationPending: true, contracts.EffectCompensationCanceled: true},
		contracts.EffectCompensationPending:     {contracts.EffectCompensationRunning: true, contracts.EffectCompensationCanceled: true},
		contracts.EffectCompensationRunning:     {contracts.EffectCompensationSucceeded: true, contracts.EffectCompensationRetryWait: true, contracts.EffectCompensationFailed: true, contracts.EffectCompensationCanceled: true},
		contracts.EffectCompensationRetryWait:   {contracts.EffectCompensationRunning: true, contracts.EffectCompensationFailed: true, contracts.EffectCompensationCanceled: true},
	}
	if !command.To.Valid() || !allowed[from][command.To] {
		return fmt.Errorf("%w: compensation %s -> %s", ErrInvalidTransition, from, command.To)
	}
	if command.To == contracts.EffectCompensationRunning {
		if err := validateIdentity(command.CompensationCommandID, "compensation command id"); err != nil {
			return err
		}
	} else if command.CompensationCommandID != "" {
		return errors.New("compensation command id is only set on running")
	}
	if command.To == contracts.EffectCompensationRetryWait {
		if command.RetryAt == nil || !command.RetryAt.After(command.At) || command.Failure == nil || command.Failure.Class != contracts.FailureTransient {
			return errors.New("compensation retry-wait requires transient failure and future time")
		}
	} else if command.RetryAt != nil {
		return errors.New("retry time is only set on compensation retry-wait")
	}
	if command.To == contracts.EffectCompensationFailed {
		if err := validateFailure(command.Failure, false); err != nil || command.Failure == nil {
			return errors.New("failed compensation requires failure")
		}
	} else if command.To != contracts.EffectCompensationRetryWait && command.Failure != nil {
		return errors.New("compensation failure is only set on failure or retry-wait")
	}
	return nil
}

func durableIntent(kind contracts.DurableIntentKind, aggregateID string, revision uint64, payload map[string]any) (contracts.DurableIntent, error) {
	digest, err := canonicaljson.DigestValue(payload)
	if err != nil {
		return contracts.DurableIntent{}, err
	}
	id, err := execution.StableIntentID(kind, aggregateID, revision)
	if err != nil {
		return contracts.DurableIntent{}, err
	}
	return contracts.DurableIntent{Kind: kind, Identity: id, AggregateID: aggregateID, PayloadDigest: digest, Payload: payload}, nil
}

func newEvent(record contracts.EffectRecord, eventType, commandID string, at time.Time, payload map[string]any) (contracts.DomainEvent, error) {
	digest, err := canonicaljson.DigestValue(payload)
	if err != nil {
		return contracts.DomainEvent{}, err
	}
	id, err := execution.StableEventID("effect", record.EffectID, record.Revision)
	if err != nil {
		return contracts.DomainEvent{}, err
	}
	return contracts.DomainEvent{
		EventID: id, AggregateType: "effect", AggregateID: record.EffectID, AggregateRevision: record.Revision,
		Type: eventType, CommandID: commandID,
		PayloadSchemaDigest: "sha256:82ef96cebaf5fbe16269fd18b0240d78f5b9b90a4155a17eb797115b09148ecf",
		PayloadDigest:       digest, Payload: payload, OccurredAt: at.UTC(),
	}, nil
}

func stableID(prefix, domain string, parts ...any) (string, error) {
	// Reuse the public deterministic domains while keeping the execution
	// package's helper surface minimal.
	payload := append([]any{domain}, parts...)
	digest, err := canonicaljson.DigestValue(payload)
	if err != nil {
		return "", err
	}
	return prefix + "-" + digest[len("sha256:"):], nil
}

func validateIdentity(value, label string) error {
	if !contracts.ValidIdentifier(value) {
		return fmt.Errorf("%s must be a portable identifier", label)
	}
	return nil
}

func validateOptionalIdentity(value, label string) error {
	if value == "" {
		return nil
	}
	return validateIdentity(value, label)
}

func validateSortedIdentities(values []string, label string) error {
	if !sort.StringsAreSorted(values) {
		return fmt.Errorf("%s must be sorted", label)
	}
	for index, value := range values {
		if err := validateIdentity(value, fmt.Sprintf("%s[%d]", label, index)); err != nil {
			return err
		}
		if index > 0 && values[index-1] == value {
			return fmt.Errorf("%s contains duplicate %q", label, value)
		}
	}
	return nil
}

func validateFailure(failure *contracts.StructuredFailure, uncertain bool) error {
	if failure == nil {
		return errors.New("failure is required")
	}
	if !failure.Class.Valid() || !contracts.ValidIdentifier(failure.Code) || failure.Message == "" {
		return errors.New("failure is invalid")
	}
	if uncertain && failure.Class != contracts.FailureUncertain {
		return errors.New("failure must be uncertain")
	}
	return validateOptionalIdentity(failure.EvidenceRef, "failure evidence ref")
}

func cloneIntent(intent contracts.EffectIntent) contracts.EffectIntent {
	intent.RequiredCapabilityRefs = append([]string(nil), intent.RequiredCapabilityRefs...)
	intent.Intent, _ = contracts.CloneObject(intent.Intent)
	return intent
}

func cloneRecord(record contracts.EffectRecord) contracts.EffectRecord {
	record.Intent = cloneIntent(record.Intent)
	record.PrimaryFailure = cloneFailure(record.PrimaryFailure)
	record.CompensationFailure = cloneFailure(record.CompensationFailure)
	record.ApplyingAt = cloneTime(record.ApplyingAt)
	record.PrimaryTerminalAt = cloneTime(record.PrimaryTerminalAt)
	record.CompensationStartedAt = cloneTime(record.CompensationStartedAt)
	record.CompensationFinishedAt = cloneTime(record.CompensationFinishedAt)
	record.CompensationNextAt = cloneTime(record.CompensationNextAt)
	return record
}

func cloneEnvelope(envelope contracts.CommandEnvelope) contracts.CommandEnvelope {
	envelope.RequiredCapabilityRefs = append([]string(nil), envelope.RequiredCapabilityRefs...)
	envelope.Fence = cloneFence(envelope.Fence)
	return envelope
}

func cloneFence(fence contracts.TargetFence) contracts.TargetFence {
	if fence.Revision != nil {
		copy := *fence.Revision
		fence.Revision = &copy
	}
	if fence.Generation != nil {
		copy := *fence.Generation
		fence.Generation = &copy
	}
	if fence.IdempotentCreate != nil {
		copy := *fence.IdempotentCreate
		fence.IdempotentCreate = &copy
	}
	return fence
}

func cloneReceipt(receipt contracts.CommandReceipt) contracts.CommandReceipt {
	receipt.Failure = cloneFailure(receipt.Failure)
	return receipt
}

func cloneLedger(ledger contracts.CommandLedger) contracts.CommandLedger {
	ledger.Envelope = cloneEnvelope(ledger.Envelope)
	ledger.Receipts = append([]contracts.CommandReceipt(nil), ledger.Receipts...)
	for index := range ledger.Receipts {
		ledger.Receipts[index] = cloneReceipt(ledger.Receipts[index])
	}
	return ledger
}

func cloneFailure(failure *contracts.StructuredFailure) *contracts.StructuredFailure {
	if failure == nil {
		return nil
	}
	copy := *failure
	return &copy
}

func cloneTime(value *time.Time) *time.Time {
	if value == nil {
		return nil
	}
	copy := *value
	return &copy
}

func equalStrings(left, right []string) bool {
	if len(left) != len(right) {
		return false
	}
	for index := range left {
		if left[index] != right[index] {
			return false
		}
	}
	return true
}
