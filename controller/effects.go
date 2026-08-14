package controller

import (
	"context"
	"errors"
	"time"

	"github.com/lxk36/xgc2-orchestration-core/durable/store"
	"github.com/lxk36/xgc2-orchestration-core/kernel/canonicaljson"
	"github.com/lxk36/xgc2-orchestration-core/kernel/effect"
	"github.com/lxk36/xgc2-orchestration-core/kernel/execution"
	"github.com/lxk36/xgc2-orchestration-core/sdk/go/contracts"
)

// BeginEffectRequest carries private dispatch credentials only at the
// controller boundary. The durable command ledger stores their hashes, never
// the raw idempotency key or capability token.
type BeginEffectRequest struct {
	EffectID        string
	CommandID       string
	RequestID       string
	IdempotencyKey  string
	Action          string
	ActorRef        string
	SourceRef       string
	ReasonCode      string
	Reason          string
	Risk            contracts.CommandRisk
	Fence           contracts.TargetFence
	ManifestDigest  string
	Deadline        time.Time
	CancellationID  string
	CapabilityToken string
}

type BeginEffectResult struct {
	Effect   contracts.EffectRecord  `json:"effect"`
	Ledger   contracts.CommandLedger `json:"ledger"`
	IntentID string                  `json:"intentId"`
	Replay   bool                    `json:"replay"`
}

type ObserveEffectRequest struct {
	EffectID  string
	Receipt   contracts.CommandReceipt
	CommandID string
}

type ObserveEffectResult struct {
	Effect contracts.EffectRecord  `json:"effect"`
	Ledger contracts.CommandLedger `json:"ledger"`
	Replay bool                    `json:"replay"`
}

type ObserveEffectCompensationRequest struct {
	EffectID  string
	Receipt   contracts.CommandReceipt
	CommandID string
}

type ReconcileEffectCompensationRequest struct {
	EffectID         string
	ExpectedRevision uint64
	EvidenceDigest   string
	ReconciledBy     string
	ReasonCode       string
	CommandID        string
}

type ReconcileEffectCompensationResult struct {
	Effect contracts.EffectRecord `json:"effect"`
	Replay bool                   `json:"replay"`
}

func (controller *Controller) GetEffect(ctx context.Context, effectID string) (contracts.EffectRecord, error) {
	record, err := controller.store.GetAggregate(ctx, effectKey(effectID))
	if err != nil {
		return contracts.EffectRecord{}, err
	}
	return decodeEffect(record)
}

func (controller *Controller) ListEffects(ctx context.Context, afterEffectID string, limit int) ([]contracts.EffectRecord, error) {
	records, err := controller.store.ListAggregates(ctx, effectAggregateType, afterEffectID, limit)
	if err != nil {
		return nil, err
	}
	result := make([]contracts.EffectRecord, 0, len(records))
	for _, record := range records {
		current, decodeErr := decodeEffect(record)
		if decodeErr != nil {
			return nil, decodeErr
		}
		result = append(result, current)
	}
	return result, nil
}

func (controller *Controller) GetCommandLedger(ctx context.Context, commandID string) (contracts.CommandLedger, error) {
	if ctx == nil || !contracts.ValidIdentifier(commandID) {
		return contracts.CommandLedger{}, errors.New("command ledger identity is invalid")
	}
	records, err := listAllAggregates(ctx, controller.store, commandLedgerType)
	if err != nil {
		return contracts.CommandLedger{}, err
	}
	var result contracts.CommandLedger
	found := false
	for _, record := range records {
		ledger, decodeErr := decodeCommandLedger(record)
		if decodeErr != nil {
			return contracts.CommandLedger{}, decodeErr
		}
		if ledger.Envelope.CommandID != commandID {
			continue
		}
		if found {
			return contracts.CommandLedger{}, errors.New("command id is ambiguous across durable scopes")
		}
		result, found = ledger, true
	}
	if !found {
		return contracts.CommandLedger{}, store.ErrNotFound
	}
	return result, nil
}

// GetEffectCommandLedger resolves the ledger in the exact Effect operation
// scope. Use this method when command IDs may be reused by another Effect.
func (controller *Controller) GetEffectCommandLedger(
	ctx context.Context, effectID, commandID string,
) (contracts.CommandLedger, error) {
	current, err := controller.GetEffect(ctx, effectID)
	if err != nil {
		return contracts.CommandLedger{}, err
	}
	primary := current.CommandID == commandID
	compensation := current.CompensationCommandID == commandID
	if primary && compensation {
		return contracts.CommandLedger{}, errors.New("command id is ambiguous across Effect operation scopes")
	}
	operation := ""
	if compensation {
		operation = "effect.compensation.begin"
	} else if primary {
		operation = "effect.begin"
	} else {
		return contracts.CommandLedger{}, store.ErrNotFound
	}
	return controller.getScopedEffectCommandLedger(ctx, current, operation, commandID)
}

func (controller *Controller) getScopedEffectCommandLedger(
	ctx context.Context, current contracts.EffectRecord, operation, commandID string,
) (contracts.CommandLedger, error) {
	scope, err := controller.effectCommandScope(ctx, operation, current)
	if err != nil {
		return contracts.CommandLedger{}, err
	}
	key, err := commandLedgerKey(scope, commandID)
	if err != nil {
		return contracts.CommandLedger{}, err
	}
	record, err := controller.store.GetAggregate(ctx, key)
	if err != nil {
		return contracts.CommandLedger{}, err
	}
	return decodeCommandLedger(record)
}

// BeginEffect atomically moves a prepared Effect to applying, creates its
// immutable command ledger, and emits the durable outbox item. It never calls
// the provider adapter itself.
func (controller *Controller) BeginEffect(ctx context.Context, request BeginEffectRequest) (BeginEffectResult, error) {
	if ctx == nil {
		return BeginEffectResult{}, errors.New("begin effect context is required")
	}
	effectRecord, err := controller.store.GetAggregate(ctx, effectKey(request.EffectID))
	if err != nil {
		return BeginEffectResult{}, err
	}
	current, err := decodeEffect(effectRecord)
	if err != nil {
		return BeginEffectResult{}, err
	}
	commandScope, err := controller.effectCommandScope(ctx, "effect.begin", current)
	if err != nil {
		return BeginEffectResult{}, err
	}
	at := controller.clock.Now().UTC()
	envelope, err := buildCommandEnvelope(current.Intent, request)
	if err != nil {
		return BeginEffectResult{}, err
	}
	if current.CommandID == request.CommandID {
		ledger, replayErr := controller.getScopedEffectCommandLedger(ctx, current, "effect.begin", request.CommandID)
		if replayErr != nil {
			return BeginEffectResult{}, replayErr
		}
		if err := effect.ValidateEnvelopeForIntent(envelope, current.Intent, time.Time{}, true); err != nil || !samePublicEnvelope(ledger.Envelope, envelope) {
			return BeginEffectResult{}, effect.ErrCommandConflict
		}
		return BeginEffectResult{Effect: current, Ledger: ledger, Replay: true}, nil
	}
	decision, err := effect.Begin(current, effect.BeginCommand{
		EffectID: request.EffectID, ExpectedRevision: current.Revision,
		Envelope: envelope, CommandID: request.CommandID, At: at,
	})
	if err != nil {
		return BeginEffectResult{}, err
	}
	effectMutation, err := aggregateMutation(effectKey(request.EffectID), effectRecord.Revision, decision.Effect)
	if err != nil {
		return BeginEffectResult{}, err
	}
	durableLedger := *decision.Ledger
	durableLedger.Envelope.IdempotencyKey = ""
	durableLedger.Envelope.CapabilityToken = ""
	ledgerKey, err := commandLedgerKey(commandScope, request.CommandID)
	if err != nil {
		return BeginEffectResult{}, err
	}
	ledgerMutation, err := aggregateMutation(ledgerKey, 0, durableLedger)
	if err != nil {
		return BeginEffectResult{}, err
	}
	ledgerEvent, err := aggregateEvent(ledgerMutation, "command.enqueued", request.CommandID, at, map[string]any{
		"effectId": request.EffectID, "commandIdentityDigest": envelope.IdentityDigest,
	})
	if err != nil {
		return BeginEffectResult{}, err
	}
	result := BeginEffectResult{Effect: decision.Effect, Ledger: durableLedger, IntentID: decision.Intents[0].Identity}
	outcome, err := canonicaljson.Marshal(result)
	if err != nil {
		return BeginEffectResult{}, err
	}
	identity, err := canonicaljson.DigestValue(map[string]any{
		"operation": "effect.begin", "effectId": request.EffectID,
		"command": envelope,
	})
	if err != nil {
		return BeginEffectResult{}, err
	}
	committed, err := controller.store.Commit(ctx, store.Transaction{
		CommandScope: commandScope, CommandID: request.CommandID, IdentityDigest: identity,
		Expected: []store.ExpectedRevision{
			{Key: effectMutation.Key, Revision: effectRecord.Revision},
			{Key: ledgerMutation.Key, Revision: 0},
		},
		Mutations: []store.AggregateRecord{effectMutation, ledgerMutation},
		Events:    append(decision.Events, ledgerEvent),
		Intents:   []store.IntentSeed{{Intent: decision.Intents[0], AvailableAt: at}},
		Outcome:   outcome, At: at,
	})
	if err != nil {
		return BeginEffectResult{}, err
	}
	if committed.Replay {
		if err := canonicaljson.UnmarshalStrict(committed.Outcome, &result); err != nil {
			return BeginEffectResult{}, err
		}
		result.Replay = true
	}
	return result, nil
}

// ObserveEffect appends exactly one immutable provider receipt and atomically
// advances both the Effect and its CommandLedger.
func (controller *Controller) ObserveEffect(ctx context.Context, request ObserveEffectRequest) (ObserveEffectResult, error) {
	if ctx == nil {
		return ObserveEffectResult{}, errors.New("observe effect context is required")
	}
	effectRecord, err := controller.store.GetAggregate(ctx, effectKey(request.EffectID))
	if err != nil {
		return ObserveEffectResult{}, err
	}
	current, err := decodeEffect(effectRecord)
	if err != nil {
		return ObserveEffectResult{}, err
	}
	ledgerScope, err := controller.effectCommandScope(ctx, "effect.begin", current)
	if err != nil {
		return ObserveEffectResult{}, err
	}
	commandScope, err := controller.effectCommandScope(ctx, "effect.observe", current)
	if err != nil {
		return ObserveEffectResult{}, err
	}
	ledgerKey, err := commandLedgerKey(ledgerScope, request.Receipt.CommandID)
	if err != nil {
		return ObserveEffectResult{}, err
	}
	ledgerRecord, err := controller.store.GetAggregate(ctx, ledgerKey)
	if err != nil {
		return ObserveEffectResult{}, err
	}
	ledger, err := decodeCommandLedger(ledgerRecord)
	if err != nil {
		return ObserveEffectResult{}, err
	}
	for _, prior := range ledger.Receipts {
		if prior.ReceiptID != request.Receipt.ReceiptID {
			continue
		}
		if !sameReceipt(prior, request.Receipt) {
			return ObserveEffectResult{}, effect.ErrReceiptConflict
		}
		return ObserveEffectResult{Effect: current, Ledger: ledger, Replay: true}, nil
	}
	decision, err := effect.Observe(current, ledger, effect.ObserveCommand{
		EffectID: request.EffectID, ExpectedRevision: current.Revision,
		Receipt: request.Receipt, CommandID: request.CommandID,
	})
	if err != nil {
		return ObserveEffectResult{}, err
	}
	effectMutation, err := aggregateMutation(effectKey(request.EffectID), effectRecord.Revision, decision.Effect)
	if err != nil {
		return ObserveEffectResult{}, err
	}
	ledgerMutation, err := aggregateMutation(ledgerKey, ledgerRecord.Revision, *decision.Ledger)
	if err != nil {
		return ObserveEffectResult{}, err
	}
	ledgerEvent, err := aggregateEvent(ledgerMutation, "command.receipt."+string(request.Receipt.Status), request.CommandID, request.Receipt.ObservedAt, map[string]any{
		"effectId": request.EffectID, "receiptId": request.Receipt.ReceiptID,
		"sequence": request.Receipt.Sequence, "status": request.Receipt.Status,
	})
	if err != nil {
		return ObserveEffectResult{}, err
	}
	result := ObserveEffectResult{Effect: decision.Effect, Ledger: *decision.Ledger}
	outcome, err := canonicaljson.Marshal(result)
	if err != nil {
		return ObserveEffectResult{}, err
	}
	identity, err := canonicaljson.DigestValue(map[string]any{
		"operation": "effect.observe", "effectId": request.EffectID,
		"receipt": request.Receipt,
	})
	if err != nil {
		return ObserveEffectResult{}, err
	}
	resolutionSeeds, err := waitResolutionSeeds(decision.Effect, request.Receipt.ObservedAt)
	if err != nil {
		return ObserveEffectResult{}, err
	}
	committed, err := controller.store.Commit(ctx, store.Transaction{
		CommandScope: commandScope, CommandID: request.CommandID, IdentityDigest: identity,
		Expected: []store.ExpectedRevision{
			{Key: effectMutation.Key, Revision: effectRecord.Revision},
			{Key: ledgerMutation.Key, Revision: ledgerRecord.Revision},
		},
		Mutations: []store.AggregateRecord{effectMutation, ledgerMutation},
		Events:    append(decision.Events, ledgerEvent),
		Intents:   resolutionSeeds,
		Outcome:   outcome, At: request.Receipt.ObservedAt,
	})
	if err != nil {
		return ObserveEffectResult{}, err
	}
	if committed.Replay {
		if err := canonicaljson.UnmarshalStrict(committed.Outcome, &result); err != nil {
			return ObserveEffectResult{}, err
		}
		result.Replay = true
	}
	return result, nil
}

// BeginEffectCompensation durably fences a compensation command and creates
// its immutable ledger before any provider is called. Reusing the same command
// is an idempotent replay; changing it after dispatch is rejected.
func (controller *Controller) BeginEffectCompensation(ctx context.Context, request BeginEffectRequest) (BeginEffectResult, error) {
	if ctx == nil {
		return BeginEffectResult{}, errors.New("begin effect compensation context is required")
	}
	effectRecord, err := controller.store.GetAggregate(ctx, effectKey(request.EffectID))
	if err != nil {
		return BeginEffectResult{}, err
	}
	current, err := decodeEffect(effectRecord)
	if err != nil {
		return BeginEffectResult{}, err
	}
	commandScope, err := controller.effectCommandScope(ctx, "effect.compensation.begin", current)
	if err != nil {
		return BeginEffectResult{}, err
	}
	compensationIntent := current.Intent
	compensationIntent.TargetRef = current.ExternalIdentity
	compensationIntent.Intent = map[string]any{
		"effectId": current.EffectID, "externalIdentityRef": current.ExternalIdentity,
		"ownerRunRef": current.Intent.RunID, "originalIntentDigest": current.Intent.IntentDigest,
	}
	compensationIntent.IntentArtifactRef = ""
	compensationIntent.IntentDigest, err = canonicaljson.DigestValue(compensationIntent.Intent)
	if err != nil {
		return BeginEffectResult{}, err
	}
	envelope, err := buildCommandEnvelope(compensationIntent, request)
	if err != nil {
		return BeginEffectResult{}, err
	}
	if current.CompensationCommandID == request.CommandID {
		ledger, replayErr := controller.getScopedEffectCommandLedger(ctx, current, "effect.compensation.begin", request.CommandID)
		if replayErr != nil {
			return BeginEffectResult{}, replayErr
		}
		if !samePublicEnvelope(ledger.Envelope, envelope) {
			return BeginEffectResult{}, effect.ErrCommandConflict
		}
		return BeginEffectResult{Effect: current, Ledger: ledger, Replay: true}, nil
	}
	at := controller.clock.Now().UTC()
	if at.Before(current.UpdatedAt) {
		at = current.UpdatedAt
	}
	decision, err := effect.TransitionCompensation(current, effect.CompensationCommand{
		EffectID: current.EffectID, ExpectedRevision: current.Revision,
		To: contracts.EffectCompensationRunning, CompensationCommandID: request.CommandID,
		CommandID: request.CommandID, At: at,
	})
	if err != nil {
		return BeginEffectResult{}, err
	}
	durableLedger := contracts.CommandLedger{Envelope: envelope, Receipts: []contracts.CommandReceipt{}}
	durableLedger.Envelope.IdempotencyKey = ""
	durableLedger.Envelope.CapabilityToken = ""
	if err := effect.ValidateLedger(durableLedger); err != nil {
		return BeginEffectResult{}, err
	}
	effectMutation, err := aggregateMutation(effectKey(current.EffectID), effectRecord.Revision, decision.Effect)
	if err != nil {
		return BeginEffectResult{}, err
	}
	ledgerKey, err := commandLedgerKey(commandScope, request.CommandID)
	if err != nil {
		return BeginEffectResult{}, err
	}
	ledgerMutation, err := aggregateMutation(ledgerKey, 0, durableLedger)
	if err != nil {
		return BeginEffectResult{}, err
	}
	ledgerEvent, err := aggregateEvent(ledgerMutation, "compensation-command.enqueued", request.CommandID, at, map[string]any{
		"effectId": current.EffectID, "commandIdentityDigest": envelope.IdentityDigest,
	})
	if err != nil {
		return BeginEffectResult{}, err
	}
	result := BeginEffectResult{Effect: decision.Effect, Ledger: durableLedger}
	outcome, err := canonicaljson.Marshal(result)
	if err != nil {
		return BeginEffectResult{}, err
	}
	identity, err := canonicaljson.DigestValue(map[string]any{
		"operation": "effect.compensation.begin", "effectId": current.EffectID, "command": envelope,
	})
	if err != nil {
		return BeginEffectResult{}, err
	}
	committed, err := controller.store.Commit(ctx, store.Transaction{
		CommandScope: commandScope, CommandID: request.CommandID, IdentityDigest: identity,
		Expected: []store.ExpectedRevision{
			{Key: effectMutation.Key, Revision: effectRecord.Revision},
			{Key: ledgerMutation.Key, Revision: 0},
		},
		Mutations: []store.AggregateRecord{effectMutation, ledgerMutation},
		Events:    append(decision.Events, ledgerEvent), Outcome: outcome, At: at,
	})
	if err != nil {
		return BeginEffectResult{}, err
	}
	if committed.Replay {
		if err := canonicaljson.UnmarshalStrict(committed.Outcome, &result); err != nil {
			return BeginEffectResult{}, err
		}
		result.Replay = true
	}
	return result, nil
}

// ObserveEffectCompensation persists one provider receipt without mutating the
// primary Effect result. Terminal receipts close the independent Saga state.
func (controller *Controller) ObserveEffectCompensation(ctx context.Context, request ObserveEffectCompensationRequest) (ObserveEffectResult, error) {
	if ctx == nil {
		return ObserveEffectResult{}, errors.New("observe effect compensation context is required")
	}
	effectRecord, err := controller.store.GetAggregate(ctx, effectKey(request.EffectID))
	if err != nil {
		return ObserveEffectResult{}, err
	}
	current, err := decodeEffect(effectRecord)
	if err != nil {
		return ObserveEffectResult{}, err
	}
	ledgerScope, err := controller.effectCommandScope(ctx, "effect.compensation.begin", current)
	if err != nil {
		return ObserveEffectResult{}, err
	}
	commandScope, err := controller.effectCommandScope(ctx, "effect.compensation.observe", current)
	if err != nil {
		return ObserveEffectResult{}, err
	}
	ledgerKey, err := commandLedgerKey(ledgerScope, request.Receipt.CommandID)
	if err != nil {
		return ObserveEffectResult{}, err
	}
	ledgerRecord, err := controller.store.GetAggregate(ctx, ledgerKey)
	if err != nil {
		return ObserveEffectResult{}, err
	}
	ledger, err := decodeCommandLedger(ledgerRecord)
	if err != nil {
		return ObserveEffectResult{}, err
	}
	for _, prior := range ledger.Receipts {
		if prior.ReceiptID != request.Receipt.ReceiptID {
			continue
		}
		if !sameReceipt(prior, request.Receipt) {
			return ObserveEffectResult{}, effect.ErrReceiptConflict
		}
		return ObserveEffectResult{Effect: current, Ledger: ledger, Replay: true}, nil
	}
	decision, err := effect.ObserveCompensation(current, ledger, effect.ObserveCompensationCommand{
		EffectID: current.EffectID, ExpectedRevision: current.Revision,
		Receipt: request.Receipt, CommandID: request.CommandID,
	})
	if err != nil {
		return ObserveEffectResult{}, err
	}
	effectMutation, err := aggregateMutation(effectKey(current.EffectID), effectRecord.Revision, decision.Effect)
	if err != nil {
		return ObserveEffectResult{}, err
	}
	ledgerMutation, err := aggregateMutation(ledgerKey, ledgerRecord.Revision, *decision.Ledger)
	if err != nil {
		return ObserveEffectResult{}, err
	}
	ledgerEvent, err := aggregateEvent(ledgerMutation, "compensation-command.receipt."+string(request.Receipt.Status), request.CommandID, request.Receipt.ObservedAt, map[string]any{
		"effectId": current.EffectID, "receiptId": request.Receipt.ReceiptID,
		"sequence": request.Receipt.Sequence, "status": request.Receipt.Status,
	})
	if err != nil {
		return ObserveEffectResult{}, err
	}
	result := ObserveEffectResult{Effect: decision.Effect, Ledger: *decision.Ledger}
	outcome, err := canonicaljson.Marshal(result)
	if err != nil {
		return ObserveEffectResult{}, err
	}
	identity, err := canonicaljson.DigestValue(map[string]any{
		"operation": "effect.compensation.observe", "effectId": current.EffectID, "receipt": request.Receipt,
	})
	if err != nil {
		return ObserveEffectResult{}, err
	}
	committed, err := controller.store.Commit(ctx, store.Transaction{
		CommandScope: commandScope, CommandID: request.CommandID, IdentityDigest: identity,
		Expected: []store.ExpectedRevision{
			{Key: effectMutation.Key, Revision: effectRecord.Revision},
			{Key: ledgerMutation.Key, Revision: ledgerRecord.Revision},
		},
		Mutations: []store.AggregateRecord{effectMutation, ledgerMutation},
		Events:    append(decision.Events, ledgerEvent), Outcome: outcome, At: request.Receipt.ObservedAt,
	})
	if err != nil {
		return ObserveEffectResult{}, err
	}
	if committed.Replay {
		if err := canonicaljson.UnmarshalStrict(committed.Outcome, &result); err != nil {
			return ObserveEffectResult{}, err
		}
		result.Replay = true
	}
	return result, nil
}

// ReconcileEffectCompensation records exact content-addressed evidence that a
// Required compensation no longer owns its external mutation. The request is
// fenced to the precise failed/canceled/retry-wait Effect revision.
func (controller *Controller) ReconcileEffectCompensation(
	ctx context.Context,
	request ReconcileEffectCompensationRequest,
) (ReconcileEffectCompensationResult, error) {
	if controller == nil || ctx == nil || !contracts.ValidIdentifier(request.EffectID) || request.ExpectedRevision == 0 ||
		!contracts.ValidDigest(request.EvidenceDigest) || !contracts.ValidIdentifier(request.ReconciledBy) ||
		!contracts.ValidIdentifier(request.ReasonCode) || !contracts.ValidIdentifier(request.CommandID) {
		return ReconcileEffectCompensationResult{}, errors.New("compensation reconciliation request is invalid")
	}
	record, err := controller.store.GetAggregate(ctx, effectKey(request.EffectID))
	if err != nil {
		return ReconcileEffectCompensationResult{}, err
	}
	current, err := decodeEffect(record)
	if err != nil {
		return ReconcileEffectCompensationResult{}, err
	}
	commandScope, err := controller.effectCommandScope(ctx, "effect.compensation.reconcile", current)
	if err != nil {
		return ReconcileEffectCompensationResult{}, err
	}
	if current.CompensationReconciliation != nil {
		proof := current.CompensationReconciliation
		if proof.CommandID == request.CommandID && proof.RequestedRevision == request.ExpectedRevision &&
			proof.EvidenceDigest == request.EvidenceDigest && proof.ReconciledBy == request.ReconciledBy &&
			proof.ReasonCode == request.ReasonCode {
			return ReconcileEffectCompensationResult{Effect: current, Replay: true}, nil
		}
		if proof.CommandID == request.CommandID {
			return ReconcileEffectCompensationResult{}, store.ErrIdentityConflict
		}
		return ReconcileEffectCompensationResult{}, effect.ErrRevisionConflict
	}
	if current.Revision != request.ExpectedRevision || record.Revision != request.ExpectedRevision {
		return ReconcileEffectCompensationResult{}, effect.ErrRevisionConflict
	}
	at := controller.clock.Now().UTC()
	if at.Before(current.UpdatedAt) {
		at = current.UpdatedAt
	}
	decision, err := effect.ReconcileCompensation(current, effect.ReconcileCompensationCommand{
		EffectID: request.EffectID, ExpectedRevision: request.ExpectedRevision,
		EvidenceDigest: request.EvidenceDigest, ReconciledBy: request.ReconciledBy,
		ReasonCode: request.ReasonCode, CommandID: request.CommandID, At: at,
	})
	if err != nil {
		return ReconcileEffectCompensationResult{}, err
	}
	mutation, err := aggregateMutation(effectKey(request.EffectID), record.Revision, decision.Effect)
	if err != nil {
		return ReconcileEffectCompensationResult{}, err
	}
	result := ReconcileEffectCompensationResult{Effect: decision.Effect}
	outcome, err := canonicaljson.Marshal(result)
	if err != nil {
		return ReconcileEffectCompensationResult{}, err
	}
	identity, err := canonicaljson.DigestValue(map[string]any{
		"operation": "effect.compensation.reconcile", "effectId": request.EffectID,
		"expectedRevision": request.ExpectedRevision, "evidenceDigest": request.EvidenceDigest,
		"reconciledBy": request.ReconciledBy, "reasonCode": request.ReasonCode, "commandId": request.CommandID,
	})
	if err != nil {
		return ReconcileEffectCompensationResult{}, err
	}
	committed, err := controller.store.Commit(ctx, store.Transaction{
		CommandScope: commandScope, CommandID: request.CommandID, IdentityDigest: identity,
		Expected:  []store.ExpectedRevision{{Key: mutation.Key, Revision: record.Revision}},
		Mutations: []store.AggregateRecord{mutation}, Events: decision.Events, Outcome: outcome, At: at,
	})
	if err != nil {
		return ReconcileEffectCompensationResult{}, err
	}
	if err := canonicaljson.UnmarshalStrict(committed.Outcome, &result); err != nil {
		return ReconcileEffectCompensationResult{}, err
	}
	result.Replay = committed.Replay
	return result, nil
}

func waitResolutionSeeds(current contracts.EffectRecord, at time.Time) ([]store.IntentSeed, error) {
	if !current.State.Terminal() {
		return nil, nil
	}
	payload := map[string]any{
		"effectId": current.EffectID, "runId": current.Intent.RunID,
		"invocationId": current.Intent.InvocationID, "effectState": current.State,
	}
	digest, err := canonicaljson.DigestValue(payload)
	if err != nil {
		return nil, err
	}
	identity, err := execution.StableIntentID(contracts.IntentWaitResolution, current.EffectID, current.Revision)
	if err != nil {
		return nil, err
	}
	return []store.IntentSeed{{Intent: contracts.DurableIntent{
		Kind: contracts.IntentWaitResolution, Identity: identity, AggregateID: current.EffectID,
		PayloadDigest: digest, Payload: payload,
	}, AvailableAt: at}}, nil
}

func buildCommandEnvelope(intent contracts.EffectIntent, request BeginEffectRequest) (contracts.CommandEnvelope, error) {
	deadline := request.Deadline.UTC()
	if deadline.IsZero() {
		deadline = intent.Deadline.UTC()
	}
	idempotencyHash, err := execution.PrivateTokenDigest(request.IdempotencyKey)
	if err != nil {
		return contracts.CommandEnvelope{}, err
	}
	capabilityHash := ""
	if len(intent.RequiredCapabilityRefs) != 0 {
		capabilityHash, err = execution.PrivateTokenDigest(request.CapabilityToken)
		if err != nil {
			return contracts.CommandEnvelope{}, err
		}
	}
	envelope := contracts.CommandEnvelope{
		CommandID: request.CommandID, RequestID: request.RequestID,
		EffectID: intent.EffectID, IdempotencyKey: request.IdempotencyKey, IdempotencyKeyHash: idempotencyHash,
		NamespaceID: intent.NamespaceID, TargetRef: intent.TargetRef,
		Action: request.Action, ActorRef: request.ActorRef, SourceRef: request.SourceRef,
		ReasonCode: request.ReasonCode, Reason: request.Reason, Risk: request.Risk, Fence: request.Fence,
		PayloadDigest: intent.IntentDigest, PayloadArtifactRef: intent.IntentArtifactRef,
		PolicyDigest: intent.PolicyDigest, DescriptorDigest: intent.DescriptorDigest,
		ManifestDigest: request.ManifestDigest, Deadline: deadline, CancellationID: request.CancellationID,
		RequiredCapabilityRefs: append([]string(nil), intent.RequiredCapabilityRefs...),
		CapabilityTokenHash:    capabilityHash, CapabilityToken: request.CapabilityToken,
	}
	envelope.IdentityDigest, err = effect.CommandIdentityDigest(envelope)
	if err != nil {
		return contracts.CommandEnvelope{}, err
	}
	return envelope, nil
}

func samePublicEnvelope(left, right contracts.CommandEnvelope) bool {
	left.IdempotencyKey, left.CapabilityToken = "", ""
	right.IdempotencyKey, right.CapabilityToken = "", ""
	leftDigest, leftErr := canonicaljson.DigestValue(left)
	rightDigest, rightErr := canonicaljson.DigestValue(right)
	return leftErr == nil && rightErr == nil && leftDigest == rightDigest
}

func sameReceipt(left, right contracts.CommandReceipt) bool {
	leftDigest, leftErr := canonicaljson.DigestValue(left)
	rightDigest, rightErr := canonicaljson.DigestValue(right)
	return leftErr == nil && rightErr == nil && leftDigest == rightDigest
}
