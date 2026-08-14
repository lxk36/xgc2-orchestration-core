package controller

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"time"

	"github.com/lxk36/xgc2-orchestration-core/durable/store"
	"github.com/lxk36/xgc2-orchestration-core/kernel/canonicaljson"
	"github.com/lxk36/xgc2-orchestration-core/kernel/effect"
	"github.com/lxk36/xgc2-orchestration-core/kernel/execution"
	"github.com/lxk36/xgc2-orchestration-core/sdk/go/contracts"
)

type TerminateRunRequest struct {
	RunID            string
	ExpectedRevision uint64
	Kind             contracts.TerminationKind
	RequestedBy      string
	ReasonCode       string
	Reason           string
	PrimaryFailure   *contracts.StructuredFailure
	CommandID        string
}

type TerminateRunResult struct {
	Run    contracts.Run `json:"run"`
	Replay bool          `json:"replay"`
}

// RequestRunTermination atomically freezes the exact operator/agent intent
// before any Invocation, child Run, or Effect is canceled. Replaying the same
// command returns the first durable outcome; changing its bytes fails closed.
func (controller *Controller) RequestRunTermination(ctx context.Context, request TerminateRunRequest) (TerminateRunResult, error) {
	return controller.requestRunTermination(ctx, request, "")
}

// RequestActiveRunTermination requires the exact canonical owner key and Run
// identity for owner-backed root ingress. Generic termination cannot be used
// to stop an arbitrary reserved Run.
func (controller *Controller) RequestActiveRunTermination(
	ctx context.Context, key contracts.ActiveOwnerKey, request TerminateRunRequest,
) (TerminateRunResult, error) {
	if controller == nil || ctx == nil || !contracts.ValidIdentifier(request.RunID) {
		return TerminateRunResult{}, errors.New("controller, context, and target Run are required")
	}
	run, err := controller.ValidateRunActiveOwnerKey(ctx, key, request.RunID)
	if err != nil {
		return TerminateRunResult{}, err
	}
	if run.Termination == nil && !run.Status.Terminal() {
		owner, ownerErr := controller.GetActiveRunOwner(ctx, key)
		if ownerErr != nil {
			return TerminateRunResult{}, ownerErr
		}
		if owner.State != contracts.ActiveRunOwnerActive || owner.RunID != run.RunID ||
			owner.Generation != run.ActiveOwnerGeneration || owner.PolicyRef != run.AdmissionPolicyRef ||
			owner.PolicyDigest != run.AdmissionPolicyDigest {
			return TerminateRunResult{}, fmt.Errorf("%w: key is not owned by the target Run", ErrActiveOwnerConflict)
		}
	}
	return controller.requestRunTermination(ctx, request, run.ActiveOwnerRef)
}

func (controller *Controller) requestRunTermination(ctx context.Context, request TerminateRunRequest, activeOwnerRef string) (TerminateRunResult, error) {
	if controller == nil || ctx == nil || !contracts.ValidIdentifier(request.RunID) ||
		!contracts.ValidIdentifier(request.RequestedBy) || !contracts.ValidIdentifier(request.ReasonCode) ||
		!contracts.ValidIdentifier(request.CommandID) || request.ExpectedRevision == 0 || !request.Kind.RequiresStopping() {
		return TerminateRunResult{}, errors.New("run termination request or identity is invalid")
	}
	record, err := controller.store.GetAggregate(ctx, runKey(request.RunID))
	if err != nil {
		return TerminateRunResult{}, err
	}
	current, err := decodeRun(record)
	if err != nil {
		return TerminateRunResult{}, err
	}
	if current.ActiveOwnerRef != activeOwnerRef {
		if current.ActiveOwnerRef != "" || activeOwnerRef != "" {
			return TerminateRunResult{}, fmt.Errorf("%w: owner-backed Run requires its exact active owner key", ErrReservedIngressDenied)
		}
	}
	commandScope, err := runCommandScope("run.terminate", current, runAggregateType, current.RunID)
	if err != nil {
		return TerminateRunResult{}, err
	}
	if current.Termination != nil {
		if terminationMatchesRequest(current.RunID, *current.Termination, request) {
			return TerminateRunResult{Run: current, Replay: true}, nil
		}
		if current.Termination.CommandID == request.CommandID {
			return TerminateRunResult{}, store.ErrIdentityConflict
		}
		return TerminateRunResult{}, execution.ErrRevisionConflict
	}
	if current.Status.Terminal() {
		return TerminateRunResult{}, errors.New("terminal run has no matching termination command")
	}
	if current.Revision != request.ExpectedRevision || record.Revision != request.ExpectedRevision {
		return TerminateRunResult{}, execution.ErrRevisionConflict
	}
	at := controller.clock.Now().UTC()
	if at.Before(current.UpdatedAt) {
		at = current.UpdatedAt
	}
	termination := &contracts.TerminationIntent{
		Kind: request.Kind, RequestedRevision: request.ExpectedRevision,
		RequestedBy: request.RequestedBy, ReasonCode: request.ReasonCode,
		Reason: request.Reason, PrimaryFailure: cloneStructuredFailure(request.PrimaryFailure),
		CommandID: request.CommandID, RequestedAt: at,
	}
	decision, err := execution.TransitionRun(current, execution.RunTransitionCommand{
		RunID: current.RunID, ExpectedRevision: current.Revision, To: contracts.RunStopping,
		Termination: termination, CommandID: request.CommandID, At: at,
	})
	if err != nil {
		return TerminateRunResult{}, err
	}
	mutation, err := aggregateMutation(runKey(current.RunID), record.Revision, decision.Run)
	if err != nil {
		return TerminateRunResult{}, err
	}
	identityDigest, err := canonicaljson.DigestValue(map[string]any{
		"runId": request.RunID, "requestedRevision": request.ExpectedRevision, "kind": request.Kind,
		"requestedBy": request.RequestedBy, "reasonCode": request.ReasonCode, "reason": request.Reason,
		"primaryFailure": request.PrimaryFailure, "commandId": request.CommandID,
	})
	if err != nil {
		return TerminateRunResult{}, err
	}
	outcome, err := canonicaljson.Marshal(decision.Run)
	if err != nil {
		return TerminateRunResult{}, err
	}
	intents := make([]store.IntentSeed, len(decision.Intents))
	for index := range decision.Intents {
		intents[index] = store.IntentSeed{Intent: decision.Intents[index], AvailableAt: at}
	}
	committed, err := controller.store.Commit(ctx, store.Transaction{
		CommandScope: commandScope, CommandID: request.CommandID, IdentityDigest: identityDigest,
		Expected:  []store.ExpectedRevision{{Key: mutation.Key, Revision: record.Revision}},
		Mutations: []store.AggregateRecord{mutation}, Events: decision.Events, Intents: intents,
		Outcome: outcome, At: at,
	})
	if err != nil {
		return TerminateRunResult{}, err
	}
	var durable contracts.Run
	if err := canonicaljson.UnmarshalStrict(committed.Outcome, &durable); err != nil {
		return TerminateRunResult{}, err
	}
	return TerminateRunResult{Run: durable, Replay: committed.Replay}, nil
}

func terminationMatchesRequest(runID string, intent contracts.TerminationIntent, request TerminateRunRequest) bool {
	left, leftErr := canonicaljson.DigestValue(map[string]any{
		"runId": runID, "requestedRevision": intent.RequestedRevision,
		"kind": intent.Kind, "requestedBy": intent.RequestedBy, "reasonCode": intent.ReasonCode,
		"reason": intent.Reason, "primaryFailure": intent.PrimaryFailure, "commandId": intent.CommandID,
	})
	right, rightErr := canonicaljson.DigestValue(map[string]any{
		"runId": request.RunID, "requestedRevision": request.ExpectedRevision,
		"kind": request.Kind, "requestedBy": request.RequestedBy, "reasonCode": request.ReasonCode,
		"reason": request.Reason, "primaryFailure": request.PrimaryFailure, "commandId": request.CommandID,
	})
	return leftErr == nil && rightErr == nil && left == right
}

func cloneStructuredFailure(failure *contracts.StructuredFailure) *contracts.StructuredFailure {
	if failure == nil {
		return nil
	}
	copy := *failure
	return &copy
}

func (controller *Controller) cancelOpenInvocations(ctx context.Context, run contracts.Run, at time.Time) error {
	snapshot, snapshotRevision, err := controller.GetSnapshot(ctx, run.RunID)
	if err != nil {
		return err
	}
	records, err := listAllAggregates(ctx, controller.store, invocationType)
	if err != nil {
		return err
	}
	sort.Slice(records, func(left, right int) bool { return records[left].Key.ID < records[right].Key.ID })
	for _, record := range records {
		ledger, decodeErr := decodeLedger(record)
		if decodeErr != nil {
			return decodeErr
		}
		if ledger.Invocation.RunID != run.RunID || ledger.Invocation.Status.Terminal() {
			continue
		}
		cancelAt := at
		if cancelAt.Before(ledger.Invocation.UpdatedAt) {
			cancelAt = ledger.Invocation.UpdatedAt
		}
		commandID := phaseCommand(run.RunID, "cancel-"+ledger.Invocation.NodeID, ledger.Invocation.Revision)
		decision, cancelErr := execution.CancelInvocation(ledger, execution.CancelInvocationCommand{
			InvocationID: ledger.Invocation.InvocationID, ExpectedRevision: ledger.Invocation.Revision,
			RunID: run.RunID, RunRevision: run.Revision, TerminationCommandID: run.Termination.CommandID,
			CommandID: commandID, At: cancelAt,
		})
		if cancelErr != nil {
			return cancelErr
		}
		var snapshotMutation *store.AggregateRecord
		var snapshotEvent *contracts.DomainEvent
		if snapshot.Waiting != nil && snapshot.Waiting.InvocationID == ledger.Invocation.InvocationID {
			nextSnapshot := snapshot
			nextSnapshot.Waiting = nil
			mutation, mutationErr := aggregateMutation(snapshotKey(run.RunID), snapshotRevision, nextSnapshot)
			if mutationErr != nil {
				return mutationErr
			}
			event, eventErr := aggregateEvent(mutation, "snapshot.wait-canceled", commandID, cancelAt, map[string]any{
				"invocationId":   ledger.Invocation.InvocationID,
				"waitGeneration": ledger.Invocation.WaitGeneration,
			})
			if eventErr != nil {
				return eventErr
			}
			snapshotMutation, snapshotEvent = &mutation, &event
			snapshot = nextSnapshot
		}
		if err := controller.commitInvocationDecision(
			ctx, record.Revision, decision, commandID, cancelAt, snapshotMutation, snapshotEvent,
		); err != nil {
			return err
		}
		if snapshotMutation != nil {
			snapshotRevision++
		}
	}
	return nil
}

func (controller *Controller) cancelPreparedEffects(ctx context.Context, run contracts.Run, at time.Time) error {
	records, err := listAllAggregates(ctx, controller.store, effectAggregateType)
	if err != nil {
		return err
	}
	sort.Slice(records, func(left, right int) bool { return records[left].Key.ID < records[right].Key.ID })
	for _, record := range records {
		current, decodeErr := decodeEffect(record)
		if decodeErr != nil {
			return decodeErr
		}
		if current.Intent.RunID != run.RunID || current.State != contracts.EffectPrepared {
			continue
		}
		cancelAt := at
		if cancelAt.Before(current.UpdatedAt) {
			cancelAt = current.UpdatedAt
		}
		commandID := phaseCommand(run.RunID, "cancel-effect-"+current.EffectID, current.Revision)
		decision, cancelErr := effect.CancelPrepared(current, effect.CancelPreparedCommand{
			EffectID: current.EffectID, ExpectedRevision: current.Revision, CommandID: commandID, At: cancelAt,
		})
		if cancelErr != nil {
			return cancelErr
		}
		mutation, mutationErr := aggregateMutation(effectKey(current.EffectID), record.Revision, decision.Effect)
		if mutationErr != nil {
			return mutationErr
		}
		if err := controller.commit(ctx, commandID, cancelAt,
			[]store.ExpectedRevision{{Key: mutation.Key, Revision: record.Revision}},
			[]store.AggregateRecord{mutation}, decision.Events, decision.Intents, decision.Effect,
		); err != nil {
			return err
		}
	}
	return nil
}

func (controller *Controller) terminateOpenChildren(ctx context.Context, run contracts.Run) error {
	records, err := listAllAggregates(ctx, controller.store, runAggregateType)
	if err != nil {
		return err
	}
	sort.Slice(records, func(left, right int) bool { return records[left].Key.ID < records[right].Key.ID })
	for _, record := range records {
		child, decodeErr := decodeRun(record)
		if decodeErr != nil {
			return decodeErr
		}
		if child.Parent == nil || child.Parent.ParentRunID != run.RunID || child.Status.Terminal() {
			continue
		}
		if child.Status != contracts.RunStopping {
			commandID := phaseCommand(run.RunID, "stop-child-"+child.RunID, child.Revision)
			if _, err := controller.RequestRunTermination(ctx, TerminateRunRequest{
				RunID: child.RunID, ExpectedRevision: child.Revision, Kind: run.Termination.Kind,
				RequestedBy: controller.ownerRef, ReasonCode: "parent.stopping",
				Reason:         fmt.Sprintf("parent Run %s is stopping", run.RunID),
				PrimaryFailure: cloneStructuredFailure(run.Termination.PrimaryFailure), CommandID: commandID,
			}); err != nil {
				return err
			}
		}
		driven, driveErr := controller.Drive(ctx, child.RunID)
		if driven.Status.Terminal() {
			continue
		}
		if driveErr == nil || errors.Is(driveErr, ErrRunClosureOpen) || errors.Is(driveErr, ErrRunWaiting) || errors.Is(driveErr, ErrAttemptLeaseActive) {
			return ErrRunClosureOpen
		}
		return driveErr
	}
	return nil
}

func (controller *Controller) terminationIntentsSettled(ctx context.Context, run contracts.Run, snapshot RunSnapshot) (bool, error) {
	identities, err := requiredTerminationIntentIDs(ctx, controller.store, run, snapshot)
	if err != nil {
		return false, err
	}
	for _, identity := range identities {
		intent, getErr := controller.store.GetIntent(ctx, identity)
		if errors.Is(getErr, store.ErrNotFound) {
			return false, fmt.Errorf("termination required intent %s is missing", identity)
		}
		if getErr != nil {
			return false, getErr
		}
		switch intent.Status {
		case store.IntentCompleted:
			continue
		case store.IntentDead:
			return false, fmt.Errorf("termination required intent %s is dead", identity)
		default:
			return false, nil
		}
	}
	return true, nil
}

// requiredTerminationIntentIDs mechanically derives every asynchronous
// identity that must settle before a stopping Run may publish closure proof.
// Each identity originates in the same transaction as its source transition.
func requiredTerminationIntentIDs(ctx context.Context, durable store.Store, run contracts.Run, snapshot RunSnapshot) ([]string, error) {
	identities := make([]string, 0, 4)
	runCleanup, err := execution.StableIntentID(contracts.IntentCleanup, run.RunID, run.Revision)
	if err != nil {
		return nil, err
	}
	identities = append(identities, runCleanup)
	if snapshot.ActionCall != nil {
		childResolution, stableErr := execution.StableIntentID(contracts.IntentChildResolution, snapshot.ActionCall.ChildRunID, 1)
		if stableErr != nil {
			return nil, stableErr
		}
		identities = append(identities, childResolution)
	}
	effects, err := listAllAggregates(ctx, durable, effectAggregateType)
	if err != nil {
		return nil, err
	}
	for _, record := range effects {
		current, decodeErr := decodeEffect(record)
		if decodeErr != nil {
			return nil, decodeErr
		}
		if current.Intent.RunID != run.RunID || !current.State.Terminal() || current.State == contracts.EffectCanceled {
			continue
		}
		if current.PrimaryTerminalRevision == 0 {
			return nil, fmt.Errorf("terminal effect %s lacks its exact terminal revision", current.EffectID)
		}
		waitResolution, stableErr := execution.StableIntentID(contracts.IntentWaitResolution, current.EffectID, current.PrimaryTerminalRevision)
		if stableErr != nil {
			return nil, stableErr
		}
		identities = append(identities, waitResolution)
	}
	sort.Strings(identities)
	for index, identity := range identities {
		if index > 0 && identities[index-1] == identity {
			return nil, fmt.Errorf("termination required intent %s is duplicated", identity)
		}
	}
	return identities, nil
}
