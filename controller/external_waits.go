package controller

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/lxk36/xgc2-orchestration-core/durable/store"
	"github.com/lxk36/xgc2-orchestration-core/kernel/canonicaljson"
	"github.com/lxk36/xgc2-orchestration-core/kernel/execution"
	workflowkernel "github.com/lxk36/xgc2-orchestration-core/kernel/workflow"
	"github.com/lxk36/xgc2-orchestration-core/sdk/go/contracts"
)

// ResolveExternalWaitRequest is the explicit ingress for a durable event,
// approval, or timer observation. Effect waits are resolved only from provider
// Receipts and child Action waits only from terminal child Runs, so callers
// cannot bypass either authority through this method.
type ResolveExternalWaitRequest struct {
	RunID              string
	InvocationID       string
	WaitGeneration     uint32
	SubjectRef         string
	ConditionDigest    string
	Status             contracts.NodeWaitResolutionStatus
	Payload            map[string]any
	PayloadArtifactRef string
	Failure            *contracts.StructuredFailure
	ObservedAt         time.Time
	CommandID          string
}

const (
	externalWaitResolutionSchema = "xgc.external-wait-resolution/v1"
	externalWaitOccurrenceSchema = "xgc.external-wait-occurrence/v1"
	externalWaitResolutionType   = "external-wait-resolution"
	externalWaitOccurrenceType   = "external-wait-occurrence"
)

var ErrExternalWaitConsumed = errors.New("external wait occurrence is already consumed")

// externalWaitResolutionReceipt is the one durable ingress receipt for a
// command. It stores the complete request identity, including the exact wait
// occurrence, rather than trying to reconstruct identity from mutable Run
// state after the occurrence has been consumed.
type externalWaitResolutionReceipt struct {
	SchemaVersion         string                             `json:"schemaVersion"`
	RunID                 string                             `json:"runId"`
	InvocationID          string                             `json:"invocationId"`
	WaitGeneration        uint32                             `json:"waitGeneration"`
	WaitKind              contracts.NodeWaitKind             `json:"waitKind"`
	SubjectRef            string                             `json:"subjectRef"`
	ConditionDigest       string                             `json:"conditionDigest"`
	Status                contracts.NodeWaitResolutionStatus `json:"status"`
	Payload               map[string]any                     `json:"payload,omitempty"`
	PayloadDigest         string                             `json:"payloadDigest,omitempty"`
	PayloadArtifactRef    string                             `json:"payloadArtifactRef,omitempty"`
	Failure               *contracts.StructuredFailure       `json:"failure,omitempty"`
	ObservedAt            time.Time                          `json:"observedAt"`
	CommandID             string                             `json:"commandId"`
	RequestIdentityDigest string                             `json:"requestIdentityDigest"`
}

// externalWaitOccurrenceConsumption is a direct, immutable index from an
// occurrence to the command that consumed it. A second command can therefore
// fail as consumed without scanning event history, even when a later wait has
// the same subject and condition.
type externalWaitOccurrenceConsumption struct {
	SchemaVersion         string `json:"schemaVersion"`
	RunID                 string `json:"runId"`
	InvocationID          string `json:"invocationId"`
	WaitGeneration        uint32 `json:"waitGeneration"`
	CommandID             string `json:"commandId"`
	RequestIdentityDigest string `json:"requestIdentityDigest"`
}

// ResolveExternalWait atomically consumes the exact wait occurrence, resumes
// its pure node, and returns the Run to normal workflow driving. The persisted
// wait subject and condition digest are mandatory fences; names alone never
// identify an occurrence.
func (controller *Controller) ResolveExternalWait(ctx context.Context, request ResolveExternalWaitRequest) (contracts.Run, error) {
	return controller.resolveExternalWait(ctx, request, "")
}

// ResolveActiveExternalWait requires the exact canonical owner key and Run ID
// before consuming a signal, approval, or timer occurrence for an
// owner-backed reserved Run. Generic external wait ingress cannot unlock it.
func (controller *Controller) ResolveActiveExternalWait(
	ctx context.Context, key contracts.ActiveOwnerKey, request ResolveExternalWaitRequest,
) (contracts.Run, error) {
	if controller == nil || ctx == nil || !contracts.ValidIdentifier(request.RunID) {
		return contracts.Run{}, errors.New("controller, context, and target Run are required")
	}
	_, _, ownerRef, err := normalizeActiveOwnerKey(key)
	if err != nil {
		return contracts.Run{}, err
	}
	return controller.resolveExternalWait(ctx, request, ownerRef)
}

func (controller *Controller) resolveExternalWait(
	ctx context.Context, request ResolveExternalWaitRequest, activeOwnerRef string,
) (contracts.Run, error) {
	if controller == nil || ctx == nil || !contracts.ValidIdentifier(request.RunID) ||
		!contracts.ValidIdentifier(request.InvocationID) || request.WaitGeneration == 0 ||
		!contracts.ValidIdentifier(request.SubjectRef) || !contracts.ValidDigest(request.ConditionDigest) ||
		!request.Status.Valid() || !contracts.ValidIdentifier(request.CommandID) || request.ObservedAt.IsZero() ||
		(request.PayloadArtifactRef != "" && !contracts.ValidIdentifier(request.PayloadArtifactRef)) {
		return contracts.Run{}, errors.New("external wait resolution identity, status, evidence, or time is invalid")
	}
	if err := controller.authorizeExternalWait(ctx, request.RunID, activeOwnerRef); err != nil {
		// Preserve generic command-ledger identity conflicts when a replay changes
		// only the Run ID to an unknown Run. Active ingress never gets this
		// fallback: its exact key and target Run must authorize before receipt
		// lookup or any mutation.
		if activeOwnerRef != "" || !errors.Is(err, store.ErrNotFound) {
			return contracts.Run{}, err
		}
	}
	requestIdentityDigest, payloadDigest, err := externalWaitRequestIdentity(request)
	if err != nil {
		return contracts.Run{}, err
	}
	if receipt, found, err := controller.getExternalWaitReceipt(ctx, request.CommandID); err != nil {
		return contracts.Run{}, err
	} else if found {
		if receipt.RequestIdentityDigest != requestIdentityDigest {
			return contracts.Run{}, store.ErrIdentityConflict
		}
		if err := controller.verifyExternalWaitOccurrence(ctx, receipt); err != nil {
			return contracts.Run{}, err
		}
		return controller.replayExternalWait(ctx, receipt)
	}
	occurrenceKey, err := externalWaitOccurrenceKey(request.RunID, request.InvocationID, request.WaitGeneration)
	if err != nil {
		return contracts.Run{}, err
	}
	if consumption, found, err := controller.getExternalWaitConsumption(ctx, occurrenceKey); err != nil {
		return contracts.Run{}, err
	} else if found {
		if err := controller.verifyExternalWaitConsumption(ctx, consumption); err != nil {
			return contracts.Run{}, err
		}
		return contracts.Run{}, ErrExternalWaitConsumed
	}
	if err := validateExternalWaitRequestEvidence(request); err != nil {
		return contracts.Run{}, err
	}
	runRecord, err := controller.store.GetAggregate(ctx, runKey(request.RunID))
	if err != nil {
		return contracts.Run{}, err
	}
	run, err := decodeRun(runRecord)
	if err != nil {
		return contracts.Run{}, err
	}
	if run.Status.Terminal() {
		return contracts.Run{}, ErrExternalWaitConsumed
	}
	if run.Status != contracts.RunWaiting {
		return contracts.Run{}, fmt.Errorf("run %s is %s instead of waiting", run.RunID, run.Status)
	}
	snapshot, snapshotRevision, err := controller.GetSnapshot(ctx, run.RunID)
	if err != nil {
		return contracts.Run{}, err
	}
	if snapshot.ActionCall != nil || snapshot.Waiting == nil || snapshot.Waiting.Wait == nil {
		return contracts.Run{}, errors.New("run snapshot has no externally resolvable wait")
	}
	wait := *snapshot.Waiting.Wait
	if wait.Kind == contracts.NodeWaitEffect || wait.SubjectRef != request.SubjectRef ||
		wait.ConditionDigest != request.ConditionDigest {
		return contracts.Run{}, errors.New("external resolution does not match the exact durable wait")
	}
	if snapshot.NextNode < 0 || snapshot.NextNode >= len(snapshot.NodeOrder) {
		return contracts.Run{}, errors.New("external wait node position is outside the pinned plan")
	}
	invocationID, err := execution.StableInvocationID(run.RunID, snapshot.NodeOrder[snapshot.NextNode])
	if err != nil {
		return contracts.Run{}, err
	}
	if invocationID != request.InvocationID {
		return contracts.Run{}, errors.New("external resolution does not match the exact durable wait occurrence")
	}
	invocationRecord, err := controller.store.GetAggregate(ctx, store.AggregateKey{Type: invocationType, ID: invocationID})
	if err != nil {
		return contracts.Run{}, err
	}
	ledger, err := decodeLedger(invocationRecord)
	if err != nil {
		return contracts.Run{}, err
	}
	attempt := activeAttempt(ledger)
	if attempt == nil || ledger.Invocation.Status != contracts.InvocationWaiting ||
		ledger.Invocation.CurrentWaitRef != wait.SubjectRef || ledger.Invocation.WaitGeneration != request.WaitGeneration {
		return contracts.Run{}, errors.New("invocation does not own the external wait")
	}
	now := request.ObservedAt.UTC()
	if now.Before(run.UpdatedAt) || now.Before(attempt.UpdatedAt) {
		return contracts.Run{}, errors.New("external wait resolution time moves backwards")
	}

	workflowNode, exists := findWorkflowNode(snapshot.Definition, ledger.Invocation.NodeID)
	if !exists {
		return contracts.Run{}, errors.New("waiting workflow node disappeared from the pinned definition")
	}
	inputs, err := workflowkernel.ResolveNodeInputs(
		snapshot.Definition, workflowNode.NodeID, snapshot.Inputs, snapshot.Trigger, snapshot.Scope, snapshot.NodeOutputs, nil,
	)
	if err != nil {
		return contracts.Run{}, err
	}
	inputDigest, err := canonicaljson.DigestValue(inputs)
	if err != nil || inputDigest != ledger.Invocation.ResolvedInputDigest {
		return contracts.Run{}, errors.New("resumed node inputs differ from the frozen invocation")
	}
	resolution := contracts.NodeWaitResolution{
		Kind: wait.Kind, SubjectRef: wait.SubjectRef, ConditionDigest: wait.ConditionDigest,
		Status: request.Status, Payload: request.Payload, PayloadArtifactRef: request.PayloadArtifactRef,
		Failure: request.Failure, ObservedAt: now,
	}
	resolution.PayloadDigest = payloadDigest
	receipt := externalWaitResolutionReceipt{
		SchemaVersion: externalWaitResolutionSchema, RunID: request.RunID,
		InvocationID: request.InvocationID, WaitGeneration: request.WaitGeneration, WaitKind: wait.Kind,
		SubjectRef: request.SubjectRef, ConditionDigest: request.ConditionDigest, Status: request.Status,
		Payload: request.Payload, PayloadDigest: payloadDigest, PayloadArtifactRef: request.PayloadArtifactRef,
		Failure: cloneStructuredFailure(request.Failure), ObservedAt: request.ObservedAt,
		CommandID: request.CommandID, RequestIdentityDigest: requestIdentityDigest,
	}
	receiptMutation, err := aggregateMutation(externalWaitResolutionKey(request.CommandID), 0, receipt)
	if err != nil {
		return contracts.Run{}, err
	}
	consumption := externalWaitOccurrenceConsumption{
		SchemaVersion: externalWaitOccurrenceSchema, RunID: request.RunID,
		InvocationID: request.InvocationID, WaitGeneration: request.WaitGeneration,
		CommandID: request.CommandID, RequestIdentityDigest: requestIdentityDigest,
	}
	consumptionMutation, err := aggregateMutation(occurrenceKey, 0, consumption)
	if err != nil {
		return contracts.Run{}, err
	}
	receiptEvent, err := aggregateEvent(
		receiptMutation, "external-wait-resolution.recorded", request.CommandID, now,
		map[string]any{
			"runId": request.RunID, "invocationId": request.InvocationID,
			"waitGeneration": request.WaitGeneration, "requestIdentityDigest": requestIdentityDigest,
		},
	)
	if err != nil {
		return contracts.Run{}, err
	}
	consumptionEvent, err := aggregateEvent(
		consumptionMutation, "external-wait-occurrence.consumed", request.CommandID, now,
		map[string]any{"commandId": request.CommandID, "requestIdentityDigest": requestIdentityDigest},
	)
	if err != nil {
		return contracts.Run{}, err
	}

	var result contracts.NodeResult
	failure := request.Failure
	if request.Status == contracts.NodeWaitResolvedSucceeded {
		result, err = controller.nodes.Resume(ctx, contracts.NodeResumeRequest{
			InvocationID: ledger.Invocation.InvocationID, RunID: run.RunID,
			NodeID: ledger.Invocation.NodeID, TypeRef: ledger.Invocation.TypeRef,
			DescriptorDigest: ledger.Invocation.DescriptorDigest,
			AttemptID:        attempt.AttemptID, AttemptOrdinal: attempt.Ordinal,
			Input: inputs, InputDigest: inputDigest, Wait: wait, Resolution: resolution,
			RequestedAt: now,
		})
		if err != nil {
			local := structuredFailure("node.resume", err)
			failure = &local
		} else if result.Status == contracts.NodeResultFailed {
			failure = result.Failure
		}
	} else if failure == nil {
		failure = &contracts.StructuredFailure{
			Class: contracts.FailureCanceled, Code: "wait." + string(request.Status),
			Message: "external wait was " + string(request.Status),
		}
	}
	if failure == nil && result.Status != contracts.NodeResultSucceeded {
		failure = &contracts.StructuredFailure{Class: contracts.FailurePermanent, Code: "wait.resolution", Message: "external wait did not produce a terminal node result"}
	}

	to := contracts.InvocationSucceeded
	outputDigest := result.OutputDigest
	if failure != nil {
		to = contracts.InvocationFailed
		outputDigest = ""
	}
	commandID := request.CommandID
	invocationDecision, err := execution.ResolveInvocationWait(ledger, execution.ResolveInvocationWaitCommand{
		InvocationID: ledger.Invocation.InvocationID, ExpectedRevision: ledger.Invocation.Revision,
		AttemptID: attempt.AttemptID, ExpectedAttemptRevision: attempt.Revision,
		WaitRef: wait.SubjectRef, WaitGeneration: ledger.Invocation.WaitGeneration,
		To: to, OutputRefsDigest: outputDigest, Failure: failure, CommandID: commandID, At: now,
	})
	if err != nil {
		return contracts.Run{}, err
	}
	snapshot.Waiting = nil
	if failure == nil {
		snapshot.NodeOutputs[ledger.Invocation.NodeID] = result.Output
		snapshot.NextNode++
	} else {
		snapshot.Failure = failure
	}
	snapshotMutation, err := aggregateMutation(snapshotKey(run.RunID), snapshotRevision, snapshot)
	if err != nil {
		return contracts.Run{}, err
	}
	eventType := "snapshot.node-resumed"
	eventPayload := map[string]any{
		"nodeId": ledger.Invocation.NodeID, "waitSubjectRef": wait.SubjectRef,
		"outputDigest": outputDigest, "resolutionIdentityDigest": requestIdentityDigest,
	}
	if failure != nil {
		eventType = "snapshot.node-failed"
		eventPayload = map[string]any{
			"nodeId": ledger.Invocation.NodeID, "waitSubjectRef": wait.SubjectRef,
			"failureCode": failure.Code, "resolutionIdentityDigest": requestIdentityDigest,
		}
	}
	snapshotEvent, err := aggregateEvent(snapshotMutation, eventType, commandID, now, eventPayload)
	if err != nil {
		return contracts.Run{}, err
	}
	invocationMutation, err := aggregateMutation(store.AggregateKey{Type: invocationType, ID: ledger.Invocation.InvocationID}, invocationRecord.Revision, invocationDecision.Ledger)
	if err != nil {
		return contracts.Run{}, err
	}
	runCommand := execution.RunTransitionCommand{
		RunID: run.RunID, ExpectedRevision: run.Revision, To: contracts.RunRunning,
		CommandID: commandID, At: now,
	}
	if failure != nil {
		runCommand.To = contracts.RunStopping
		runCommand.Termination = &contracts.TerminationIntent{
			Kind: contracts.TerminationFailed, RequestedBy: controller.ownerRef,
			ReasonCode: failure.Code, Reason: failure.Message, PrimaryFailure: failure,
			CommandID: commandID, RequestedAt: now,
		}
	}
	runDecision, err := execution.TransitionRun(run, runCommand)
	if err != nil {
		return contracts.Run{}, err
	}
	runMutation, err := aggregateMutation(runKey(run.RunID), runRecord.Revision, runDecision.Run)
	if err != nil {
		return contracts.Run{}, err
	}
	expected := []store.ExpectedRevision{
		{Key: runMutation.Key, Revision: runRecord.Revision},
		{Key: invocationMutation.Key, Revision: invocationRecord.Revision},
		{Key: snapshotMutation.Key, Revision: snapshotRevision},
		{Key: receiptMutation.Key, Revision: 0},
		{Key: consumptionMutation.Key, Revision: 0},
	}
	mutations := []store.AggregateRecord{
		runMutation, invocationMutation, snapshotMutation, receiptMutation, consumptionMutation,
	}
	events := append(append(runDecision.Events, invocationDecision.Events...), snapshotEvent)
	events = append(events, receiptEvent, consumptionEvent)
	intents := make([]store.IntentSeed, len(runDecision.Intents))
	for index := range runDecision.Intents {
		intents[index] = store.IntentSeed{Intent: runDecision.Intents[index], AvailableAt: now}
	}
	outcome, err := canonicaljson.Marshal(runDecision.Run)
	if err != nil {
		return contracts.Run{}, err
	}
	committed, err := controller.store.Commit(ctx, store.Transaction{
		CommandID: commandID, IdentityDigest: requestIdentityDigest, Expected: expected,
		Mutations: mutations, Events: events, Intents: intents, Outcome: outcome, At: now,
	})
	if err != nil {
		if errors.Is(err, store.ErrRevisionConflict) || errors.Is(err, store.ErrIdentityConflict) {
			if durableReceipt, found, getErr := controller.getExternalWaitReceipt(ctx, request.CommandID); getErr != nil {
				return contracts.Run{}, getErr
			} else if found {
				if durableReceipt.RequestIdentityDigest != requestIdentityDigest {
					return contracts.Run{}, store.ErrIdentityConflict
				}
				if err := controller.verifyExternalWaitOccurrence(ctx, durableReceipt); err != nil {
					return contracts.Run{}, err
				}
				return controller.replayExternalWait(ctx, durableReceipt)
			}
			if durableConsumption, found, getErr := controller.getExternalWaitConsumption(ctx, occurrenceKey); getErr != nil {
				return contracts.Run{}, getErr
			} else if found {
				if err := controller.verifyExternalWaitConsumption(ctx, durableConsumption); err != nil {
					return contracts.Run{}, err
				}
				return contracts.Run{}, ErrExternalWaitConsumed
			}
		}
		return contracts.Run{}, err
	}
	var durableRun contracts.Run
	if err := canonicaljson.UnmarshalStrict(committed.Outcome, &durableRun); err != nil {
		return contracts.Run{}, err
	}
	return controller.driveAfterExternalWait(ctx, durableRun.RunID)
}

func externalWaitResolutionKey(commandID string) store.AggregateKey {
	return store.AggregateKey{Type: externalWaitResolutionType, ID: commandID}
}

func (controller *Controller) authorizeExternalWait(ctx context.Context, runID, activeOwnerRef string) error {
	run, err := controller.GetRun(ctx, runID)
	if err != nil {
		return err
	}
	if run.ActiveOwnerRef != activeOwnerRef {
		if run.ActiveOwnerRef != "" || activeOwnerRef != "" {
			return fmt.Errorf("%w: owner-backed Run requires its exact active owner key", ErrReservedIngressDenied)
		}
		return nil
	}
	if activeOwnerRef == "" || run.Status.Terminal() {
		return nil
	}
	ownerRecord, err := controller.store.GetAggregate(ctx, activeOwnerKey(activeOwnerRef))
	if err != nil {
		return err
	}
	owner, err := decodeActiveRunOwner(ownerRecord)
	if err != nil {
		return err
	}
	if owner.State != contracts.ActiveRunOwnerActive || owner.RunID != run.RunID ||
		owner.Key.NamespaceID != run.NamespaceID || owner.Generation != run.ActiveOwnerGeneration ||
		owner.PolicyRef != run.AdmissionPolicyRef || owner.PolicyDigest != run.AdmissionPolicyDigest {
		return fmt.Errorf("%w: key is not owned by the target Run", ErrActiveOwnerConflict)
	}
	return nil
}

func externalWaitOccurrenceKey(runID, invocationID string, waitGeneration uint32) (store.AggregateKey, error) {
	digest, err := canonicaljson.DigestValue(map[string]any{
		"schemaVersion": externalWaitOccurrenceSchema, "runId": runID,
		"invocationId": invocationID, "waitGeneration": waitGeneration,
	})
	if err != nil {
		return store.AggregateKey{}, err
	}
	return store.AggregateKey{Type: externalWaitOccurrenceType, ID: "xwo-" + digest[len("sha256:"):]}, nil
}

func externalWaitRequestIdentity(request ResolveExternalWaitRequest) (string, string, error) {
	payloadDigest := ""
	var err error
	if request.Payload != nil {
		payloadDigest, err = canonicaljson.DigestValue(request.Payload)
		if err != nil {
			return "", "", err
		}
	}
	digest, err := canonicaljson.DigestValue(map[string]any{
		"schemaVersion": externalWaitResolutionSchema,
		"runId":         request.RunID, "invocationId": request.InvocationID,
		"waitGeneration": request.WaitGeneration, "subjectRef": request.SubjectRef,
		"conditionDigest": request.ConditionDigest, "status": request.Status,
		"payload": request.Payload, "payloadArtifactRef": request.PayloadArtifactRef,
		"failure": request.Failure, "observedAt": request.ObservedAt, "commandId": request.CommandID,
	})
	return digest, payloadDigest, err
}

func (controller *Controller) getExternalWaitReceipt(ctx context.Context, commandID string) (externalWaitResolutionReceipt, bool, error) {
	record, err := controller.store.GetAggregate(ctx, externalWaitResolutionKey(commandID))
	if errors.Is(err, store.ErrNotFound) {
		return externalWaitResolutionReceipt{}, false, nil
	}
	if err != nil {
		return externalWaitResolutionReceipt{}, false, err
	}
	if record.Revision != 1 {
		return externalWaitResolutionReceipt{}, false, store.ErrCorrupt
	}
	var receipt externalWaitResolutionReceipt
	if err := canonicaljson.UnmarshalStrict(record.Payload, &receipt); err != nil {
		return externalWaitResolutionReceipt{}, false, errors.Join(store.ErrCorrupt, err)
	}
	if err := validateExternalWaitReceipt(receipt); err != nil {
		return externalWaitResolutionReceipt{}, false, errors.Join(store.ErrCorrupt, err)
	}
	return receipt, true, nil
}

func (controller *Controller) getExternalWaitConsumption(
	ctx context.Context, key store.AggregateKey,
) (externalWaitOccurrenceConsumption, bool, error) {
	record, err := controller.store.GetAggregate(ctx, key)
	if errors.Is(err, store.ErrNotFound) {
		return externalWaitOccurrenceConsumption{}, false, nil
	}
	if err != nil {
		return externalWaitOccurrenceConsumption{}, false, err
	}
	if record.Revision != 1 {
		return externalWaitOccurrenceConsumption{}, false, store.ErrCorrupt
	}
	var consumption externalWaitOccurrenceConsumption
	if err := canonicaljson.UnmarshalStrict(record.Payload, &consumption); err != nil {
		return externalWaitOccurrenceConsumption{}, false, errors.Join(store.ErrCorrupt, err)
	}
	if consumption.SchemaVersion != externalWaitOccurrenceSchema ||
		!contracts.ValidIdentifier(consumption.RunID) || !contracts.ValidIdentifier(consumption.InvocationID) ||
		consumption.WaitGeneration == 0 || !contracts.ValidIdentifier(consumption.CommandID) ||
		!contracts.ValidDigest(consumption.RequestIdentityDigest) {
		return externalWaitOccurrenceConsumption{}, false, store.ErrCorrupt
	}
	return consumption, true, nil
}

func validateExternalWaitReceipt(receipt externalWaitResolutionReceipt) error {
	if receipt.SchemaVersion != externalWaitResolutionSchema || !contracts.ValidIdentifier(receipt.RunID) ||
		!contracts.ValidIdentifier(receipt.InvocationID) || receipt.WaitGeneration == 0 ||
		!receipt.WaitKind.Valid() || receipt.WaitKind == contracts.NodeWaitEffect ||
		!contracts.ValidIdentifier(receipt.SubjectRef) || !contracts.ValidDigest(receipt.ConditionDigest) ||
		!receipt.Status.Valid() || !contracts.ValidIdentifier(receipt.CommandID) || receipt.ObservedAt.IsZero() ||
		(receipt.PayloadArtifactRef != "" && !contracts.ValidIdentifier(receipt.PayloadArtifactRef)) ||
		!contracts.ValidDigest(receipt.RequestIdentityDigest) {
		return errors.New("external wait resolution receipt is invalid")
	}
	request := ResolveExternalWaitRequest{
		RunID: receipt.RunID, InvocationID: receipt.InvocationID, WaitGeneration: receipt.WaitGeneration,
		SubjectRef: receipt.SubjectRef, ConditionDigest: receipt.ConditionDigest, Status: receipt.Status,
		Payload: receipt.Payload, PayloadArtifactRef: receipt.PayloadArtifactRef,
		Failure: receipt.Failure, ObservedAt: receipt.ObservedAt, CommandID: receipt.CommandID,
	}
	if err := validateExternalWaitRequestEvidence(request); err != nil {
		return err
	}
	digest, payloadDigest, err := externalWaitRequestIdentity(request)
	if err != nil || digest != receipt.RequestIdentityDigest || payloadDigest != receipt.PayloadDigest {
		return errors.New("external wait resolution receipt digest is invalid")
	}
	return nil
}

func validateExternalWaitRequestEvidence(request ResolveExternalWaitRequest) error {
	switch request.Status {
	case contracts.NodeWaitResolvedSucceeded:
		if request.Payload == nil || request.Failure != nil {
			return errors.New("successful external wait resolution requires payload and no failure")
		}
	case contracts.NodeWaitResolvedFailed:
		if request.Payload != nil || request.PayloadArtifactRef != "" || request.Failure == nil {
			return errors.New("failed external wait resolution requires only a failure")
		}
	case contracts.NodeWaitResolvedCanceled:
		if request.Payload != nil || request.PayloadArtifactRef != "" || request.Failure != nil {
			return errors.New("canceled external wait resolution carries no evidence")
		}
	}
	if request.Failure != nil {
		failure := request.Failure
		if !failure.Class.Valid() || !contracts.ValidIdentifier(failure.Code) || failure.Message == "" ||
			len(failure.Message) > 4096 || !utf8.ValidString(failure.Message) ||
			strings.TrimSpace(failure.Message) != failure.Message ||
			(failure.EvidenceRef != "" && !contracts.ValidIdentifier(failure.EvidenceRef)) {
			return errors.New("external wait resolution failure is invalid")
		}
	}
	return nil
}

func (controller *Controller) verifyExternalWaitOccurrence(ctx context.Context, receipt externalWaitResolutionReceipt) error {
	key, err := externalWaitOccurrenceKey(receipt.RunID, receipt.InvocationID, receipt.WaitGeneration)
	if err != nil {
		return err
	}
	consumption, found, err := controller.getExternalWaitConsumption(ctx, key)
	if err != nil {
		return err
	}
	if !found || consumption.RunID != receipt.RunID || consumption.InvocationID != receipt.InvocationID ||
		consumption.WaitGeneration != receipt.WaitGeneration || consumption.CommandID != receipt.CommandID ||
		consumption.RequestIdentityDigest != receipt.RequestIdentityDigest {
		return store.ErrCorrupt
	}
	return nil
}

func (controller *Controller) verifyExternalWaitConsumption(ctx context.Context, consumption externalWaitOccurrenceConsumption) error {
	receipt, found, err := controller.getExternalWaitReceipt(ctx, consumption.CommandID)
	if err != nil {
		return err
	}
	if !found || receipt.RunID != consumption.RunID || receipt.InvocationID != consumption.InvocationID ||
		receipt.WaitGeneration != consumption.WaitGeneration ||
		receipt.RequestIdentityDigest != consumption.RequestIdentityDigest {
		return store.ErrCorrupt
	}
	return nil
}

func (controller *Controller) replayExternalWait(
	ctx context.Context, receipt externalWaitResolutionReceipt,
) (contracts.Run, error) {
	record, err := controller.store.GetAggregate(ctx, runKey(receipt.RunID))
	if err != nil {
		return contracts.Run{}, err
	}
	current, err := decodeRun(record)
	if err != nil {
		return contracts.Run{}, err
	}
	// Replays return durable state without starting new orchestration work or
	// exposing a post-commit Drive race as part of ingress identity.
	return current, nil
}

func (controller *Controller) driveAfterExternalWait(ctx context.Context, runID string) (contracts.Run, error) {
	record, err := controller.store.GetAggregate(ctx, runKey(runID))
	if err != nil {
		return contracts.Run{}, err
	}
	current, err := decodeRun(record)
	if err != nil || current.Status.Terminal() || current.Status == contracts.RunWaiting {
		return current, err
	}
	driven, driveErr := controller.Drive(ctx, runID)
	if driveErr == nil {
		return driven, nil
	}
	// A concurrent exact replay can observe the resolution commit and race the
	// first caller while it atomically publishes the terminal Run and releases
	// its owner. The losing drive may have read the old owner generation; the
	// newly published terminal Run is nevertheless the authoritative outcome.
	if record, readErr := controller.store.GetAggregate(ctx, runKey(runID)); readErr == nil {
		if current, decodeErr := decodeRun(record); decodeErr == nil && current.Status.Terminal() {
			return current, nil
		}
	}
	if errors.Is(driveErr, store.ErrRevisionConflict) || errors.Is(driveErr, store.ErrIdentityConflict) ||
		errors.Is(driveErr, ErrRunWaiting) || errors.Is(driveErr, ErrRunClosureOpen) ||
		errors.Is(driveErr, ErrAttemptLeaseActive) {
		record, err := controller.store.GetAggregate(ctx, runKey(runID))
		if err != nil {
			return contracts.Run{}, err
		}
		return decodeRun(record)
	}
	return driven, driveErr
}
