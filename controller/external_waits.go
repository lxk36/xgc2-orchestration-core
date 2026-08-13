package controller

import (
	"context"
	"errors"
	"fmt"
	"time"

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
	SubjectRef         string
	ConditionDigest    string
	Status             contracts.NodeWaitResolutionStatus
	Payload            map[string]any
	PayloadArtifactRef string
	Failure            *contracts.StructuredFailure
	ObservedAt         time.Time
	CommandID          string
}

// ResolveExternalWait atomically consumes the exact wait occurrence, resumes
// its pure node, and returns the Run to normal workflow driving. The persisted
// wait subject and condition digest are mandatory fences; names alone never
// identify an occurrence.
func (controller *Controller) ResolveExternalWait(ctx context.Context, request ResolveExternalWaitRequest) (contracts.Run, error) {
	if controller == nil || ctx == nil || !contracts.ValidIdentifier(request.RunID) ||
		!contracts.ValidIdentifier(request.SubjectRef) || !contracts.ValidDigest(request.ConditionDigest) ||
		!request.Status.Valid() || !contracts.ValidIdentifier(request.CommandID) || request.ObservedAt.IsZero() ||
		(request.PayloadArtifactRef != "" && !contracts.ValidIdentifier(request.PayloadArtifactRef)) {
		return contracts.Run{}, errors.New("external wait resolution identity, status, evidence, or time is invalid")
	}
	runRecord, err := controller.store.GetAggregate(ctx, runKey(request.RunID))
	if err != nil {
		return contracts.Run{}, err
	}
	run, err := decodeRun(runRecord)
	if err != nil || run.Status.Terminal() {
		return run, err
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
		ledger.Invocation.CurrentWaitRef != wait.SubjectRef {
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
	if request.Payload != nil {
		resolution.PayloadDigest, err = canonicaljson.DigestValue(request.Payload)
		if err != nil {
			return contracts.Run{}, err
		}
	}
	resolutionIdentityDigest, err := canonicaljson.DigestValue(map[string]any{
		"runId": request.RunID, "commandId": request.CommandID, "resolution": resolution,
	})
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
		"outputDigest": outputDigest, "resolutionIdentityDigest": resolutionIdentityDigest,
	}
	if failure != nil {
		eventType = "snapshot.node-failed"
		eventPayload = map[string]any{
			"nodeId": ledger.Invocation.NodeID, "waitSubjectRef": wait.SubjectRef,
			"failureCode": failure.Code, "resolutionIdentityDigest": resolutionIdentityDigest,
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
	if err := controller.commit(ctx, commandID, now,
		[]store.ExpectedRevision{
			{Key: runMutation.Key, Revision: runRecord.Revision},
			{Key: invocationMutation.Key, Revision: invocationRecord.Revision},
			{Key: snapshotMutation.Key, Revision: snapshotRevision},
		},
		[]store.AggregateRecord{runMutation, invocationMutation, snapshotMutation},
		append(append(runDecision.Events, invocationDecision.Events...), snapshotEvent),
		runDecision.Intents, runDecision.Run,
	); err != nil {
		if errors.Is(err, store.ErrIdentityConflict) {
			return controller.replayExternalWait(ctx, run.RunID, commandID, resolutionIdentityDigest)
		}
		return contracts.Run{}, err
	}
	driven, driveErr := controller.Drive(ctx, run.RunID)
	if errors.Is(driveErr, store.ErrRevisionConflict) || errors.Is(driveErr, store.ErrIdentityConflict) {
		// The external resolution above is already committed under its exact
		// request identity. A concurrent coordinator may win the following
		// internal phase command; return that durable Run so the caller can keep
		// advancing instead of exposing an internal CAS race as an ingress error.
		current, currentErr := controller.store.GetAggregate(ctx, runKey(run.RunID))
		if currentErr != nil {
			return contracts.Run{}, currentErr
		}
		return decodeRun(current)
	}
	return driven, driveErr
}

func (controller *Controller) replayExternalWait(
	ctx context.Context,
	runID, commandID, resolutionIdentityDigest string,
) (contracts.Run, error) {
	afterRevision := uint64(0)
	for {
		events, err := controller.store.EventsAfter(ctx, snapshotKey(runID), afterRevision, 500)
		if err != nil {
			return contracts.Run{}, err
		}
		for _, event := range events {
			if event.CommandID != commandID {
				continue
			}
			persisted, _ := event.Payload["resolutionIdentityDigest"].(string)
			if persisted != resolutionIdentityDigest {
				return contracts.Run{}, store.ErrIdentityConflict
			}
			record, err := controller.store.GetAggregate(ctx, runKey(runID))
			if err != nil {
				return contracts.Run{}, err
			}
			return decodeRun(record)
		}
		if len(events) < 500 {
			return contracts.Run{}, store.ErrIdentityConflict
		}
		afterRevision = events[len(events)-1].AggregateRevision
	}
}
