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

func (controller *Controller) executeClaimedActionCall(
	ctx context.Context,
	runRecord store.AggregateRecord,
	run contracts.Run,
	snapshot RunSnapshot,
	snapshotRevision uint64,
	invocationRevision uint64,
	ledger contracts.InvocationLedger,
	descriptor contracts.NodeDescriptor,
	workflowNode contracts.WorkflowNodeDefinition,
	leaseToken string,
	now time.Time,
) (contracts.Run, error) {
	if controller.actions == nil {
		return controller.failClaimed(ctx, runRecord, run, snapshot, snapshotRevision, invocationRevision, ledger, leaseToken,
			"action-call.catalog-unavailable", errors.New("child Action resolver is unavailable"), now)
	}
	if workflowNode.CallAction == nil || descriptor.Mode != contracts.NodeWaiting || descriptor.Determinism != contracts.NodeRecorded {
		return controller.failClaimed(ctx, runRecord, run, snapshot, snapshotRevision, invocationRevision, ledger, leaseToken,
			"action-call.contract", errors.New("child Action node does not use the recorded waiting contract"), now)
	}
	call := *workflowNode.CallAction
	actionVersion, definition, err := controller.actions.ResolveAction(ctx, run.NamespaceID, call.TargetActionRef)
	if err != nil {
		return controller.failClaimed(ctx, runRecord, run, snapshot, snapshotRevision, invocationRevision, ledger, leaseToken,
			"action-call.resolve", err, now)
	}
	if !actionVersion.Ref().Equal(call.TargetActionRef) {
		return controller.failClaimed(ctx, runRecord, run, snapshot, snapshotRevision, invocationRevision, ledger, leaseToken,
			"action-call.pin", errors.New("child Action resolver returned another immutable Action"), now)
	}
	plan, err := controller.Compile(definition)
	if err != nil || controller.validatePinnedAction(actionVersion, definition, plan) != nil {
		if err == nil {
			err = controller.validatePinnedAction(actionVersion, definition, plan)
		}
		return controller.failClaimed(ctx, runRecord, run, snapshot, snapshotRevision, invocationRevision, ledger, leaseToken,
			"action-call.definition", err, now)
	}
	if !schemaEqual(actionVersion.InputSchema, call.InputSchema) ||
		!schemaEqual(actionVersion.ResultSchema, call.ResultSchema) ||
		!schemaEqual(definition.TriggerSchema, call.TriggerSchema) ||
		!schemaEqual(definition.ScopeSchema, call.ScopeSchema) {
		return controller.failClaimed(ctx, runRecord, run, snapshot, snapshotRevision, invocationRevision, ledger, leaseToken,
			"action-call.schema", errors.New("child Action schemas differ from the frozen call contract"), now)
	}
	childInputs, childTrigger, childScope, err := workflowkernel.ResolveCallActionContext(
		snapshot.Definition, workflowNode.NodeID, snapshot.Inputs, snapshot.Trigger, snapshot.Scope, snapshot.NodeOutputs,
	)
	if err != nil {
		return controller.failClaimed(ctx, runRecord, run, snapshot, snapshotRevision, invocationRevision, ledger, leaseToken,
			"action-call.mapping", err, now)
	}
	mappingDigest, err := canonicaljson.DigestValue(call)
	if err != nil {
		return contracts.Run{}, err
	}
	childRunID, err := execution.StableChildRunID(ledger.Invocation.InvocationID, call.TargetActionRef)
	if err != nil {
		return contracts.Run{}, err
	}
	triggerEventID, err := execution.StableChildTriggerEventID(childRunID)
	if err != nil {
		return contracts.Run{}, err
	}
	triggerSchemaDigest, err := canonicaljson.DigestValue(call.TriggerSchema)
	if err != nil {
		return contracts.Run{}, err
	}
	inputDigest, err := canonicaljson.DigestValue(childInputs)
	if err != nil {
		return contracts.Run{}, err
	}
	triggerDigest, err := canonicaljson.DigestValue(childTrigger)
	if err != nil {
		return contracts.Run{}, err
	}
	scopeDigest, err := canonicaljson.DigestValue(childScope)
	if err != nil {
		return contracts.Run{}, err
	}
	parentLink := &contracts.ParentRunLink{
		ParentRunID: run.RunID, ParentInvocationID: ledger.Invocation.InvocationID,
		CallNodeID: workflowNode.NodeID, MappingDigest: mappingDigest,
	}
	child, err := controller.invokeChild(ctx, InvokeRequest{
		RunID: childRunID, NamespaceID: run.NamespaceID, Action: actionVersion, Definition: definition,
		Trigger: contracts.TriggerEvent{
			EventID: triggerEventID, Kind: contracts.TriggerActionCall, Version: "v1",
			OccurredAt: ledger.Invocation.CreatedAt, ReceivedAt: ledger.Invocation.CreatedAt,
			SourceRef: controller.ownerRef, ActorRef: run.ActorRef, SubjectRef: run.RunID,
			PayloadSchemaDigest: triggerSchemaDigest, Payload: childTrigger,
		},
		Candidate: childInputs, CandidateOrigin: contracts.OriginParentMap,
		CandidateRef: ledger.Invocation.InvocationID, MappingDigest: mappingDigest,
		Scope: childScope, ScopeRef: digestRef("scope", scopeDigest), CorrelationRef: run.RootRunID,
		CommandID: phaseCommand(childRunID, "invoke", 1), Parent: parentLink, RootRunID: run.RootRunID,
	})
	if err != nil {
		return controller.failClaimed(ctx, runRecord, run, snapshot, snapshotRevision, invocationRevision, ledger, leaseToken,
			"action-call.invoke", err, now)
	}
	if child.Run.Parent == nil || child.Run.Parent.ParentInvocationID != ledger.Invocation.InvocationID ||
		child.Run.RootRunID != run.RootRunID {
		return controller.failClaimed(ctx, runRecord, run, snapshot, snapshotRevision, invocationRevision, ledger, leaseToken,
			"action-call.lineage", errors.New("child Run replay returned conflicting lineage"), now)
	}
	attempt := activeAttempt(ledger)
	if attempt == nil {
		return contracts.Run{}, errors.New("child Action invocation has no active attempt")
	}
	commandID := phaseCommand(run.RunID, "wait-child-"+workflowNode.NodeID, ledger.Invocation.Revision)
	fence := execution.AttemptFence{
		InvocationID: ledger.Invocation.InvocationID, InvocationRevision: ledger.Invocation.Revision,
		AttemptID: attempt.AttemptID, AttemptRevision: attempt.Revision, LeaseToken: leaseToken, At: now,
	}
	invocationDecision, err := execution.TransitionInvocation(ledger, execution.TransitionInvocationCommand{
		Fence: fence, To: contracts.InvocationWaiting, AttemptTo: contracts.AttemptWaiting,
		WaitRef: childRunID, WaitGeneration: 1, CommandID: commandID,
	})
	if err != nil {
		return contracts.Run{}, err
	}
	snapshot.Waiting = nil
	snapshot.ActionCall = &ActionCallWait{
		InvocationID: ledger.Invocation.InvocationID, NodeID: workflowNode.NodeID,
		ChildRunID: childRunID, ActionRef: call.TargetActionRef, MappingDigest: mappingDigest,
		InputDigest: inputDigest, TriggerDigest: triggerDigest, ScopeDigest: scopeDigest,
	}
	snapshotMutation, err := aggregateMutation(snapshotKey(run.RunID), snapshotRevision, snapshot)
	if err != nil {
		return contracts.Run{}, err
	}
	runDecision, err := execution.TransitionRun(run, execution.RunTransitionCommand{
		RunID: run.RunID, ExpectedRevision: run.Revision, To: contracts.RunWaiting,
		CommandID: commandID, At: now,
	})
	if err != nil {
		return contracts.Run{}, err
	}
	intent, err := childResolutionIntent(childRunID, run.RunID, ledger.Invocation.InvocationID)
	if err != nil {
		return contracts.Run{}, err
	}
	runDecision.Intents = append(runDecision.Intents, intent)
	if err := controller.commitWaiting(
		ctx, runRecord.Revision, invocationRevision, snapshotRevision,
		invocationDecision, runDecision, snapshotMutation, nil, nil, commandID, now,
	); err != nil {
		return contracts.Run{}, err
	}
	childRun, driveErr := controller.Drive(ctx, childRunID)
	if childRun.Status.Terminal() || driveErr == nil || errors.Is(driveErr, ErrRunWaiting) ||
		errors.Is(driveErr, ErrAttemptLeaseActive) || errors.Is(driveErr, ErrRunClosureOpen) {
		return runDecision.Run, ErrRunWaiting
	}
	return runDecision.Run, driveErr
}

func schemaEqual(left, right contracts.Schema) bool {
	leftDigest, leftErr := canonicaljson.DigestValue(left)
	rightDigest, rightErr := canonicaljson.DigestValue(right)
	return leftErr == nil && rightErr == nil && leftDigest == rightDigest
}

func childResolutionIntent(childRunID, parentRunID, invocationID string) (contracts.DurableIntent, error) {
	payload := map[string]any{
		"childRunId": childRunID, "parentRunId": parentRunID, "invocationId": invocationID,
	}
	payloadDigest, err := canonicaljson.DigestValue(payload)
	if err != nil {
		return contracts.DurableIntent{}, err
	}
	identity, err := execution.StableIntentID(contracts.IntentChildResolution, childRunID, 1)
	if err != nil {
		return contracts.DurableIntent{}, err
	}
	return contracts.DurableIntent{
		Kind: contracts.IntentChildResolution, Identity: identity, AggregateID: childRunID,
		PayloadDigest: payloadDigest, Payload: payload,
	}, nil
}

func childFailure(run contracts.Run) contracts.StructuredFailure {
	if run.PrimaryFailure != nil {
		return *run.PrimaryFailure
	}
	return contracts.StructuredFailure{
		Class: contracts.FailurePermanent, Code: "action-call.child-" + string(run.Status),
		Message: fmt.Sprintf("child Action Run %s terminated with status %s", run.RunID, run.Status),
	}
}

// ResolveActionCall joins one terminal child Run into its exact waiting parent
// invocation. The child result is schema-checked and crosses the boundary only
// through the frozen resultMap.
func (controller *Controller) ResolveActionCall(ctx context.Context, childRunID string) (contracts.Run, error) {
	if ctx == nil || !contracts.ValidIdentifier(childRunID) {
		return contracts.Run{}, errors.New("child Action resolution context or Run identity is invalid")
	}
	childRecord, err := controller.store.GetAggregate(ctx, runKey(childRunID))
	if err != nil {
		return contracts.Run{}, err
	}
	child, err := decodeRun(childRecord)
	if err != nil {
		return contracts.Run{}, err
	}
	if !child.Status.Terminal() {
		driven, driveErr := controller.Drive(ctx, childRunID)
		if driven.Status.Terminal() {
			child = driven
		} else if driveErr == nil || errors.Is(driveErr, ErrRunWaiting) || errors.Is(driveErr, ErrAttemptLeaseActive) || errors.Is(driveErr, ErrRunClosureOpen) {
			return child, ErrRunWaiting
		} else {
			return child, driveErr
		}
	}
	if child.Parent == nil {
		return contracts.Run{}, errors.New("child Action Run has no parent link")
	}
	parentRecord, err := controller.store.GetAggregate(ctx, runKey(child.Parent.ParentRunID))
	if err != nil {
		return contracts.Run{}, err
	}
	parent, err := decodeRun(parentRecord)
	if err != nil || parent.Status.Terminal() {
		return parent, err
	}
	// A child result arriving after the parent froze a termination intent cannot
	// resume or fail the canceled parent Invocation.
	if parent.Status == contracts.RunStopping {
		return parent, nil
	}
	if parent.Status != contracts.RunWaiting || parent.RootRunID != child.RootRunID ||
		parent.NamespaceID != child.NamespaceID {
		return contracts.Run{}, errors.New("child Action parent is not the matching waiting Run")
	}
	snapshot, snapshotRevision, err := controller.GetSnapshot(ctx, parent.RunID)
	if err != nil {
		return contracts.Run{}, err
	}
	wait := snapshot.ActionCall
	if wait == nil || wait.ChildRunID != child.RunID || wait.InvocationID != child.Parent.ParentInvocationID ||
		wait.NodeID != child.Parent.CallNodeID || wait.MappingDigest != child.Parent.MappingDigest ||
		!wait.ActionRef.Equal(child.ActionRef) {
		return contracts.Run{}, errors.New("parent snapshot does not own the terminal child Action wait")
	}
	invocationRecord, err := controller.store.GetAggregate(ctx, store.AggregateKey{Type: invocationType, ID: wait.InvocationID})
	if err != nil {
		return contracts.Run{}, err
	}
	ledger, err := decodeLedger(invocationRecord)
	if err != nil {
		return contracts.Run{}, err
	}
	attempt := activeAttempt(ledger)
	if attempt == nil || ledger.Invocation.Status != contracts.InvocationWaiting ||
		ledger.Invocation.CurrentWaitRef != child.RunID || ledger.Invocation.WaitGeneration != 1 {
		return contracts.Run{}, errors.New("parent invocation does not own the child Action wait")
	}
	now := controller.clock.Now().UTC()
	if now.Before(child.UpdatedAt) {
		now = child.UpdatedAt
	}
	commandID := phaseCommand(parent.RunID, "resolve-child-"+wait.NodeID, ledger.Invocation.Revision)
	var output map[string]any
	var outputDigest string
	var failure *contracts.StructuredFailure
	if child.Status == contracts.RunSucceeded {
		childSnapshot, _, snapshotErr := controller.GetSnapshot(ctx, child.RunID)
		if snapshotErr != nil || childSnapshot.ResultDigest == "" {
			local := structuredFailure("action-call.child-result", errors.New("succeeded child Run has no durable result"))
			failure = &local
		} else {
			childResult := childSnapshot.Result
			if childResult == nil {
				// An empty object is omitted by JSON encoding but remains a valid,
				// digest-bearing result for an Action whose result schema is empty.
				childResult = map[string]any{}
			}
			mapped, mapErr := workflowkernel.ResolveCallActionResult(snapshot.Definition, wait.NodeID, childResult)
			if mapErr != nil {
				local := structuredFailure("action-call.result-map", mapErr)
				failure = &local
			} else {
				output = mapped
				outputDigest, err = canonicaljson.DigestValue(output)
				if err != nil {
					return contracts.Run{}, err
				}
			}
		}
	} else {
		local := childFailure(child)
		failure = &local
	}
	to := contracts.InvocationSucceeded
	if failure != nil {
		to = contracts.InvocationFailed
		outputDigest = ""
	}
	invocationDecision, err := execution.ResolveInvocationWait(ledger, execution.ResolveInvocationWaitCommand{
		InvocationID: ledger.Invocation.InvocationID, ExpectedRevision: ledger.Invocation.Revision,
		AttemptID: attempt.AttemptID, ExpectedAttemptRevision: attempt.Revision,
		WaitRef: child.RunID, WaitGeneration: ledger.Invocation.WaitGeneration,
		To: to, OutputRefsDigest: outputDigest, Failure: failure, CommandID: commandID, At: now,
	})
	if err != nil {
		return contracts.Run{}, err
	}
	snapshot.ActionCall = nil
	if failure == nil {
		snapshot.NodeOutputs[wait.NodeID] = output
		snapshot.NextNode++
	} else {
		snapshot.Failure = failure
	}
	snapshotMutation, err := aggregateMutation(snapshotKey(parent.RunID), snapshotRevision, snapshot)
	if err != nil {
		return contracts.Run{}, err
	}
	eventType := "snapshot.child-action-succeeded"
	payload := map[string]any{"nodeId": wait.NodeID, "childRunId": child.RunID, "outputDigest": outputDigest}
	if failure != nil {
		eventType = "snapshot.child-action-failed"
		payload = map[string]any{"nodeId": wait.NodeID, "childRunId": child.RunID, "failureCode": failure.Code}
	}
	snapshotEvent, err := aggregateEvent(snapshotMutation, eventType, commandID, now, payload)
	if err != nil {
		return contracts.Run{}, err
	}
	invocationMutation, err := aggregateMutation(
		store.AggregateKey{Type: invocationType, ID: ledger.Invocation.InvocationID},
		invocationRecord.Revision, invocationDecision.Ledger,
	)
	if err != nil {
		return contracts.Run{}, err
	}
	runCommand := execution.RunTransitionCommand{
		RunID: parent.RunID, ExpectedRevision: parent.Revision, To: contracts.RunRunning,
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
	runDecision, err := execution.TransitionRun(parent, runCommand)
	if err != nil {
		return contracts.Run{}, err
	}
	runMutation, err := aggregateMutation(runKey(parent.RunID), parentRecord.Revision, runDecision.Run)
	if err != nil {
		return contracts.Run{}, err
	}
	if err := controller.commit(ctx, commandID, now,
		[]store.ExpectedRevision{
			{Key: runMutation.Key, Revision: parentRecord.Revision},
			{Key: invocationMutation.Key, Revision: invocationRecord.Revision},
			{Key: snapshotMutation.Key, Revision: snapshotRevision},
		},
		[]store.AggregateRecord{runMutation, invocationMutation, snapshotMutation},
		append(append(runDecision.Events, invocationDecision.Events...), snapshotEvent),
		runDecision.Intents, runDecision.Run,
	); err != nil {
		return contracts.Run{}, err
	}
	return controller.Drive(ctx, parent.RunID)
}
