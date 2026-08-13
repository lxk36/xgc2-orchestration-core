package controller

import (
	"context"
	"errors"
	"fmt"

	"github.com/lxk36/xgc2-orchestration-core/durable/store"
	"github.com/lxk36/xgc2-orchestration-core/kernel/canonicaljson"
	"github.com/lxk36/xgc2-orchestration-core/kernel/execution"
	workflowkernel "github.com/lxk36/xgc2-orchestration-core/kernel/workflow"
	"github.com/lxk36/xgc2-orchestration-core/sdk/go/contracts"
)

// ResolveEffectWait consumes one terminal Effect observation. A successful
// Effect is folded by the node's pure Resumer; a failed or uncertain Effect
// fails the invocation and starts normal run cleanup. The original effectful
// Execute method is never called again.
func (controller *Controller) ResolveEffectWait(ctx context.Context, effectID string) (contracts.Run, error) {
	if ctx == nil || !contracts.ValidIdentifier(effectID) {
		return contracts.Run{}, errors.New("effect wait context or identity is invalid")
	}
	effectRecord, err := controller.store.GetAggregate(ctx, effectKey(effectID))
	if err != nil {
		return contracts.Run{}, err
	}
	currentEffect, err := decodeEffect(effectRecord)
	if err != nil {
		return contracts.Run{}, err
	}
	if !currentEffect.State.Terminal() {
		return contracts.Run{}, errors.New("effect wait cannot resolve before a terminal receipt")
	}
	runRecord, err := controller.store.GetAggregate(ctx, runKey(currentEffect.Intent.RunID))
	if err != nil {
		return contracts.Run{}, err
	}
	run, err := decodeRun(runRecord)
	if err != nil {
		return contracts.Run{}, err
	}
	if run.Status.Terminal() {
		return run, nil
	}
	if run.Status != contracts.RunWaiting {
		return contracts.Run{}, fmt.Errorf("run %s is %s instead of waiting", run.RunID, run.Status)
	}
	snapshot, snapshotRevision, err := controller.GetSnapshot(ctx, run.RunID)
	if err != nil {
		return contracts.Run{}, err
	}
	if snapshot.Waiting == nil || snapshot.Waiting.Wait == nil || snapshot.Waiting.Wait.Kind != contracts.NodeWaitEffect ||
		snapshot.Waiting.Wait.SubjectRef != currentEffect.Intent.EffectKey ||
		snapshot.Waiting.Wait.ConditionDigest != currentEffect.Intent.IntentDigest {
		return contracts.Run{}, errors.New("run snapshot does not wait for the terminal effect")
	}
	invocationRecord, err := controller.store.GetAggregate(ctx, store.AggregateKey{Type: invocationType, ID: currentEffect.Intent.InvocationID})
	if err != nil {
		return contracts.Run{}, err
	}
	ledger, err := decodeLedger(invocationRecord)
	if err != nil {
		return contracts.Run{}, err
	}
	attempt := activeAttempt(ledger)
	if attempt == nil || ledger.Invocation.Status != contracts.InvocationWaiting ||
		ledger.Invocation.CurrentWaitRef != currentEffect.Intent.EffectKey {
		return contracts.Run{}, errors.New("invocation does not own the terminal effect wait")
	}
	now := controller.clock.Now().UTC()
	if now.Before(currentEffect.UpdatedAt) {
		now = currentEffect.UpdatedAt
	}
	commandID := phaseCommand(run.RunID, "resolve-"+ledger.Invocation.NodeID, ledger.Invocation.Revision)

	var result contracts.NodeResult
	var failure *contracts.StructuredFailure
	if currentEffect.State == contracts.EffectApplied {
		workflowNode, exists := findWorkflowNode(snapshot.Definition, ledger.Invocation.NodeID)
		if !exists {
			return contracts.Run{}, errors.New("waiting workflow node disappeared from the pinned definition")
		}
		inputs, resolveErr := workflowkernel.ResolveNodeInputs(
			snapshot.Definition, workflowNode.NodeID, snapshot.Inputs, snapshot.Trigger, snapshot.Scope, snapshot.NodeOutputs, nil,
		)
		if resolveErr != nil {
			return contracts.Run{}, resolveErr
		}
		inputDigest, digestErr := canonicaljson.DigestValue(inputs)
		if digestErr != nil || inputDigest != ledger.Invocation.ResolvedInputDigest {
			return contracts.Run{}, errors.New("resumed node inputs differ from the frozen invocation")
		}
		payload := map[string]any{
			"effectId": currentEffect.EffectID, "state": currentEffect.State,
			"resultDigest":      currentEffect.ResultDigest,
			"resultArtifactRef": currentEffect.ResultArtifactRef,
			"externalIdentity":  currentEffect.ExternalIdentity,
		}
		payloadDigest, digestErr := canonicaljson.DigestValue(payload)
		if digestErr != nil {
			return contracts.Run{}, digestErr
		}
		result, err = controller.nodes.Resume(ctx, contracts.NodeResumeRequest{
			InvocationID: ledger.Invocation.InvocationID, RunID: run.RunID,
			NodeID: ledger.Invocation.NodeID, TypeRef: ledger.Invocation.TypeRef,
			DescriptorDigest: ledger.Invocation.DescriptorDigest,
			AttemptID:        attempt.AttemptID, AttemptOrdinal: attempt.Ordinal,
			Input: inputs, InputDigest: inputDigest, Wait: *snapshot.Waiting.Wait,
			Resolution: contracts.NodeWaitResolution{
				Kind: contracts.NodeWaitEffect, SubjectRef: currentEffect.Intent.EffectKey,
				ConditionDigest: currentEffect.Intent.IntentDigest, Status: contracts.NodeWaitResolvedSucceeded,
				Payload: payload, PayloadDigest: payloadDigest,
				PayloadArtifactRef: currentEffect.ResultArtifactRef, ObservedAt: currentEffect.UpdatedAt,
			},
			RequestedAt: now,
		})
		if err != nil {
			local := structuredFailure("node.resume", err)
			failure = &local
		} else if result.Status == contracts.NodeResultFailed {
			failure = result.Failure
		}
	} else {
		failure = currentEffect.PrimaryFailure
	}
	if failure == nil && result.Status != contracts.NodeResultSucceeded {
		local := contracts.StructuredFailure{Class: contracts.FailurePermanent, Code: "effect.resolution", Message: "effect wait did not produce a terminal node result"}
		failure = &local
	}

	to := contracts.InvocationSucceeded
	outputDigest := result.OutputDigest
	if failure != nil {
		to = contracts.InvocationFailed
		outputDigest = ""
	}
	invocationDecision, err := execution.ResolveInvocationWait(ledger, execution.ResolveInvocationWaitCommand{
		InvocationID: ledger.Invocation.InvocationID, ExpectedRevision: ledger.Invocation.Revision,
		AttemptID: attempt.AttemptID, ExpectedAttemptRevision: attempt.Revision,
		WaitRef: currentEffect.Intent.EffectKey, WaitGeneration: ledger.Invocation.WaitGeneration,
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
	snapshotEventType := "snapshot.node-resumed"
	snapshotPayload := map[string]any{"nodeId": ledger.Invocation.NodeID, "effectId": effectID, "outputDigest": outputDigest}
	if failure != nil {
		snapshotEventType = "snapshot.node-failed"
		snapshotPayload = map[string]any{"nodeId": ledger.Invocation.NodeID, "effectId": effectID, "failureCode": failure.Code}
	}
	snapshotEvent, err := aggregateEvent(snapshotMutation, snapshotEventType, commandID, now, snapshotPayload)
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
		return contracts.Run{}, err
	}
	return controller.Drive(ctx, run.RunID)
}
