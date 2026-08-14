package controller

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/lxk36/xgc2-orchestration-core/durable/store"
	"github.com/lxk36/xgc2-orchestration-core/kernel/canonicaljson"
	"github.com/lxk36/xgc2-orchestration-core/kernel/execution"
	nodekernel "github.com/lxk36/xgc2-orchestration-core/kernel/node"
	workflowkernel "github.com/lxk36/xgc2-orchestration-core/kernel/workflow"
	"github.com/lxk36/xgc2-orchestration-core/sdk/go/contracts"
)

func (controller *Controller) Drive(ctx context.Context, runID string) (contracts.Run, error) {
	if ctx == nil || !contracts.ValidIdentifier(runID) {
		return contracts.Run{}, errors.New("drive context or run identity is invalid")
	}
	for step := 0; step < 100000; step++ {
		runRecord, err := controller.store.GetAggregate(ctx, runKey(runID))
		if err != nil {
			return contracts.Run{}, err
		}
		run, err := decodeRun(runRecord)
		if err != nil || run.Status.Terminal() {
			return run, err
		}
		snapshot, snapshotRevision, err := controller.GetSnapshot(ctx, runID)
		if err != nil {
			return contracts.Run{}, err
		}
		now := controller.clock.Now().UTC()
		if now.Before(run.UpdatedAt) {
			now = run.UpdatedAt
		}
		switch run.Status {
		case contracts.RunAccepted, contracts.RunQueued:
			decision, transitionErr := execution.TransitionRun(run, execution.RunTransitionCommand{
				RunID: run.RunID, ExpectedRevision: run.Revision, To: contracts.RunRunning,
				CommandID: phaseCommand(runID, "start", run.Revision), At: now,
			})
			if transitionErr != nil {
				return contracts.Run{}, transitionErr
			}
			if err := controller.commitRunDecision(ctx, runRecord.Revision, decision, decision.Events[0].CommandID, now); err != nil {
				return contracts.Run{}, err
			}
			continue
		case contracts.RunWaiting:
			if snapshot.RetryWait == nil {
				return run, ErrRunWaiting
			}
			resumed, resumeErr := controller.resumeRetryWait(ctx, runRecord, run, snapshot, snapshotRevision, now)
			if resumeErr != nil {
				return resumed, resumeErr
			}
			continue
		case contracts.RunStopping:
			return controller.finalizeStoppingRun(ctx, runRecord, run, now)
		case contracts.RunRunning:
		default:
			return contracts.Run{}, fmt.Errorf("unsupported run status %q", run.Status)
		}

		if snapshot.NextNode >= len(snapshot.NodeOrder) {
			return controller.succeedRun(ctx, runRecord, run, snapshot, snapshotRevision, now)
		}
		nodeID := snapshot.NodeOrder[snapshot.NextNode]
		disposition, err := currentNodeDisposition(snapshot)
		if err != nil {
			return contracts.Run{}, err
		}
		if disposition == nodeBlocked {
			return contracts.Run{}, errors.New("current workflow node has unresolved predecessors despite stable topological order")
		}
		workflowNode, ok := findWorkflowNode(snapshot.Definition, nodeID)
		if !ok {
			return contracts.Run{}, fmt.Errorf("pinned node %q disappeared from snapshot", nodeID)
		}
		descriptor := controller.descriptors[workflowNode.TypeRef]
		inputs := map[string]any{}
		if disposition == nodeReady {
			inputs, err = workflowkernel.ResolveNodeInputs(
				snapshot.Definition, nodeID, snapshot.Inputs, snapshot.Trigger, snapshot.Scope, snapshot.NodeOutputs, nil,
			)
			if err != nil {
				return controller.failWithoutAttempt(ctx, runRecord, run, snapshot, snapshotRevision, "node.input-resolution", err, now)
			}
		}
		inputDigest, err := canonicaljson.DigestValue(inputs)
		if err != nil {
			return contracts.Run{}, err
		}
		invocationID, _ := execution.StableInvocationID(runID, nodeID)
		invocationKey := store.AggregateKey{Type: invocationType, ID: invocationID}
		invocationRecord, getErr := controller.store.GetAggregate(ctx, invocationKey)
		if errors.Is(getErr, store.ErrNotFound) {
			activated, activateErr := execution.ActivateInvocation(execution.ActivateInvocationCommand{
				InvocationID: invocationID, NamespaceID: run.NamespaceID, RunID: runID, NodeID: nodeID,
				TypeRef: workflowNode.TypeRef, DescriptorDigest: workflowNode.DescriptorDigest,
				ResolvedInputDigest: inputDigest, InputRefsDigest: inputDigest,
				Compensatable: descriptor.CompensationTypeRef != "", CommandID: phaseCommand(runID, "activate-"+nodeID, 1), At: now,
			})
			if activateErr != nil {
				return contracts.Run{}, activateErr
			}
			if err := controller.commitInvocationDecision(ctx, 0, activated, activated.Events[0].CommandID, now, nil, nil); err != nil {
				return contracts.Run{}, err
			}
			continue
		}
		if getErr != nil {
			return contracts.Run{}, getErr
		}
		ledger, err := decodeLedger(invocationRecord)
		if err != nil {
			return contracts.Run{}, err
		}
		if disposition == nodeSkipped {
			if ledger.Invocation.Status != contracts.InvocationReady {
				return contracts.Run{}, fmt.Errorf("skipped node invocation %s is %s", nodeID, ledger.Invocation.Status)
			}
			decision, resolveErr := execution.ResolveUnleasedInvocation(ledger, execution.ResolveUnleasedInvocationCommand{
				InvocationID: invocationID, ExpectedRevision: ledger.Invocation.Revision, To: contracts.InvocationSkipped,
				CommandID: phaseCommand(runID, "skip-"+nodeID, ledger.Invocation.Revision), At: now,
			})
			if resolveErr != nil {
				return contracts.Run{}, resolveErr
			}
			if err := recordSkippedNode(&snapshot, nodeID); err != nil {
				return contracts.Run{}, err
			}
			snapshotMutation, mutationErr := aggregateMutation(snapshotKey(runID), snapshotRevision, snapshot)
			if mutationErr != nil {
				return contracts.Run{}, mutationErr
			}
			snapshotEvent, eventErr := aggregateEvent(snapshotMutation, "snapshot.node-skipped", decision.Events[0].CommandID, now, map[string]any{"nodeId": nodeID})
			if eventErr != nil {
				return contracts.Run{}, eventErr
			}
			if err := controller.commitInvocationDecision(ctx, invocationRecord.Revision, decision, decision.Events[0].CommandID, now, &snapshotMutation, &snapshotEvent); err != nil {
				return contracts.Run{}, err
			}
			continue
		}
		claimedNow := false
		leaseToken := ""
		if ledger.Invocation.Status == contracts.InvocationReady || ledger.Invocation.Status == contracts.InvocationRetryWait {
			leaseToken, err = controller.tokens.NewToken()
			if err != nil {
				return contracts.Run{}, err
			}
			claimed, claimErr := execution.ClaimInvocation(ledger, execution.ClaimInvocationCommand{
				InvocationID: invocationID, ExpectedRevision: ledger.Invocation.Revision, Phase: contracts.AttemptExecution,
				OwnerRef: controller.ownerRef, LeaseToken: leaseToken, LeaseExpiresAt: now.Add(controller.leaseDuration),
				CommandID: phaseCommand(runID, "claim-"+nodeID, ledger.Invocation.Revision), At: now,
			})
			if claimErr != nil {
				return contracts.Run{}, claimErr
			}
			if err := controller.commitInvocationDecision(ctx, invocationRecord.Revision, claimed, claimed.Events[0].CommandID, now, nil, nil); err != nil {
				return contracts.Run{}, err
			}
			ledger = claimed.Ledger
			invocationRecord.Revision++
			claimedNow = true
			if controller.afterClaim != nil {
				if err := controller.afterClaim(ledger); err != nil {
					return contracts.Run{}, err
				}
			}
		}
		if ledger.Invocation.Status == contracts.InvocationRunning && !claimedNow {
			attempt := activeAttempt(ledger)
			if attempt == nil || now.Before(attempt.LeaseExpiresAt) {
				return run, ErrAttemptLeaseActive
			}
			// A child Action call is retryable because both its Run and trigger
			// identities are derived from this immutable parent invocation.
			policy := retryPolicy(workflowNode)
			retry := ledger.Invocation.ExecutionAttemptCount < policy.MaxAttempts &&
				(workflowNode.CallAction != nil ||
					descriptor.Mode == contracts.NodePure && descriptor.Determinism == contracts.NodeDeterministic)
			failure := contracts.StructuredFailure{Class: contracts.FailureUncertain, Code: "worker.lease-expired", Message: "worker lease expired before a durable node result"}
			expired, expireErr := execution.ExpireInvocationAttempt(ledger, execution.ExpireInvocationAttemptCommand{
				InvocationID: invocationID, ExpectedRevision: ledger.Invocation.Revision,
				AttemptID: attempt.AttemptID, ExpectedAttemptRevision: attempt.Revision, Retry: retry,
				Failure: failure, CommandID: phaseCommand(runID, "expire-"+nodeID, ledger.Invocation.Revision), At: now,
			})
			if expireErr != nil {
				return contracts.Run{}, expireErr
			}
			if retry {
				if err := controller.commitInvocationDecision(ctx, invocationRecord.Revision, expired, expired.Events[0].CommandID, now, nil, nil); err != nil {
					return contracts.Run{}, err
				}
				continue
			}
			resolved, resolveErr := controller.commitTerminalNodeFailure(
				ctx, runRecord, run, snapshot, snapshotRevision, invocationRecord.Revision,
				expired, failure, expired.Events[0].CommandID, now,
			)
			if errors.Is(resolveErr, errContinueDrive) {
				continue
			}
			return resolved, resolveErr
		}
		if ledger.Invocation.Status != contracts.InvocationRunning {
			if ledger.Invocation.Status == contracts.InvocationWaiting {
				return run, ErrRunWaiting
			}
			return contracts.Run{}, fmt.Errorf("node invocation %s is %s while run is active", nodeID, ledger.Invocation.Status)
		}
		if workflowNode.CallAction != nil {
			executedRun, executeErr := controller.executeClaimedActionCall(
				ctx, runRecord, run, snapshot, snapshotRevision, invocationRecord.Revision,
				ledger, descriptor, workflowNode, leaseToken, now,
			)
			if errors.Is(executeErr, errContinueDrive) {
				continue
			}
			return executedRun, executeErr
		}
		executedRun, executeErr := controller.executeClaimed(ctx, runRecord, run, snapshot, snapshotRevision, invocationRecord.Revision, ledger, descriptor, inputs, leaseToken, now)
		if errors.Is(executeErr, errContinueDrive) {
			continue
		}
		return executedRun, executeErr
	}
	return contracts.Run{}, errors.New("drive exceeded bounded orchestration steps")
}

func (controller *Controller) executeClaimed(
	ctx context.Context,
	runRecord store.AggregateRecord,
	run contracts.Run,
	snapshot RunSnapshot,
	snapshotRevision uint64,
	invocationRevision uint64,
	ledger contracts.InvocationLedger,
	descriptor contracts.NodeDescriptor,
	inputs map[string]any,
	leaseToken string,
	now time.Time,
) (contracts.Run, error) {
	attempt := activeAttempt(ledger)
	if attempt == nil {
		return contracts.Run{}, errors.New("claimed invocation has no active attempt")
	}
	deadline := now.Add(controller.invocationTimeout)
	grants := []contracts.CapabilityGrant{}
	if controller.grants != nil {
		resolved, err := controller.grants.ResolveGrants(ctx, run, descriptor, deadline)
		if err != nil {
			return controller.failClaimed(ctx, runRecord, run, snapshot, snapshotRevision, invocationRevision, ledger, leaseToken, "capability.denied", err, now)
		}
		grants = resolved
	}
	inputDigest, _ := canonicaljson.DigestValue(inputs)
	request := contracts.NodeInvocationRequest{
		SchemaVersion: nodekernel.InvocationSchemaVersion,
		InvocationID: ledger.Invocation.InvocationID, RunID: run.RunID, NodeID: ledger.Invocation.NodeID,
		TypeRef: ledger.Invocation.TypeRef, DescriptorDigest: ledger.Invocation.DescriptorDigest,
		AttemptID: attempt.AttemptID, AttemptOrdinal: attempt.Ordinal, Input: inputs, InputDigest: inputDigest,
		CapabilityGrants: grants, RequestedAt: now, Deadline: deadline,
	}
	executionContext, cancel := context.WithDeadline(ctx, deadline)
	defer cancel()
	result, err := controller.nodes.Execute(executionContext, request)
	completedAt := controller.clock.Now().UTC()
	if err != nil {
		return controller.failClaimed(ctx, runRecord, run, snapshot, snapshotRevision, invocationRevision, ledger, leaseToken, "node.execute", err, completedAt)
	}
	commandID := phaseCommand(run.RunID, "result-"+ledger.Invocation.NodeID, ledger.Invocation.Revision)
	fence := execution.AttemptFence{
		InvocationID: ledger.Invocation.InvocationID, InvocationRevision: ledger.Invocation.Revision,
		AttemptID: attempt.AttemptID, AttemptRevision: attempt.Revision,
		LeaseToken: leaseToken, At: completedAt,
	}
	switch result.Status {
	case contracts.NodeResultSucceeded:
		decision, transitionErr := execution.TransitionInvocation(ledger, execution.TransitionInvocationCommand{
			Fence: fence, To: contracts.InvocationSucceeded, AttemptTo: contracts.AttemptSucceeded,
			OutputRefsDigest: result.OutputDigest, CommandID: commandID,
		})
		if transitionErr != nil {
			return contracts.Run{}, transitionErr
		}
		if err := recordSucceededNode(&snapshot, ledger.Invocation.NodeID, result); err != nil {
			return contracts.Run{}, err
		}
		snapshotMutation, mutationErr := aggregateMutation(snapshotKey(run.RunID), snapshotRevision, snapshot)
		if mutationErr != nil {
			return contracts.Run{}, mutationErr
		}
		snapshotEvent, eventErr := aggregateEvent(snapshotMutation, "snapshot.node-succeeded", commandID, completedAt, map[string]any{
			"nodeId": ledger.Invocation.NodeID, "outputDigest": result.OutputDigest,
		})
		if eventErr != nil {
			return contracts.Run{}, eventErr
		}
		if err := controller.commitInvocationDecision(ctx, invocationRevision, decision, commandID, completedAt, &snapshotMutation, &snapshotEvent); err != nil {
			return contracts.Run{}, err
		}
		return contracts.Run{}, errContinueDrive
	case contracts.NodeResultWaiting:
		decision, transitionErr := execution.TransitionInvocation(ledger, execution.TransitionInvocationCommand{
			Fence: fence, To: contracts.InvocationWaiting, AttemptTo: contracts.AttemptWaiting,
			WaitRef: result.Wait.SubjectRef, WaitGeneration: attempt.Ordinal, CommandID: commandID,
		})
		if transitionErr != nil {
			return contracts.Run{}, transitionErr
		}
		effectMutations, effectEvents, effectErr := prepareEffects(result, run, decision.Ledger, completedAt, commandID)
		if effectErr != nil {
			return contracts.Run{}, effectErr
		}
		snapshot.Waiting = &WaitingOccurrence{
			InvocationID:   decision.Ledger.Invocation.InvocationID,
			WaitGeneration: decision.Ledger.Invocation.WaitGeneration,
			Result:         result,
		}
		snapshotMutation, mutationErr := aggregateMutation(snapshotKey(run.RunID), snapshotRevision, snapshot)
		if mutationErr != nil {
			return contracts.Run{}, mutationErr
		}
		runDecision, runErr := execution.TransitionRun(run, execution.RunTransitionCommand{
			RunID: run.RunID, ExpectedRevision: run.Revision, To: contracts.RunWaiting, CommandID: commandID, At: completedAt,
		})
		if runErr != nil {
			return contracts.Run{}, runErr
		}
		return runDecision.Run, controller.commitWaiting(ctx, runRecord.Revision, invocationRevision, snapshotRevision, decision, runDecision, snapshotMutation, effectMutations, effectEvents, commandID, completedAt)
	case contracts.NodeResultFailed:
		failure, failureErr := requireFailure(result.Failure)
		if failureErr != nil {
			return contracts.Run{}, failureErr
		}
		return controller.failClaimedWithFailure(ctx, runRecord, run, snapshot, snapshotRevision, invocationRevision, ledger, leaseToken, failure, completedAt)
	default:
		return contracts.Run{}, errors.New("validated node returned an unknown status")
	}
}

func activeAttempt(ledger contracts.InvocationLedger) *contracts.Attempt {
	for index := range ledger.Attempts {
		if ledger.Attempts[index].AttemptID == ledger.Invocation.ActiveAttemptID {
			return &ledger.Attempts[index]
		}
	}
	return nil
}

func findWorkflowNode(definition contracts.WorkflowDefinition, nodeID string) (contracts.WorkflowNodeDefinition, bool) {
	for _, workflowNode := range definition.Nodes {
		if workflowNode.NodeID == nodeID {
			return workflowNode, true
		}
	}
	return contracts.WorkflowNodeDefinition{}, false
}
