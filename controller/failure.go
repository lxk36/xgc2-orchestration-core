package controller

import (
	"context"
	"errors"
	"time"

	"github.com/lxk36/xgc2-orchestration-core/durable/store"
	"github.com/lxk36/xgc2-orchestration-core/kernel/execution"
	"github.com/lxk36/xgc2-orchestration-core/sdk/go/contracts"
)

func (controller *Controller) failClaimed(
	ctx context.Context,
	runRecord store.AggregateRecord,
	run contracts.Run,
	snapshot RunSnapshot,
	snapshotRevision uint64,
	invocationRevision uint64,
	ledger contracts.InvocationLedger,
	leaseToken string,
	code string,
	cause error,
	at time.Time,
) (contracts.Run, error) {
	failure := structuredFailure(code, cause)
	attempt := activeAttempt(ledger)
	if attempt == nil {
		return contracts.Run{}, errors.New("failed invocation has no active attempt")
	}
	commandID := phaseCommand(run.RunID, "fail-"+ledger.Invocation.NodeID, ledger.Invocation.Revision)
	decision, err := execution.TransitionInvocation(ledger, execution.TransitionInvocationCommand{
		Fence: execution.AttemptFence{
			InvocationID: ledger.Invocation.InvocationID, InvocationRevision: ledger.Invocation.Revision,
			AttemptID: attempt.AttemptID, AttemptRevision: attempt.Revision, LeaseToken: leaseToken, At: at,
		},
		To: contracts.InvocationFailed, AttemptTo: contracts.AttemptFailed, Failure: &failure, CommandID: commandID,
	})
	if err != nil {
		return contracts.Run{}, err
	}
	snapshot.Failure = &failure
	snapshotMutation, err := aggregateMutation(snapshotKey(run.RunID), snapshotRevision, snapshot)
	if err != nil {
		return contracts.Run{}, err
	}
	snapshotEvent, err := aggregateEvent(snapshotMutation, "snapshot.node-failed", commandID, at, map[string]any{
		"nodeId": ledger.Invocation.NodeID, "failureCode": failure.Code,
	})
	if err != nil {
		return contracts.Run{}, err
	}
	if err := controller.commitInvocationDecision(ctx, invocationRevision, decision, commandID, at, &snapshotMutation, &snapshotEvent); err != nil {
		return contracts.Run{}, err
	}
	return controller.beginFailure(ctx, runRecord, run, snapshot, snapshotRevision+1, failure, at)
}

func (controller *Controller) failWithoutAttempt(
	ctx context.Context,
	runRecord store.AggregateRecord,
	run contracts.Run,
	snapshot RunSnapshot,
	snapshotRevision uint64,
	code string,
	cause error,
	at time.Time,
) (contracts.Run, error) {
	failure := structuredFailure(code, cause)
	return controller.beginFailure(ctx, runRecord, run, snapshot, snapshotRevision, failure, at)
}

func (controller *Controller) beginFailure(
	ctx context.Context,
	runRecord store.AggregateRecord,
	run contracts.Run,
	snapshot RunSnapshot,
	snapshotRevision uint64,
	failure contracts.StructuredFailure,
	at time.Time,
) (contracts.Run, error) {
	commandID := phaseCommand(run.RunID, "stop-failed", run.Revision)
	termination := &contracts.TerminationIntent{
		Kind: contracts.TerminationFailed, RequestedBy: controller.ownerRef,
		ReasonCode: failure.Code, Reason: failure.Message, PrimaryFailure: &failure,
		CommandID: commandID, RequestedAt: at,
	}
	decision, err := execution.TransitionRun(run, execution.RunTransitionCommand{
		RunID: run.RunID, ExpectedRevision: run.Revision, To: contracts.RunStopping,
		Termination: termination, CommandID: commandID, At: at,
	})
	if err != nil {
		return contracts.Run{}, err
	}
	snapshot.Failure = &failure
	snapshotMutation, err := aggregateMutation(snapshotKey(run.RunID), snapshotRevision, snapshot)
	if err != nil {
		return contracts.Run{}, err
	}
	runMutation, err := aggregateMutation(runKey(run.RunID), runRecord.Revision, decision.Run)
	if err != nil {
		return contracts.Run{}, err
	}
	snapshotEvent, err := aggregateEvent(snapshotMutation, "snapshot.run-stopping", commandID, at, map[string]any{"failureCode": failure.Code})
	if err != nil {
		return contracts.Run{}, err
	}
	expected := []store.ExpectedRevision{{Key: runMutation.Key, Revision: runRecord.Revision}, {Key: snapshotMutation.Key, Revision: snapshotRevision}}
	if err := controller.commit(ctx, commandID, at, expected, []store.AggregateRecord{runMutation, snapshotMutation}, append(decision.Events, snapshotEvent), decision.Intents, decision.Run); err != nil {
		return contracts.Run{}, err
	}
	return controller.Drive(ctx, run.RunID)
}

func (controller *Controller) finalizeFailedRun(ctx context.Context, runRecord store.AggregateRecord, run contracts.Run, at time.Time) (contracts.Run, error) {
	if run.Termination == nil || run.Termination.PrimaryFailure == nil {
		return contracts.Run{}, errors.New("stopping failed run lacks its termination failure")
	}
	graph, closure, graphExpected, err := controller.deriveOwnershipClosure(ctx, run)
	if err != nil {
		return contracts.Run{}, err
	}
	if !closure.Satisfied() {
		return run, openClosureError(closure)
	}
	commandID := phaseCommand(run.RunID, "failed", run.Revision)
	decision, err := execution.TransitionRun(run, execution.RunTransitionCommand{
		RunID: run.RunID, ExpectedRevision: run.Revision, To: contracts.RunFailed,
		Closure:   closure,
		CommandID: commandID, At: at,
	})
	if err != nil {
		return contracts.Run{}, err
	}
	runMutation, err := aggregateMutation(runKey(run.RunID), runRecord.Revision, decision.Run)
	if err != nil {
		return contracts.Run{}, err
	}
	graphMutation, err := aggregateMutation(ownershipGraphKey(run.RunID), graphExpected, graph)
	if err != nil {
		return contracts.Run{}, err
	}
	graphEvent, err := aggregateEvent(graphMutation, "ownership-graph.closed", commandID, at, map[string]any{"runId": run.RunID, "terminalStatus": decision.Run.Status})
	if err != nil {
		return contracts.Run{}, err
	}
	if err := controller.commit(ctx, commandID, at,
		[]store.ExpectedRevision{{Key: runMutation.Key, Revision: runRecord.Revision}, {Key: graphMutation.Key, Revision: graphExpected}},
		[]store.AggregateRecord{runMutation, graphMutation}, append(decision.Events, graphEvent), decision.Intents, decision.Run,
	); err != nil {
		return contracts.Run{}, err
	}
	return decision.Run, nil
}

func (controller *Controller) succeedRun(
	ctx context.Context,
	runRecord store.AggregateRecord,
	run contracts.Run,
	snapshot RunSnapshot,
	snapshotRevision uint64,
	at time.Time,
) (contracts.Run, error) {
	result, err := workflowResult(snapshot)
	if err != nil {
		return controller.failWithoutAttempt(ctx, runRecord, run, snapshot, snapshotRevision, "workflow.result", err, at)
	}
	resultDigest, err := digestResult(result)
	if err != nil {
		return contracts.Run{}, err
	}
	graph, closure, graphExpected, err := controller.deriveOwnershipClosure(ctx, run)
	if err != nil {
		return contracts.Run{}, err
	}
	if !closure.Satisfied() {
		return run, openClosureError(closure)
	}
	commandID := phaseCommand(run.RunID, "succeeded", run.Revision)
	decision, err := execution.TransitionRun(run, execution.RunTransitionCommand{
		RunID: run.RunID, ExpectedRevision: run.Revision, To: contracts.RunSucceeded,
		ResultRef: digestRef("result", resultDigest),
		Closure:   closure,
		CommandID: commandID, At: at,
	})
	if err != nil {
		return contracts.Run{}, err
	}
	snapshot.Result = result
	snapshot.ResultDigest = resultDigest
	snapshotMutation, err := aggregateMutation(snapshotKey(run.RunID), snapshotRevision, snapshot)
	if err != nil {
		return contracts.Run{}, err
	}
	runMutation, err := aggregateMutation(runKey(run.RunID), runRecord.Revision, decision.Run)
	if err != nil {
		return contracts.Run{}, err
	}
	snapshotEvent, err := aggregateEvent(snapshotMutation, "snapshot.run-succeeded", commandID, at, map[string]any{"resultDigest": resultDigest})
	if err != nil {
		return contracts.Run{}, err
	}
	graphMutation, err := aggregateMutation(ownershipGraphKey(run.RunID), graphExpected, graph)
	if err != nil {
		return contracts.Run{}, err
	}
	graphEvent, err := aggregateEvent(graphMutation, "ownership-graph.closed", commandID, at, map[string]any{"runId": run.RunID, "terminalStatus": decision.Run.Status})
	if err != nil {
		return contracts.Run{}, err
	}
	if err := controller.commit(ctx, commandID, at,
		[]store.ExpectedRevision{{Key: runMutation.Key, Revision: runRecord.Revision}, {Key: snapshotMutation.Key, Revision: snapshotRevision}, {Key: graphMutation.Key, Revision: graphExpected}},
		[]store.AggregateRecord{runMutation, snapshotMutation, graphMutation}, append(append(decision.Events, snapshotEvent), graphEvent), decision.Intents, decision.Run,
	); err != nil {
		return contracts.Run{}, err
	}
	return decision.Run, nil
}
