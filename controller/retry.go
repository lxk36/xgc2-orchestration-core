package controller

import (
	"context"
	"errors"
	"time"

	"github.com/lxk36/xgc2-orchestration-core/durable/store"
	"github.com/lxk36/xgc2-orchestration-core/kernel/execution"
	"github.com/lxk36/xgc2-orchestration-core/sdk/go/contracts"
)

func (controller *Controller) resumeRetryWait(
	ctx context.Context,
	runRecord store.AggregateRecord,
	run contracts.Run,
	snapshot RunSnapshot,
	snapshotRevision uint64,
	at time.Time,
) (contracts.Run, error) {
	if snapshot.RetryWait == nil || run.Status != contracts.RunWaiting {
		return contracts.Run{}, errors.New("retry resume requires the exact waiting Run projection")
	}
	if at.Before(snapshot.RetryWait.AvailableAt) {
		return run, ErrRunWaiting
	}
	wait := *snapshot.RetryWait
	commandID := phaseCommand(run.RunID, "retry-due-"+snapshot.NodeOrder[snapshot.NextNode], run.Revision)
	runDecision, err := execution.TransitionRun(run, execution.RunTransitionCommand{
		RunID: run.RunID, ExpectedRevision: run.Revision, To: contracts.RunRunning,
		CommandID: commandID, At: at,
	})
	if err != nil {
		return contracts.Run{}, err
	}
	snapshot.RetryWait = nil
	snapshotMutation, err := aggregateMutation(snapshotKey(run.RunID), snapshotRevision, snapshot)
	if err != nil {
		return contracts.Run{}, err
	}
	snapshotEvent, err := aggregateEvent(snapshotMutation, "snapshot.node-retry-ready", commandID, at, map[string]any{
		"nodeId": snapshot.NodeOrder[snapshot.NextNode], "invocationId": wait.InvocationID,
		"attemptOrdinal": wait.AttemptOrdinal,
	})
	if err != nil {
		return contracts.Run{}, err
	}
	runMutation, err := aggregateMutation(runKey(run.RunID), runRecord.Revision, runDecision.Run)
	if err != nil {
		return contracts.Run{}, err
	}
	if err := controller.commit(ctx, commandID, at,
		[]store.ExpectedRevision{
			{Key: runMutation.Key, Revision: runRecord.Revision},
			{Key: snapshotMutation.Key, Revision: snapshotRevision},
		},
		[]store.AggregateRecord{runMutation, snapshotMutation},
		append(runDecision.Events, snapshotEvent), runDecision.Intents, runDecision.Run,
	); err != nil {
		return contracts.Run{}, err
	}
	return runDecision.Run, nil
}
