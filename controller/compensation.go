package controller

import (
	"context"
	"sort"
	"time"

	"github.com/lxk36/xgc2-orchestration-core/durable/store"
	"github.com/lxk36/xgc2-orchestration-core/kernel/effect"
	"github.com/lxk36/xgc2-orchestration-core/kernel/execution"
	"github.com/lxk36/xgc2-orchestration-core/sdk/go/contracts"
)

// scheduleNextEffectCompensation serializes Saga cleanup in reverse workflow
// order. At most one Effect is pending/running at a time, which makes resource
// dependency teardown deterministic across crashes and worker concurrency.
func (controller *Controller) scheduleNextEffectCompensation(
	ctx context.Context,
	run contracts.Run,
	snapshot RunSnapshot,
	at time.Time,
) (bool, []contracts.StructuredFailure, error) {
	records, err := listAllAggregates(ctx, controller.store, effectAggregateType)
	if err != nil {
		return false, nil, err
	}
	position := make(map[string]int, len(snapshot.NodeOrder))
	for index, nodeID := range snapshot.NodeOrder {
		invocationID, stableErr := execution.StableInvocationID(run.RunID, nodeID)
		if stableErr != nil {
			return false, nil, stableErr
		}
		position[invocationID] = index
	}
	type candidate struct {
		record store.AggregateRecord
		effect contracts.EffectRecord
		order  int
	}
	var candidates []candidate
	var failures []contracts.StructuredFailure
	for _, record := range records {
		current, decodeErr := decodeEffect(record)
		if decodeErr != nil {
			return false, nil, decodeErr
		}
		if current.Intent.RunID != run.RunID || current.State != contracts.EffectApplied ||
			current.Intent.Ownership != contracts.EffectOwned || current.Intent.CompensationPolicy == contracts.CompensationNone {
			continue
		}
		switch current.CompensationState {
		case contracts.EffectCompensationPending, contracts.EffectCompensationRunning, contracts.EffectCompensationRetryWait:
			return false, failures, nil
		case contracts.EffectCompensationFailed:
			if current.CompensationFailure != nil {
				failures = append(failures, *current.CompensationFailure)
			}
		case contracts.EffectCompensationUnscheduled:
			candidates = append(candidates, candidate{record: record, effect: current, order: position[current.Intent.InvocationID]})
		}
	}
	if len(candidates) == 0 {
		return false, failures, nil
	}
	sort.Slice(candidates, func(i, j int) bool {
		if candidates[i].order != candidates[j].order {
			return candidates[i].order > candidates[j].order
		}
		left, right := candidates[i].effect.PrimaryTerminalAt, candidates[j].effect.PrimaryTerminalAt
		if left != nil && right != nil && !left.Equal(*right) {
			return left.After(*right)
		}
		return candidates[i].effect.EffectID > candidates[j].effect.EffectID
	})
	next := candidates[0]
	if at.Before(next.effect.UpdatedAt) {
		at = next.effect.UpdatedAt
	}
	commandID := phaseCommand(run.RunID, "schedule-compensation-"+next.effect.EffectID, next.effect.Revision)
	decision, err := effect.TransitionCompensation(next.effect, effect.CompensationCommand{
		EffectID: next.effect.EffectID, ExpectedRevision: next.effect.Revision,
		To: contracts.EffectCompensationPending, CommandID: commandID, At: at,
	})
	if err != nil {
		return false, nil, err
	}
	mutation, err := aggregateMutation(effectKey(next.effect.EffectID), next.record.Revision, decision.Effect)
	if err != nil {
		return false, nil, err
	}
	if err := controller.commit(ctx, commandID, at,
		[]store.ExpectedRevision{{Key: mutation.Key, Revision: next.record.Revision}},
		[]store.AggregateRecord{mutation}, decision.Events, decision.Intents, decision.Effect,
	); err != nil {
		return false, nil, err
	}
	return true, failures, nil
}
