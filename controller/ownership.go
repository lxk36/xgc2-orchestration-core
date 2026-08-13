package controller

import (
	"context"
	"errors"
	"fmt"

	"github.com/lxk36/xgc2-orchestration-core/durable/store"
	"github.com/lxk36/xgc2-orchestration-core/kernel/canonicaljson"
	"github.com/lxk36/xgc2-orchestration-core/kernel/ownership"
	"github.com/lxk36/xgc2-orchestration-core/sdk/go/contracts"
)

func ownershipGraphKey(runID string) store.AggregateKey {
	return store.AggregateKey{Type: ownershipGraphType, ID: runID}
}

// OwnershipGraph returns the exact graph snapshot persisted in the same
// transaction that closed the Run. Its embedded Run is deliberately the
// pre-terminal revision against which closure was proved.
func (controller *Controller) OwnershipGraph(ctx context.Context, runID string) (contracts.OwnershipGraph, error) {
	if controller == nil || ctx == nil || !contracts.ValidIdentifier(runID) {
		return contracts.OwnershipGraph{}, errors.New("controller, context, and Run identity are required")
	}
	record, err := controller.store.GetAggregate(ctx, ownershipGraphKey(runID))
	if err != nil {
		return contracts.OwnershipGraph{}, err
	}
	return decodeOwnershipGraph(record)
}

func (controller *Controller) deriveOwnershipClosure(ctx context.Context, run contracts.Run) (contracts.OwnershipGraph, contracts.RunClosureFacts, uint64, error) {
	graphExpected := uint64(0)
	if current, err := controller.store.GetAggregate(ctx, ownershipGraphKey(run.RunID)); err == nil {
		graphExpected = current.Revision
	} else if !errors.Is(err, store.ErrNotFound) {
		return contracts.OwnershipGraph{}, contracts.RunClosureFacts{}, 0, err
	}
	graph := contracts.OwnershipGraph{Run: run, Revision: graphExpected + 1}
	invocations, err := listAllAggregates(ctx, controller.store, invocationType)
	if err != nil {
		return contracts.OwnershipGraph{}, contracts.RunClosureFacts{}, 0, err
	}
	for _, record := range invocations {
		ledger, decodeErr := decodeLedger(record)
		if decodeErr != nil {
			return contracts.OwnershipGraph{}, contracts.RunClosureFacts{}, 0, decodeErr
		}
		if ledger.Invocation.RunID == run.RunID {
			graph.Invocations = append(graph.Invocations, ledger)
		}
	}
	runs, err := listAllAggregates(ctx, controller.store, runAggregateType)
	if err != nil {
		return contracts.OwnershipGraph{}, contracts.RunClosureFacts{}, 0, err
	}
	for _, record := range runs {
		child, decodeErr := decodeRun(record)
		if decodeErr != nil {
			return contracts.OwnershipGraph{}, contracts.RunClosureFacts{}, 0, decodeErr
		}
		if child.Parent != nil && child.Parent.ParentRunID == run.RunID {
			graph.ChildRuns = append(graph.ChildRuns, child)
		}
	}
	effects, err := listAllAggregates(ctx, controller.store, effectAggregateType)
	if err != nil {
		return contracts.OwnershipGraph{}, contracts.RunClosureFacts{}, 0, err
	}
	for _, record := range effects {
		current, decodeErr := decodeEffect(record)
		if decodeErr != nil {
			return contracts.OwnershipGraph{}, contracts.RunClosureFacts{}, 0, decodeErr
		}
		if current.Intent.RunID == run.RunID {
			graph.Effects = append(graph.Effects, current)
		}
	}
	facts, err := ownership.ClosureFacts(graph)
	if err != nil {
		return contracts.OwnershipGraph{}, contracts.RunClosureFacts{}, 0, err
	}
	return graph, facts, graphExpected, nil
}

func listAllAggregates(ctx context.Context, durable store.Store, aggregateType string) ([]store.AggregateRecord, error) {
	result := make([]store.AggregateRecord, 0)
	after := ""
	for {
		page, err := durable.ListAggregates(ctx, aggregateType, after, 1000)
		if err != nil {
			return nil, err
		}
		result = append(result, page...)
		if len(page) < 1000 {
			return result, nil
		}
		after = page[len(page)-1].Key.ID
	}
}

func decodeOwnershipGraph(record store.AggregateRecord) (contracts.OwnershipGraph, error) {
	var graph contracts.OwnershipGraph
	if record.Key.Type != ownershipGraphType || record.Revision == 0 || canonicaljson.UnmarshalStrict(record.Payload, &graph) != nil {
		return contracts.OwnershipGraph{}, errors.New("durable ownership graph is invalid")
	}
	if graph.Run.RunID != record.Key.ID || graph.Revision != record.Revision {
		return contracts.OwnershipGraph{}, errors.New("durable ownership graph identity or revision is invalid")
	}
	if _, err := ownership.ClosureFacts(graph); err != nil {
		return contracts.OwnershipGraph{}, fmt.Errorf("durable ownership graph: %w", err)
	}
	return graph, nil
}

func openClosureError(facts contracts.RunClosureFacts) error {
	return fmt.Errorf("%w: invocations=%d attempts=%d waits=%d children=%d effects=%d compensations=%d runtimes=%d resources=%d",
		ErrRunClosureOpen, facts.ActiveInvocationCount, facts.LiveAttemptCount, facts.OpenWaitCount,
		facts.OpenChildCount, facts.OpenEffectCount, facts.OpenEffectCompensationCount,
		facts.OpenOwnedRuntimeCount, facts.OpenOwnedResourceCount)
}
