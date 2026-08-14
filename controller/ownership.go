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

// OwnershipGraph returns the exact self-contained closure record persisted in
// the same transaction that closed the Run. It never projects mutable Run
// state into the durable proof snapshot.
func (controller *Controller) OwnershipGraph(ctx context.Context, runID string) (contracts.OwnershipGraph, error) {
	if controller == nil || ctx == nil || !contracts.ValidIdentifier(runID) {
		return contracts.OwnershipGraph{}, errors.New("controller, context, and Run identity are required")
	}
	record, err := controller.store.GetAggregate(ctx, ownershipGraphKey(runID))
	if err != nil {
		return contracts.OwnershipGraph{}, err
	}
	graph, err := decodeOwnershipGraph(record)
	if err != nil {
		return contracts.OwnershipGraph{}, err
	}
	runRecord, err := controller.store.GetAggregate(ctx, runKey(runID))
	if err != nil {
		return contracts.OwnershipGraph{}, err
	}
	run, err := decodeRun(runRecord)
	if err != nil {
		return contracts.OwnershipGraph{}, err
	}
	graphRun, err := canonicaljson.Marshal(graph.TerminalRun)
	if err != nil {
		return contracts.OwnershipGraph{}, err
	}
	durableRun, err := canonicaljson.Marshal(run)
	if err != nil {
		return contracts.OwnershipGraph{}, err
	}
	if string(graphRun) != string(durableRun) {
		return contracts.OwnershipGraph{}, errors.New("durable ownership graph terminal Run differs from the Run aggregate")
	}
	return graph, nil
}

func (controller *Controller) deriveOwnershipClosure(ctx context.Context, run contracts.Run) (contracts.OwnershipClosureBase, error) {
	if _, err := controller.store.GetAggregate(ctx, ownershipGraphKey(run.RunID)); err == nil {
		return contracts.OwnershipClosureBase{}, errors.New("Run already has an immutable ownership closure proof")
	} else if !errors.Is(err, store.ErrNotFound) {
		return contracts.OwnershipClosureBase{}, err
	}
	base := contracts.OwnershipClosureBase{Run: run}
	invocations, err := listAllAggregates(ctx, controller.store, invocationType)
	if err != nil {
		return contracts.OwnershipClosureBase{}, err
	}
	for _, record := range invocations {
		ledger, decodeErr := decodeLedger(record)
		if decodeErr != nil {
			return contracts.OwnershipClosureBase{}, decodeErr
		}
		if ledger.Invocation.RunID == run.RunID {
			base.Invocations = append(base.Invocations, ledger)
		}
	}
	runs, err := listAllAggregates(ctx, controller.store, runAggregateType)
	if err != nil {
		return contracts.OwnershipClosureBase{}, err
	}
	for _, record := range runs {
		child, decodeErr := decodeRun(record)
		if decodeErr != nil {
			return contracts.OwnershipClosureBase{}, decodeErr
		}
		if child.Parent != nil && child.Parent.ParentRunID == run.RunID {
			base.ChildRuns = append(base.ChildRuns, child)
		}
	}
	effects, err := listAllAggregates(ctx, controller.store, effectAggregateType)
	if err != nil {
		return contracts.OwnershipClosureBase{}, err
	}
	for _, record := range effects {
		current, decodeErr := decodeEffect(record)
		if decodeErr != nil {
			return contracts.OwnershipClosureBase{}, decodeErr
		}
		if current.Intent.RunID == run.RunID {
			base.Effects = append(base.Effects, current)
		}
	}
	return base, nil
}

// convergedTerminalRun returns the already published terminal Run only when
// its immutable ownership graph validates and carries that exact Run. This is
// the convergence path for a transaction that lost the final closure CAS.
func (controller *Controller) convergedTerminalRun(ctx context.Context, runID string) (contracts.Run, bool, error) {
	record, err := controller.store.GetAggregate(ctx, ownershipGraphKey(runID))
	if errors.Is(err, store.ErrNotFound) {
		return contracts.Run{}, false, nil
	}
	if err != nil {
		return contracts.Run{}, false, err
	}
	graph, err := decodeOwnershipGraph(record)
	if err != nil {
		return contracts.Run{}, false, err
	}
	runRecord, err := controller.store.GetAggregate(ctx, runKey(runID))
	if err != nil {
		return contracts.Run{}, false, err
	}
	run, err := decodeRun(runRecord)
	if err != nil {
		return contracts.Run{}, false, err
	}
	graphRun, err := canonicaljson.Marshal(graph.TerminalRun)
	if err != nil {
		return contracts.Run{}, false, err
	}
	durableRun, err := canonicaljson.Marshal(run)
	if err != nil {
		return contracts.Run{}, false, err
	}
	if !run.Status.Terminal() || string(graphRun) != string(durableRun) {
		return contracts.Run{}, false, errors.New("durable ownership graph terminal Run differs from the Run aggregate")
	}
	return run, true, nil
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
	if record.Revision != 1 || graph.SchemaVersion != contracts.OwnershipGraphSchemaVersion ||
		graph.ClosureBase.Run.RunID != record.Key.ID || graph.TerminalRun.RunID != record.Key.ID ||
		graph.Revision != record.Revision {
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
