// Package ownership derives terminal Run closure facts from an exact,
// product-neutral ownership graph snapshot.
package ownership

import (
	"errors"
	"fmt"

	"github.com/lxk36/xgc2-orchestration-core/kernel/effect"
	"github.com/lxk36/xgc2-orchestration-core/kernel/execution"
	"github.com/lxk36/xgc2-orchestration-core/kernel/runtime"
	"github.com/lxk36/xgc2-orchestration-core/sdk/go/contracts"
)

func ClosureFacts(graph contracts.OwnershipGraph) (contracts.RunClosureFacts, error) {
	if graph.Revision == 0 {
		return contracts.RunClosureFacts{}, errors.New("ownership graph revision is required")
	}
	if err := execution.ValidateRun(graph.Run); err != nil {
		return contracts.RunClosureFacts{}, fmt.Errorf("ownership graph run: %w", err)
	}
	facts := contracts.RunClosureFacts{RunRevision: graph.Run.Revision, OwnershipGraphRevision: graph.Revision}
	seen := make(map[string]string)
	for index, ledger := range graph.Invocations {
		if err := execution.ValidateInvocationLedger(ledger); err != nil {
			return contracts.RunClosureFacts{}, fmt.Errorf("invocation %d: %w", index, err)
		}
		if ledger.Invocation.RunID != graph.Run.RunID {
			return contracts.RunClosureFacts{}, errors.New("ownership graph invocation belongs to another run")
		}
		if err := unique(seen, ledger.Invocation.InvocationID, "invocation"); err != nil {
			return contracts.RunClosureFacts{}, err
		}
		if !ledger.Invocation.Status.Terminal() || !ledger.Invocation.CompensationStatus.Terminal() {
			facts.ActiveInvocationCount++
		}
		if ledger.Invocation.Status == contracts.InvocationWaiting {
			facts.OpenWaitCount++
		}
		for _, attempt := range ledger.Attempts {
			if !attempt.Status.Terminal() {
				facts.LiveAttemptCount++
			}
		}
	}
	for index, child := range graph.ChildRuns {
		if err := execution.ValidateRun(child); err != nil {
			return contracts.RunClosureFacts{}, fmt.Errorf("child run %d: %w", index, err)
		}
		if child.Parent == nil || child.Parent.ParentRunID != graph.Run.RunID || child.RootRunID != graph.Run.RootRunID {
			return contracts.RunClosureFacts{}, errors.New("ownership graph child link is invalid")
		}
		if err := unique(seen, child.RunID, "child run"); err != nil {
			return contracts.RunClosureFacts{}, err
		}
		if !child.Status.Terminal() {
			facts.OpenChildCount++
		}
	}
	for index, record := range graph.Effects {
		if err := effect.ValidateRecord(record); err != nil {
			return contracts.RunClosureFacts{}, fmt.Errorf("effect %d: %w", index, err)
		}
		if record.Intent.RunID != graph.Run.RunID {
			return contracts.RunClosureFacts{}, errors.New("ownership graph effect belongs to another run")
		}
		if err := unique(seen, record.EffectID, "effect"); err != nil {
			return contracts.RunClosureFacts{}, err
		}
		if !record.State.Terminal() || record.State == contracts.EffectUncertain {
			facts.OpenEffectCount++
		}
		if record.State == contracts.EffectApplied && record.Intent.Ownership == contracts.EffectOwned &&
			record.Intent.CompensationPolicy != contracts.CompensationNone && !record.CompensationState.Terminal() {
			facts.OpenEffectCompensationCount++
		}
	}
	for index, binding := range graph.Runtimes {
		if err := runtime.ValidateBinding(binding); err != nil {
			return contracts.RunClosureFacts{}, fmt.Errorf("runtime %d: %w", index, err)
		}
		if binding.RunID != graph.Run.RunID {
			return contracts.RunClosureFacts{}, errors.New("ownership graph runtime belongs to another run")
		}
		if err := unique(seen, binding.BindingID, "runtime"); err != nil {
			return contracts.RunClosureFacts{}, err
		}
		if binding.Ownership == contracts.EffectOwned && !binding.State.Terminal() {
			facts.OpenOwnedRuntimeCount++
		}
	}
	for _, resource := range graph.Resources {
		if !contracts.ValidIdentifier(resource.BindingID) || resource.RunID != graph.Run.RunID || !resource.Ownership.Valid() || !resource.State.Valid() {
			return contracts.RunClosureFacts{}, errors.New("ownership graph resource fact is invalid")
		}
		if err := unique(seen, resource.BindingID, "resource"); err != nil {
			return contracts.RunClosureFacts{}, err
		}
		if resource.Ownership == contracts.EffectOwned && !resource.State.Terminal() {
			facts.OpenOwnedResourceCount++
		}
	}
	return facts, nil
}

func unique(seen map[string]string, id, kind string) error {
	if previous, exists := seen[id]; exists {
		return fmt.Errorf("ownership identity %q is shared by %s and %s", id, previous, kind)
	}
	seen[id] = kind
	return nil
}
