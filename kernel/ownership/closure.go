// Package ownership derives terminal Run closure facts from an exact,
// product-neutral ownership graph snapshot.
package ownership

import (
	"errors"
	"fmt"

	"github.com/lxk36/xgc2-orchestration-core/kernel/canonicaljson"
	"github.com/lxk36/xgc2-orchestration-core/kernel/effect"
	"github.com/lxk36/xgc2-orchestration-core/kernel/execution"
	"github.com/lxk36/xgc2-orchestration-core/kernel/runtime"
	"github.com/lxk36/xgc2-orchestration-core/sdk/go/contracts"
)

func ClosureFacts(graph contracts.OwnershipGraph) (contracts.RunClosureFacts, error) {
	if graph.SchemaVersion != contracts.OwnershipGraphSchemaVersion || graph.Revision == 0 {
		return contracts.RunClosureFacts{}, errors.New("ownership graph schema and revision are required")
	}
	facts, err := DeriveClosureFacts(graph.ClosureBase, graph.Revision)
	if err != nil {
		return contracts.RunClosureFacts{}, err
	}
	if facts != graph.ClosureFacts {
		return contracts.RunClosureFacts{}, errors.New("persisted closure facts differ from the closure-base proof")
	}
	if !facts.Satisfied() {
		return contracts.RunClosureFacts{}, execution.ErrClosureOpen
	}
	if err := validateTerminalProjection(graph.ClosureBase.Run, graph.TerminalRun, facts); err != nil {
		return contracts.RunClosureFacts{}, err
	}
	return facts, nil
}

// DeriveClosureFacts computes the proof output from one exact pre-terminal
// snapshot. It does not accept or inspect a terminal Run projection.
func DeriveClosureFacts(base contracts.OwnershipClosureBase, graphRevision uint64) (contracts.RunClosureFacts, error) {
	if graphRevision == 0 {
		return contracts.RunClosureFacts{}, errors.New("ownership graph revision is required")
	}
	if err := execution.ValidateRun(base.Run); err != nil {
		return contracts.RunClosureFacts{}, fmt.Errorf("ownership graph closure-base run: %w", err)
	}
	if base.Run.Status.Terminal() {
		return contracts.RunClosureFacts{}, errors.New("ownership graph closure-base run is already terminal")
	}
	facts := contracts.RunClosureFacts{RunRevision: base.Run.Revision, OwnershipGraphRevision: graphRevision}
	seen := make(map[string]string)
	for index, ledger := range base.Invocations {
		if err := execution.ValidateInvocationLedger(ledger); err != nil {
			return contracts.RunClosureFacts{}, fmt.Errorf("invocation %d: %w", index, err)
		}
		if ledger.Invocation.RunID != base.Run.RunID {
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
	for index, child := range base.ChildRuns {
		if err := execution.ValidateRun(child); err != nil {
			return contracts.RunClosureFacts{}, fmt.Errorf("child run %d: %w", index, err)
		}
		if child.Parent == nil || child.Parent.ParentRunID != base.Run.RunID || child.RootRunID != base.Run.RootRunID {
			return contracts.RunClosureFacts{}, errors.New("ownership graph child link is invalid")
		}
		if err := unique(seen, child.RunID, "child run"); err != nil {
			return contracts.RunClosureFacts{}, err
		}
		if !child.Status.Terminal() {
			facts.OpenChildCount++
		}
	}
	for index, record := range base.Effects {
		if err := effect.ValidateRecord(record); err != nil {
			return contracts.RunClosureFacts{}, fmt.Errorf("effect %d: %w", index, err)
		}
		if record.Intent.RunID != base.Run.RunID {
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
	for index, binding := range base.Runtimes {
		if err := runtime.ValidateBinding(binding); err != nil {
			return contracts.RunClosureFacts{}, fmt.Errorf("runtime %d: %w", index, err)
		}
		if binding.RunID != base.Run.RunID {
			return contracts.RunClosureFacts{}, errors.New("ownership graph runtime belongs to another run")
		}
		if err := unique(seen, binding.BindingID, "runtime"); err != nil {
			return contracts.RunClosureFacts{}, err
		}
		if binding.Ownership == contracts.EffectOwned && !binding.State.Terminal() {
			facts.OpenOwnedRuntimeCount++
		}
	}
	for _, resource := range base.Resources {
		if !contracts.ValidIdentifier(resource.BindingID) || resource.RunID != base.Run.RunID || !resource.Ownership.Valid() || !resource.State.Valid() {
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

func validateTerminalProjection(base, terminal contracts.Run, facts contracts.RunClosureFacts) error {
	if err := execution.ValidateRun(terminal); err != nil {
		return fmt.Errorf("ownership graph terminal run: %w", err)
	}
	if !terminal.Status.Terminal() || terminal.RunID != base.RunID || terminal.Revision != base.Revision+1 ||
		terminal.UpdatedAt.Before(base.UpdatedAt) {
		return errors.New("ownership graph terminal run is not the exact next run revision")
	}
	decision, err := execution.TransitionRun(base, execution.RunTransitionCommand{
		RunID: base.RunID, ExpectedRevision: base.Revision, To: terminal.Status,
		ResultRef: terminal.ResultRef, CleanupFailures: terminal.CleanupFailures, Closure: facts,
		CommandID: "verify-ownership-terminal", At: terminal.UpdatedAt,
	})
	if err != nil {
		return fmt.Errorf("ownership graph terminal transition: %w", err)
	}
	if !exactRun(decision.Run, terminal) {
		return errors.New("ownership graph terminal run differs from the proved terminal transition")
	}
	return nil
}

func exactRun(left, right contracts.Run) bool {
	leftRaw, leftErr := canonicaljson.Marshal(left)
	rightRaw, rightErr := canonicaljson.Marshal(right)
	return leftErr == nil && rightErr == nil && string(leftRaw) == string(rightRaw)
}

func unique(seen map[string]string, id, kind string) error {
	if previous, exists := seen[id]; exists {
		return fmt.Errorf("ownership identity %q is shared by %s and %s", id, previous, kind)
	}
	seen[id] = kind
	return nil
}
