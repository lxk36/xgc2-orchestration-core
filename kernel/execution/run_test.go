package execution

import (
	"errors"
	"testing"
	"time"

	"github.com/lxk36/xgc2-orchestration-core/kernel/canonicaljson"
	"github.com/lxk36/xgc2-orchestration-core/sdk/go/contracts"
)

const testDigest = "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"

func TestRunRequiresStoppingAndOwnershipClosure(t *testing.T) {
	t0 := time.Date(2026, 8, 12, 10, 0, 0, 0, time.UTC)
	admitted, err := AdmitRun(AdmitRunCommand{
		RunID: "run-1", NamespaceID: "lab", ActionRef: contracts.ActionRef{ActionID: "experiment", Version: "v1", Digest: testDigest},
		ExecutionPlanRef: "plan-1", PlanDigest: testDigest, TriggerRef: "trigger-1", TriggerDigest: testDigest,
		InputDigest: testDigest, ActorRef: "operator", SourceRef: "panel", CommandID: "admit-1", At: t0,
	})
	if err != nil {
		t.Fatal(err)
	}
	if admitted.Run.Status != contracts.RunAccepted || admitted.Run.RootRunID != admitted.Run.RunID || len(admitted.Events) != 1 {
		t.Fatalf("admitted decision = %#v", admitted)
	}

	queued := transitionRun(t, admitted.Run, contracts.RunQueued, t0.Add(time.Second), nil, contracts.RunClosureFacts{})
	running := transitionRun(t, queued, contracts.RunRunning, t0.Add(2*time.Second), nil, contracts.RunClosureFacts{})
	if running.StartedAt == nil {
		t.Fatal("running run has no startedAt")
	}

	_, err = TransitionRun(running, RunTransitionCommand{
		RunID: running.RunID, ExpectedRevision: running.Revision, To: contracts.RunFailed,
		Closure: contracts.RunClosureFacts{RunRevision: running.Revision}, CommandID: "illegal-direct-fail", At: t0.Add(3 * time.Second),
	})
	if !errors.Is(err, ErrInvalidTransition) {
		t.Fatalf("direct failure error = %v", err)
	}

	failure := &contracts.StructuredFailure{Class: contracts.FailurePermanent, Code: "node.failed", Message: "node failed", EvidenceRef: "log-1"}
	stoppingDecision, err := TransitionRun(running, RunTransitionCommand{
		RunID: running.RunID, ExpectedRevision: running.Revision, To: contracts.RunStopping,
		Termination: &contracts.TerminationIntent{Kind: contracts.TerminationFailed, RequestedRevision: running.Revision, RequestedBy: "controller", ReasonCode: "node.failed", PrimaryFailure: failure, CommandID: "stop-intent-1", RequestedAt: t0.Add(3 * time.Second)},
		CommandID:   "stop-1", At: t0.Add(3 * time.Second),
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(stoppingDecision.Intents) != 1 || stoppingDecision.Intents[0].Kind != contracts.IntentCleanup {
		t.Fatalf("stopping intents = %#v", stoppingDecision.Intents)
	}
	digest, err := canonicaljson.DigestValue(stoppingDecision.Intents[0].Payload)
	if err != nil || digest != stoppingDecision.Intents[0].PayloadDigest {
		t.Fatalf("cleanup payload digest = %q, computed %q, err %v", stoppingDecision.Intents[0].PayloadDigest, digest, err)
	}

	stopping := stoppingDecision.Run
	tamperedTermination := cloneRun(stopping)
	tamperedTermination.Termination.RequestedRevision++
	if err := ValidateRun(tamperedTermination); err == nil {
		t.Fatal("stopping Run accepted a termination intent with a different requested revision")
	}
	_, err = TransitionRun(stopping, RunTransitionCommand{
		RunID: stopping.RunID, ExpectedRevision: stopping.Revision, To: contracts.RunFailed,
		Closure: contracts.RunClosureFacts{RunRevision: stopping.Revision, LiveAttemptCount: 1}, CommandID: "finish-open", At: t0.Add(4 * time.Second),
	})
	if !errors.Is(err, ErrClosureOpen) {
		t.Fatalf("open closure error = %v", err)
	}

	terminal, err := TransitionRun(stopping, RunTransitionCommand{
		RunID: stopping.RunID, ExpectedRevision: stopping.Revision, To: contracts.RunFailed,
		Closure:         contracts.RunClosureFacts{RunRevision: stopping.Revision, OwnershipGraphRevision: 7},
		CleanupFailures: []contracts.StructuredFailure{}, CommandID: "finish-closed", At: t0.Add(5 * time.Second),
	})
	if err != nil {
		t.Fatal(err)
	}
	if terminal.Run.Status != contracts.RunFailed || terminal.Run.PrimaryFailure == nil || terminal.Run.TerminationKind != contracts.TerminationFailed {
		t.Fatalf("terminal run = %#v", terminal.Run)
	}
	if _, err := TransitionRun(terminal.Run, RunTransitionCommand{RunID: terminal.Run.RunID, ExpectedRevision: terminal.Run.Revision, To: contracts.RunStopping, CommandID: "late", At: t0.Add(6 * time.Second)}); err == nil {
		t.Fatal("terminal run accepted another transition")
	}
}

func TestRunSuccessAlsoRequiresClosure(t *testing.T) {
	t0 := time.Date(2026, 8, 12, 11, 0, 0, 0, time.UTC)
	admitted, err := AdmitRun(AdmitRunCommand{
		RunID: "run-success", NamespaceID: "lab", ActionRef: contracts.ActionRef{ActionID: "workflow", Version: "v1", Digest: testDigest},
		ExecutionPlanRef: "plan-1", PlanDigest: testDigest, TriggerRef: "trigger-1", TriggerDigest: testDigest,
		InputDigest: testDigest, ActorRef: "operator", SourceRef: "api", CommandID: "admit-success", At: t0,
	})
	if err != nil {
		t.Fatal(err)
	}
	running := transitionRun(t, admitted.Run, contracts.RunRunning, t0.Add(time.Second), nil, contracts.RunClosureFacts{})
	_, err = TransitionRun(running, RunTransitionCommand{
		RunID: running.RunID, ExpectedRevision: running.Revision, To: contracts.RunSucceeded, ResultRef: "result-1",
		Closure: contracts.RunClosureFacts{RunRevision: running.Revision, OpenChildCount: 1}, CommandID: "success-open", At: t0.Add(2 * time.Second),
	})
	if !errors.Is(err, ErrClosureOpen) {
		t.Fatalf("success with open child error = %v", err)
	}
	decision, err := TransitionRun(running, RunTransitionCommand{
		RunID: running.RunID, ExpectedRevision: running.Revision, To: contracts.RunSucceeded, ResultRef: "result-1",
		Closure: contracts.RunClosureFacts{RunRevision: running.Revision}, CommandID: "success-closed", At: t0.Add(2 * time.Second),
	})
	if err != nil {
		t.Fatal(err)
	}
	if decision.Run.TerminationKind != contracts.TerminationCompleted {
		t.Fatalf("termination = %s", decision.Run.TerminationKind)
	}
}

func transitionRun(t *testing.T, current contracts.Run, to contracts.RunStatus, at time.Time, termination *contracts.TerminationIntent, closure contracts.RunClosureFacts) contracts.Run {
	t.Helper()
	decision, err := TransitionRun(current, RunTransitionCommand{
		RunID: current.RunID, ExpectedRevision: current.Revision, To: to, Termination: termination,
		Closure: closure, CommandID: "command-" + string(to), At: at,
	})
	if err != nil {
		t.Fatal(err)
	}
	return decision.Run
}
