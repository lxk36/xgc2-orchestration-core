package execution

import (
	"errors"
	"testing"
	"time"

	"github.com/lxk36/xgc2-orchestration-core/sdk/go/contracts"
)

func TestInvocationWaitResumeAndSucceedUnderLeaseFence(t *testing.T) {
	t0 := time.Date(2026, 8, 12, 12, 0, 0, 0, time.UTC)
	ledger := activateTestInvocation(t, "run-wait", "simulate", t0)
	claimed, err := ClaimInvocation(ledger, ClaimInvocationCommand{
		InvocationID: ledger.Invocation.InvocationID, ExpectedRevision: ledger.Invocation.Revision,
		Phase: contracts.AttemptExecution, OwnerRef: "worker-1", LeaseToken: "secret-lease-1",
		LeaseExpiresAt: t0.Add(time.Minute), CommandID: "claim-1", At: t0.Add(time.Second),
	})
	if err != nil {
		t.Fatal(err)
	}
	ledger = claimed.Ledger
	attempt := ledger.Attempts[0]
	fence := AttemptFence{InvocationID: ledger.Invocation.InvocationID, InvocationRevision: ledger.Invocation.Revision, AttemptID: attempt.AttemptID, AttemptRevision: attempt.Revision, LeaseToken: "secret-lease-1", At: t0.Add(2 * time.Second)}
	waiting, err := TransitionInvocation(ledger, TransitionInvocationCommand{
		Fence: fence, To: contracts.InvocationWaiting, AttemptTo: contracts.AttemptWaiting,
		WaitRef: "timer-1", WaitGeneration: 1, CommandID: "wait-1",
	})
	if err != nil {
		t.Fatal(err)
	}
	ledger = waiting.Ledger
	attempt = ledger.Attempts[0]
	fence = AttemptFence{InvocationID: ledger.Invocation.InvocationID, InvocationRevision: ledger.Invocation.Revision, AttemptID: attempt.AttemptID, AttemptRevision: attempt.Revision, LeaseToken: "secret-lease-1", At: t0.Add(3 * time.Second)}
	resumed, err := TransitionInvocation(ledger, TransitionInvocationCommand{
		Fence: fence, To: contracts.InvocationRunning, AttemptTo: contracts.AttemptRunning, CommandID: "resume-1",
	})
	if err != nil {
		t.Fatal(err)
	}
	ledger = resumed.Ledger
	attempt = ledger.Attempts[0]
	fence = AttemptFence{InvocationID: ledger.Invocation.InvocationID, InvocationRevision: ledger.Invocation.Revision, AttemptID: attempt.AttemptID, AttemptRevision: attempt.Revision, LeaseToken: "secret-lease-1", At: t0.Add(4 * time.Second)}
	succeeded, err := TransitionInvocation(ledger, TransitionInvocationCommand{
		Fence: fence, To: contracts.InvocationSucceeded, AttemptTo: contracts.AttemptSucceeded,
		OutputRefsDigest: testDigest, CommandID: "succeed-1",
	})
	if err != nil {
		t.Fatal(err)
	}
	if succeeded.Ledger.Invocation.Status != contracts.InvocationSucceeded || succeeded.Ledger.Invocation.ActiveAttemptID != "" || !succeeded.Ledger.Attempts[0].LeaseExpiresAt.IsZero() {
		t.Fatalf("succeeded ledger = %#v", succeeded.Ledger)
	}
}

func TestInvocationRetryFreezesInputsAndRejectsExpiredLease(t *testing.T) {
	t0 := time.Date(2026, 8, 12, 13, 0, 0, 0, time.UTC)
	ledger := activateTestInvocation(t, "run-retry", "launch", t0)
	claimed, err := ClaimInvocation(ledger, ClaimInvocationCommand{
		InvocationID: ledger.Invocation.InvocationID, ExpectedRevision: ledger.Invocation.Revision,
		Phase: contracts.AttemptExecution, OwnerRef: "worker-1", LeaseToken: "lease-first",
		LeaseExpiresAt: t0.Add(10 * time.Second), CommandID: "claim-first", At: t0.Add(time.Second),
	})
	if err != nil {
		t.Fatal(err)
	}
	ledger = claimed.Ledger
	attempt := ledger.Attempts[0]
	expiredFence := AttemptFence{
		InvocationID: ledger.Invocation.InvocationID, InvocationRevision: ledger.Invocation.Revision,
		AttemptID: attempt.AttemptID, AttemptRevision: attempt.Revision, LeaseToken: "lease-first", At: attempt.LeaseExpiresAt,
	}
	_, err = HeartbeatAttempt(ledger, HeartbeatAttemptCommand{Fence: expiredFence, OwnerRef: "worker-1", LeaseExpiresAt: t0.Add(time.Minute), CommandID: "late-heartbeat"})
	if !errors.Is(err, ErrLeaseConflict) {
		t.Fatalf("expired lease error = %v", err)
	}

	failure := &contracts.StructuredFailure{Class: contracts.FailureTransient, Code: "transport.timeout", Message: "transport timeout"}
	fence := expiredFence
	fence.At = t0.Add(2 * time.Second)
	retry, err := TransitionInvocation(ledger, TransitionInvocationCommand{
		Fence: fence, To: contracts.InvocationRetryWait, AttemptTo: contracts.AttemptFailed,
		Failure: failure, RetryAt: timePointer(t0.Add(20 * time.Second)), CommandID: "retry-wait",
	})
	if err != nil {
		t.Fatal(err)
	}
	ledger = retry.Ledger
	_, err = ClaimInvocation(ledger, ClaimInvocationCommand{
		InvocationID: ledger.Invocation.InvocationID, ExpectedRevision: ledger.Invocation.Revision,
		Phase: contracts.AttemptExecution, OwnerRef: "worker-early", LeaseToken: "lease-early",
		LeaseExpiresAt: t0.Add(time.Minute), CommandID: "claim-early", At: t0.Add(19 * time.Second),
	})
	if !errors.Is(err, ErrInvalidTransition) {
		t.Fatalf("early retry claim error = %v", err)
	}
	second, err := ClaimInvocation(ledger, ClaimInvocationCommand{
		InvocationID: ledger.Invocation.InvocationID, ExpectedRevision: ledger.Invocation.Revision,
		Phase: contracts.AttemptExecution, OwnerRef: "worker-2", LeaseToken: "lease-second",
		LeaseExpiresAt: t0.Add(time.Minute), CommandID: "claim-second", At: t0.Add(21 * time.Second),
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(second.Ledger.Attempts) != 2 || second.Ledger.Attempts[1].Ordinal != 2 ||
		second.Ledger.Attempts[0].ResolvedInputDigest != second.Ledger.Attempts[1].ResolvedInputDigest {
		t.Fatalf("retry ledger = %#v", second.Ledger)
	}
	if !second.Ledger.Attempts[0].LeaseExpiresAt.IsZero() || second.Ledger.Attempts[1].LeaseExpiresAt.IsZero() {
		t.Fatal("retry retained the wrong lease lifetime")
	}
}

func TestExpiredAttemptMustBeExplicitlyRecoveredAndStaleWorkerIsFenced(t *testing.T) {
	t0 := time.Date(2026, 8, 12, 13, 30, 0, 0, time.UTC)
	ledger := activateTestInvocation(t, "run-expired", "pure-node", t0)
	claimed, err := ClaimInvocation(ledger, ClaimInvocationCommand{
		InvocationID: ledger.Invocation.InvocationID, ExpectedRevision: ledger.Invocation.Revision,
		Phase: contracts.AttemptExecution, OwnerRef: "worker-old", LeaseToken: "lease-old",
		LeaseExpiresAt: t0.Add(5 * time.Second), CommandID: "claim-expiring", At: t0.Add(time.Second),
	})
	if err != nil {
		t.Fatal(err)
	}
	ledger = claimed.Ledger
	attempt := ledger.Attempts[0]
	failure := contracts.StructuredFailure{Class: contracts.FailureUncertain, Code: "worker.lease-expired", Message: "worker lease expired before a durable result"}
	_, err = ExpireInvocationAttempt(ledger, ExpireInvocationAttemptCommand{
		InvocationID: ledger.Invocation.InvocationID, ExpectedRevision: ledger.Invocation.Revision,
		AttemptID: attempt.AttemptID, ExpectedAttemptRevision: attempt.Revision, Retry: true,
		Failure: failure, CommandID: "expire-too-early", At: t0.Add(4 * time.Second),
	})
	if !errors.Is(err, ErrLeaseConflict) {
		t.Fatalf("early expiry error = %v", err)
	}

	recovered, err := ExpireInvocationAttempt(ledger, ExpireInvocationAttemptCommand{
		InvocationID: ledger.Invocation.InvocationID, ExpectedRevision: ledger.Invocation.Revision,
		AttemptID: attempt.AttemptID, ExpectedAttemptRevision: attempt.Revision, Retry: true,
		Failure: failure, CommandID: "expire-after-lease", At: attempt.LeaseExpiresAt,
	})
	if err != nil {
		t.Fatal(err)
	}
	if recovered.Ledger.Invocation.Status != contracts.InvocationReady || recovered.Ledger.Invocation.ActiveAttemptID != "" ||
		recovered.Ledger.Attempts[0].Status != contracts.AttemptAbandoned || !recovered.Ledger.Attempts[0].LeaseExpiresAt.IsZero() {
		t.Fatalf("recovered ledger = %#v", recovered.Ledger)
	}

	staleFence := AttemptFence{
		InvocationID: ledger.Invocation.InvocationID, InvocationRevision: ledger.Invocation.Revision,
		AttemptID: attempt.AttemptID, AttemptRevision: attempt.Revision, LeaseToken: "lease-old", At: t0.Add(6 * time.Second),
	}
	_, err = TransitionInvocation(recovered.Ledger, TransitionInvocationCommand{
		Fence: staleFence, To: contracts.InvocationSucceeded, AttemptTo: contracts.AttemptSucceeded,
		OutputRefsDigest: testDigest, CommandID: "stale-success",
	})
	if !errors.Is(err, ErrRevisionConflict) && !errors.Is(err, ErrLeaseConflict) {
		t.Fatalf("stale worker transition error = %v", err)
	}

	second, err := ClaimInvocation(recovered.Ledger, ClaimInvocationCommand{
		InvocationID: recovered.Ledger.Invocation.InvocationID, ExpectedRevision: recovered.Ledger.Invocation.Revision,
		Phase: contracts.AttemptExecution, OwnerRef: "worker-new", LeaseToken: "lease-new",
		LeaseExpiresAt: t0.Add(time.Minute), CommandID: "claim-after-expiry", At: t0.Add(7 * time.Second),
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(second.Ledger.Attempts) != 2 || second.Ledger.Attempts[1].Ordinal != 2 {
		t.Fatalf("second attempt ledger = %#v", second.Ledger)
	}
}

func TestInvocationCompensationUsesSeparateAttemptsAndRetryClock(t *testing.T) {
	t0 := time.Date(2026, 8, 12, 14, 0, 0, 0, time.UTC)
	ledger := activateTestInvocation(t, "run-compensation", "allocate", t0)
	claimed, err := ClaimInvocation(ledger, ClaimInvocationCommand{
		InvocationID: ledger.Invocation.InvocationID, ExpectedRevision: ledger.Invocation.Revision,
		Phase: contracts.AttemptExecution, OwnerRef: "worker-primary", LeaseToken: "lease-primary",
		LeaseExpiresAt: t0.Add(time.Minute), CommandID: "claim-primary", At: t0.Add(time.Second),
	})
	if err != nil {
		t.Fatal(err)
	}
	ledger = claimed.Ledger
	attempt := ledger.Attempts[0]
	primary, err := TransitionInvocation(ledger, TransitionInvocationCommand{
		Fence: AttemptFence{InvocationID: ledger.Invocation.InvocationID, InvocationRevision: ledger.Invocation.Revision, AttemptID: attempt.AttemptID, AttemptRevision: attempt.Revision, LeaseToken: "lease-primary", At: t0.Add(2 * time.Second)},
		To:    contracts.InvocationSucceeded, AttemptTo: contracts.AttemptSucceeded, OutputRefsDigest: testDigest, CommandID: "primary-success",
	})
	if err != nil {
		t.Fatal(err)
	}
	scheduled, err := ScheduleInvocationCompensation(primary.Ledger, ScheduleInvocationCompensationCommand{
		InvocationID: ledger.Invocation.InvocationID, ExpectedRevision: primary.Ledger.Invocation.Revision,
		CommandID: "schedule-compensation", At: t0.Add(3 * time.Second),
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(scheduled.Intents) != 1 || scheduled.Intents[0].Kind != contracts.IntentCleanup {
		t.Fatalf("compensation scheduling intents = %#v", scheduled.Intents)
	}
	compClaim, err := ClaimCompensation(scheduled.Ledger, ClaimCompensationCommand{
		InvocationID: ledger.Invocation.InvocationID, ExpectedRevision: scheduled.Ledger.Invocation.Revision,
		OwnerRef: "worker-cleanup", LeaseToken: "lease-cleanup-1", LeaseExpiresAt: t0.Add(time.Minute),
		CommandID: "claim-compensation-1", At: t0.Add(4 * time.Second),
	})
	if err != nil {
		t.Fatal(err)
	}
	compAttempt := compClaim.Ledger.Attempts[1]
	retryAt := t0.Add(10 * time.Second)
	failure := &contracts.StructuredFailure{Class: contracts.FailureTransient, Code: "cleanup.busy", Message: "cleanup target busy"}
	retry, err := TransitionInvocationCompensation(compClaim.Ledger, TransitionInvocationCompensationCommand{
		Fence: AttemptFence{InvocationID: ledger.Invocation.InvocationID, InvocationRevision: compClaim.Ledger.Invocation.Revision, AttemptID: compAttempt.AttemptID, AttemptRevision: compAttempt.Revision, LeaseToken: "lease-cleanup-1", At: t0.Add(5 * time.Second)},
		To:    contracts.CompensationRetryWait, AttemptTo: contracts.AttemptFailed, Failure: failure, RetryAt: &retryAt, CommandID: "compensation-retry",
	})
	if err != nil {
		t.Fatal(err)
	}
	_, err = ClaimCompensation(retry.Ledger, ClaimCompensationCommand{
		InvocationID: ledger.Invocation.InvocationID, ExpectedRevision: retry.Ledger.Invocation.Revision,
		OwnerRef: "worker-cleanup", LeaseToken: "lease-cleanup-early", LeaseExpiresAt: t0.Add(time.Minute),
		CommandID: "claim-compensation-early", At: t0.Add(9 * time.Second),
	})
	if !errors.Is(err, ErrInvalidTransition) {
		t.Fatalf("early compensation retry error = %v", err)
	}
	second, err := ClaimCompensation(retry.Ledger, ClaimCompensationCommand{
		InvocationID: ledger.Invocation.InvocationID, ExpectedRevision: retry.Ledger.Invocation.Revision,
		OwnerRef: "worker-cleanup", LeaseToken: "lease-cleanup-2", LeaseExpiresAt: t0.Add(time.Minute),
		CommandID: "claim-compensation-2", At: retryAt,
	})
	if err != nil {
		t.Fatal(err)
	}
	secondAttempt := second.Ledger.Attempts[2]
	done, err := TransitionInvocationCompensation(second.Ledger, TransitionInvocationCompensationCommand{
		Fence: AttemptFence{InvocationID: ledger.Invocation.InvocationID, InvocationRevision: second.Ledger.Invocation.Revision, AttemptID: secondAttempt.AttemptID, AttemptRevision: secondAttempt.Revision, LeaseToken: "lease-cleanup-2", At: t0.Add(11 * time.Second)},
		To:    contracts.CompensationSucceeded, AttemptTo: contracts.AttemptSucceeded, CommandID: "compensation-success",
	})
	if err != nil {
		t.Fatal(err)
	}
	if done.Ledger.Invocation.Status != contracts.InvocationSucceeded || done.Ledger.Invocation.CompensationStatus != contracts.CompensationSucceeded ||
		done.Ledger.Invocation.ExecutionAttemptCount != 1 || done.Ledger.Invocation.CompensationAttemptCount != 2 {
		t.Fatalf("compensation ledger = %#v", done.Ledger)
	}
}

func TestReadyInvocationCanSkipWithoutLease(t *testing.T) {
	t0 := time.Date(2026, 8, 12, 15, 0, 0, 0, time.UTC)
	ledger := activateTestInvocation(t, "run-skip", "conditional", t0)
	decision, err := ResolveUnleasedInvocation(ledger, ResolveUnleasedInvocationCommand{
		InvocationID: ledger.Invocation.InvocationID, ExpectedRevision: ledger.Invocation.Revision,
		To: contracts.InvocationSkipped, CommandID: "skip-condition", At: t0.Add(time.Second),
	})
	if err != nil {
		t.Fatal(err)
	}
	if decision.Ledger.Invocation.Status != contracts.InvocationSkipped || decision.Ledger.Invocation.CompensationStatus != contracts.CompensationNotRequired || len(decision.Ledger.Attempts) != 0 {
		t.Fatalf("skipped ledger = %#v", decision.Ledger)
	}
}

func activateTestInvocation(t *testing.T, runID, nodeID string, at time.Time) contracts.InvocationLedger {
	t.Helper()
	id, err := StableInvocationID(runID, nodeID)
	if err != nil {
		t.Fatal(err)
	}
	decision, err := ActivateInvocation(ActivateInvocationCommand{
		InvocationID: id, NamespaceID: "lab", RunID: runID, NodeID: nodeID,
		TypeRef: "xgc.test-node/v1", DescriptorDigest: testDigest, ResolvedInputDigest: testDigest,
		InputRefsDigest: testDigest, Compensatable: true, CommandID: "activate-" + nodeID, At: at,
	})
	if err != nil {
		t.Fatal(err)
	}
	return decision.Ledger
}

func timePointer(value time.Time) *time.Time { return &value }
