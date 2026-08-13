package effect

import (
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/lxk36/xgc2-orchestration-core/kernel/canonicaljson"
	"github.com/lxk36/xgc2-orchestration-core/kernel/execution"
	"github.com/lxk36/xgc2-orchestration-core/sdk/go/contracts"
)

const digest = "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"

func TestPrepareBeforeOutboxAndImmutableReceiptLedger(t *testing.T) {
	t0 := time.Date(2026, 8, 12, 14, 0, 0, 0, time.UTC)
	record := prepareTestEffect(t, t0, contracts.CompensationRequired)

	envelope := testEnvelope(t, record.Intent, "command-1", "idempotency-secret-1", "capability-secret-1", t0.Add(time.Minute))
	begin, err := Begin(record, BeginCommand{
		EffectID: record.EffectID, ExpectedRevision: record.Revision, Envelope: envelope,
		CommandID: envelope.CommandID, At: t0.Add(time.Second),
	})
	if err != nil {
		t.Fatal(err)
	}
	if record.State != contracts.EffectPrepared || record.CommandID != "" {
		t.Fatal("begin mutated the prepared input aggregate")
	}
	if begin.Effect.State != contracts.EffectApplying || len(begin.Intents) != 1 || begin.Intents[0].Kind != contracts.IntentOutbox || begin.Ledger == nil {
		t.Fatalf("begin decision = %#v", begin)
	}
	wire, err := json.Marshal(begin.Intents[0].Payload)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(wire), "idempotency-secret-1") || strings.Contains(string(wire), "capability-secret-1") {
		t.Fatalf("outbox leaked private command material: %s", wire)
	}

	fenceDigest, err := FenceDigest(envelope.Fence)
	if err != nil {
		t.Fatal(err)
	}
	accepted := receiptFor(t, envelope, 1, contracts.ReceiptAccepted, t0.Add(2*time.Second), fenceDigest)
	observed, err := Observe(begin.Effect, *begin.Ledger, ObserveCommand{
		EffectID: record.EffectID, ExpectedRevision: begin.Effect.Revision, Receipt: accepted, CommandID: "observe-accepted",
	})
	if err != nil {
		t.Fatal(err)
	}
	if observed.Effect.State != contracts.EffectApplying || len(observed.Ledger.Receipts) != 1 || len(begin.Ledger.Receipts) != 0 {
		t.Fatal("accepted receipt did not preserve immutable applying ledger")
	}

	succeeded := receiptFor(t, envelope, 2, contracts.ReceiptSucceeded, t0.Add(3*time.Second), fenceDigest)
	succeeded.ResultDigest = digest
	succeeded.ResultArtifactRef = "artifact-1"
	succeeded.ExternalIdentity = "process-42"
	terminal, err := Observe(observed.Effect, *observed.Ledger, ObserveCommand{
		EffectID: record.EffectID, ExpectedRevision: observed.Effect.Revision, Receipt: succeeded, CommandID: "observe-success",
	})
	if err != nil {
		t.Fatal(err)
	}
	if terminal.Effect.State != contracts.EffectApplied || terminal.Effect.ResultDigest != digest || len(terminal.Ledger.Receipts) != 2 {
		t.Fatalf("terminal effect = %#v", terminal)
	}
	late := receiptFor(t, envelope, 3, contracts.ReceiptFailed, t0.Add(4*time.Second), fenceDigest)
	late.Failure = &contracts.StructuredFailure{Class: contracts.FailurePermanent, Code: "late.failure", Message: "late failure"}
	if _, err := Observe(terminal.Effect, *terminal.Ledger, ObserveCommand{EffectID: record.EffectID, ExpectedRevision: terminal.Effect.Revision, Receipt: late, CommandID: "late-observation"}); err == nil {
		t.Fatal("terminal receipt ledger accepted a later response")
	}
}

func TestUncertainEffectCanReconcileButCannotBlindRetry(t *testing.T) {
	t0 := time.Date(2026, 8, 12, 15, 0, 0, 0, time.UTC)
	record := prepareTestEffect(t, t0, contracts.CompensationNone)
	envelope := testEnvelope(t, record.Intent, "command-uncertain", "idempotency-uncertain", "capability-uncertain", t0.Add(time.Minute))
	begin, err := Begin(record, BeginCommand{EffectID: record.EffectID, ExpectedRevision: record.Revision, Envelope: envelope, CommandID: envelope.CommandID, At: t0.Add(time.Second)})
	if err != nil {
		t.Fatal(err)
	}
	fenceDigest, _ := FenceDigest(envelope.Fence)
	accepted := receiptFor(t, envelope, 1, contracts.ReceiptAccepted, t0.Add(2*time.Second), fenceDigest)
	acceptedDecision, err := Observe(begin.Effect, *begin.Ledger, ObserveCommand{EffectID: record.EffectID, ExpectedRevision: begin.Effect.Revision, Receipt: accepted, CommandID: "observe-accepted"})
	if err != nil {
		t.Fatal(err)
	}
	receipt := receiptFor(t, envelope, 2, contracts.ReceiptUncertain, t0.Add(3*time.Second), fenceDigest)
	receipt.ExternalIdentity = "maybe-process-42"
	receipt.Failure = &contracts.StructuredFailure{Class: contracts.FailureUncertain, Code: "transport.lost", Message: "provider result was not observed"}
	uncertain, err := Observe(acceptedDecision.Effect, *acceptedDecision.Ledger, ObserveCommand{EffectID: record.EffectID, ExpectedRevision: acceptedDecision.Effect.Revision, Receipt: receipt, CommandID: "observe-uncertain"})
	if err != nil {
		t.Fatal(err)
	}
	if uncertain.Effect.State != contracts.EffectUncertain {
		t.Fatalf("state = %s", uncertain.Effect.State)
	}

	retryEnvelope := testEnvelope(t, record.Intent, "command-blind-retry", "new-key", "new-capability", t0.Add(2*time.Minute))
	_, err = Begin(uncertain.Effect, BeginCommand{EffectID: record.EffectID, ExpectedRevision: uncertain.Effect.Revision, Envelope: retryEnvelope, CommandID: retryEnvelope.CommandID, At: t0.Add(4 * time.Second)})
	if !errors.Is(err, ErrReconcileRequired) {
		t.Fatalf("blind retry error = %v", err)
	}

	reconcile, err := RequestReconciliation(uncertain.Effect, ReconcileCommand{
		EffectID: record.EffectID, ExpectedRevision: uncertain.Effect.Revision, CommandID: "reconcile-1", At: t0.Add(4 * time.Second),
	})
	if err != nil {
		t.Fatal(err)
	}
	if reconcile.Effect.State != contracts.EffectUncertain || len(reconcile.Intents) != 1 || reconcile.Intents[0].Kind != contracts.IntentReconcile {
		t.Fatalf("reconcile decision = %#v", reconcile)
	}
}

func TestCompensationHasIndependentRetryLifecycle(t *testing.T) {
	t0 := time.Date(2026, 8, 12, 16, 0, 0, 0, time.UTC)
	record := applyTestEffect(t, t0)
	pending := transitionCompensation(t, record, CompensationCommand{
		To: contracts.EffectCompensationPending, CommandID: "schedule-compensation", At: t0.Add(4 * time.Second),
	})
	if len(pending.Intents) != 1 || pending.Intents[0].Kind != contracts.IntentCleanup {
		t.Fatalf("pending intents = %#v", pending.Intents)
	}
	running := transitionCompensation(t, pending.Effect, CompensationCommand{
		To: contracts.EffectCompensationRunning, CompensationCommandID: "compensate-1", CommandID: "start-compensation-1", At: t0.Add(5 * time.Second),
	})
	failure := &contracts.StructuredFailure{Class: contracts.FailureTransient, Code: "cleanup.busy", Message: "target busy"}
	retryAt := t0.Add(30 * time.Second)
	retry := transitionCompensation(t, running.Effect, CompensationCommand{
		To: contracts.EffectCompensationRetryWait, Failure: failure, RetryAt: &retryAt, CommandID: "retry-compensation", At: t0.Add(6 * time.Second),
	})
	second := transitionCompensation(t, retry.Effect, CompensationCommand{
		To: contracts.EffectCompensationRunning, CompensationCommandID: "compensate-2", CommandID: "start-compensation-2", At: t0.Add(31 * time.Second),
	})
	done := transitionCompensation(t, second.Effect, CompensationCommand{
		To: contracts.EffectCompensationSucceeded, CommandID: "finish-compensation", At: t0.Add(32 * time.Second),
	})
	if done.Effect.CompensationAttemptCount != 2 || done.Effect.CompensationState != contracts.EffectCompensationSucceeded || done.Effect.State != contracts.EffectApplied {
		t.Fatalf("compensated effect = %#v", done.Effect)
	}
}

func TestDetachedEffectCannotClaimCompensation(t *testing.T) {
	t0 := time.Date(2026, 8, 12, 10, 0, 0, 0, time.UTC)
	intent := prepareTestEffect(t, t0, contracts.CompensationNone).Intent
	intent.Ownership = contracts.EffectDetached
	if err := ValidateIntent(intent, t0); err != nil {
		t.Fatalf("detached non-compensating effect: %v", err)
	}
	intent.CompensationPolicy = contracts.CompensationRequired
	if err := ValidateIntent(intent, t0); err == nil {
		t.Fatal("detached effect claimed Run-owned compensation")
	}
}

func TestCancelPreparedEffectNeverInventsProviderOutcome(t *testing.T) {
	t0 := time.Date(2026, 8, 12, 10, 30, 0, 0, time.UTC)
	prepared := prepareTestEffect(t, t0, contracts.CompensationRequired)
	canceled, err := CancelPrepared(prepared, CancelPreparedCommand{
		EffectID: prepared.EffectID, ExpectedRevision: prepared.Revision,
		CommandID: "cancel-before-outbox", At: t0.Add(time.Second),
	})
	if err != nil {
		t.Fatal(err)
	}
	if canceled.Effect.State != contracts.EffectCanceled ||
		canceled.Effect.CompensationState != contracts.EffectCompensationCanceled ||
		canceled.Effect.CommandID != "" || canceled.Effect.ApplyingAt != nil ||
		canceled.Effect.PrimaryTerminalAt == nil || len(canceled.Intents) != 0 {
		t.Fatalf("canceled prepared effect = %#v", canceled.Effect)
	}
	envelope := testEnvelope(t, prepared.Intent, "late-begin", "late-key", "late-token", t0.Add(time.Minute))
	if _, err := Begin(canceled.Effect, BeginCommand{
		EffectID: canceled.Effect.EffectID, ExpectedRevision: canceled.Effect.Revision,
		Envelope: envelope, CommandID: envelope.CommandID, At: t0.Add(2 * time.Second),
	}); !errors.Is(err, ErrInvalidTransition) {
		t.Fatalf("late begin after cancellation error = %v", err)
	}
}

func prepareTestEffect(t *testing.T, at time.Time, policy contracts.CompensationPolicy) contracts.EffectRecord {
	t.Helper()
	effectID, err := StableEffectID("invocation-1", "start-simulator")
	if err != nil {
		t.Fatal(err)
	}
	intentPayload := map[string]any{"runtimeRef": "simulator-1"}
	intentDigest, err := canonicaljson.DigestValue(intentPayload)
	if err != nil {
		t.Fatal(err)
	}
	decision, err := Prepare(PrepareCommand{
		Intent: contracts.EffectIntent{
			EffectID: effectID, NamespaceID: "lab", RunID: "run-1", InvocationID: "invocation-1", PreparedAttemptID: "attempt-1",
			EffectKey: "start-simulator", Kind: "xgc.process-start/v1", TargetRef: "simulator-1",
			IntentSchemaDigest: digest, Intent: intentPayload, IntentDigest: intentDigest, Ownership: contracts.EffectOwned, CompensationPolicy: policy,
			RequiredCapabilityRefs: []string{"process.control"}, PolicyDigest: digest, DescriptorDigest: digest, Deadline: at.Add(10 * time.Minute),
		},
		CommandID: "prepare-1", At: at,
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(decision.Intents) != 0 {
		t.Fatalf("prepare emitted adapter intent: %#v", decision.Intents)
	}
	replayed, err := ReplayPrepare(decision.Effect, PrepareCommand{Intent: decision.Effect.Intent, CommandID: "prepare-replay", At: at})
	if err != nil || len(replayed.Events) != 0 || replayed.Effect.PreparationDigest != decision.Effect.PreparationDigest {
		t.Fatalf("prepare replay = %#v, err %v", replayed, err)
	}
	return decision.Effect
}

func testEnvelope(t *testing.T, intent contracts.EffectIntent, commandID, key, token string, deadline time.Time) contracts.CommandEnvelope {
	t.Helper()
	keyHash, err := execution.PrivateTokenDigest(key)
	if err != nil {
		t.Fatal(err)
	}
	tokenHash, err := execution.PrivateTokenDigest(token)
	if err != nil {
		t.Fatal(err)
	}
	envelope := contracts.CommandEnvelope{
		CommandID: commandID, EffectID: intent.EffectID, IdempotencyKey: key, IdempotencyKeyHash: keyHash,
		NamespaceID: intent.NamespaceID, TargetRef: intent.TargetRef, Action: "process.start", ActorRef: "controller",
		SourceRef: "orchestrator", ReasonCode: "workflow.effect", Risk: contracts.RiskHigh,
		Fence:         contracts.TargetFence{Kind: contracts.FenceGeneration, Generation: &contracts.GenerationFence{BindingID: "runtime-1", Generation: 3, FencingToken: 9}},
		PayloadDigest: digest, PolicyDigest: intent.PolicyDigest, DescriptorDigest: intent.DescriptorDigest,
		Deadline: deadline, CancellationID: "cancel-1", RequiredCapabilityRefs: append([]string(nil), intent.RequiredCapabilityRefs...),
		CapabilityToken: token, CapabilityTokenHash: tokenHash,
	}
	envelope.IdentityDigest, err = CommandIdentityDigest(envelope)
	if err != nil {
		t.Fatal(err)
	}
	return envelope
}

func receiptFor(t *testing.T, envelope contracts.CommandEnvelope, sequence uint32, status contracts.ReceiptStatus, at time.Time, fenceDigest string) contracts.CommandReceipt {
	t.Helper()
	id, err := StableReceiptID(envelope.CommandID, sequence)
	if err != nil {
		t.Fatal(err)
	}
	return contracts.CommandReceipt{
		ReceiptID: id, CommandID: envelope.CommandID, Sequence: sequence, Status: status,
		IdentityDigest: envelope.IdentityDigest, FenceDigest: fenceDigest, ProviderRef: "provider-1",
		ProviderDigest: digest, PolicyDigest: envelope.PolicyDigest, AuthorizationDigest: digest, ObservedAt: at,
	}
}

func applyTestEffect(t *testing.T, t0 time.Time) contracts.EffectRecord {
	t.Helper()
	record := prepareTestEffect(t, t0, contracts.CompensationRequired)
	envelope := testEnvelope(t, record.Intent, "command-applied", "key-applied", "cap-applied", t0.Add(time.Minute))
	begin, err := Begin(record, BeginCommand{EffectID: record.EffectID, ExpectedRevision: record.Revision, Envelope: envelope, CommandID: envelope.CommandID, At: t0.Add(time.Second)})
	if err != nil {
		t.Fatal(err)
	}
	fenceDigest, _ := FenceDigest(envelope.Fence)
	accepted := receiptFor(t, envelope, 1, contracts.ReceiptAccepted, t0.Add(2*time.Second), fenceDigest)
	acceptedDecision, err := Observe(begin.Effect, *begin.Ledger, ObserveCommand{EffectID: record.EffectID, ExpectedRevision: begin.Effect.Revision, Receipt: accepted, CommandID: "observe-applied-accepted"})
	if err != nil {
		t.Fatal(err)
	}
	receipt := receiptFor(t, envelope, 2, contracts.ReceiptSucceeded, t0.Add(3*time.Second), fenceDigest)
	receipt.ResultDigest = digest
	observed, err := Observe(acceptedDecision.Effect, *acceptedDecision.Ledger, ObserveCommand{EffectID: record.EffectID, ExpectedRevision: acceptedDecision.Effect.Revision, Receipt: receipt, CommandID: "observe-applied"})
	if err != nil {
		t.Fatal(err)
	}
	return observed.Effect
}

func transitionCompensation(t *testing.T, current contracts.EffectRecord, command CompensationCommand) Decision {
	t.Helper()
	command.EffectID = current.EffectID
	command.ExpectedRevision = current.Revision
	decision, err := TransitionCompensation(current, command)
	if err != nil {
		t.Fatal(err)
	}
	return decision
}
