package controller

import (
	"context"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/lxk36/xgc2-orchestration-core/durable/filestore"
	"github.com/lxk36/xgc2-orchestration-core/durable/store"
	"github.com/lxk36/xgc2-orchestration-core/durable/worker"
	"github.com/lxk36/xgc2-orchestration-core/kernel/canonicaljson"
	"github.com/lxk36/xgc2-orchestration-core/kernel/effect"
	"github.com/lxk36/xgc2-orchestration-core/kernel/execution"
	"github.com/lxk36/xgc2-orchestration-core/kernel/node"
	"github.com/lxk36/xgc2-orchestration-core/kernel/ownership"
	workflowkernel "github.com/lxk36/xgc2-orchestration-core/kernel/workflow"
	"github.com/lxk36/xgc2-orchestration-core/sdk/go/contracts"
	nodesdk "github.com/lxk36/xgc2-orchestration-core/sdk/go/node"
)

const testPackageDigest = "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"

type fakeClock struct {
	mu  sync.Mutex
	now time.Time
}

func (clock *fakeClock) Now() time.Time {
	clock.mu.Lock()
	defer clock.mu.Unlock()
	return clock.now
}

func (clock *fakeClock) Advance(duration time.Duration) {
	clock.mu.Lock()
	clock.now = clock.now.Add(duration)
	clock.mu.Unlock()
}

type countExecutor struct {
	descriptor contracts.NodeDescriptor
	mu         sync.Mutex
	calls      int
	function   func(map[string]any) map[string]any
}

func (executor *countExecutor) Descriptor() contracts.NodeDescriptor { return executor.descriptor }

func (executor *countExecutor) Execute(_ context.Context, request contracts.NodeInvocationRequest) (contracts.NodeResult, error) {
	executor.mu.Lock()
	executor.calls++
	executor.mu.Unlock()
	output := executor.function(request.Input)
	digest, err := canonicaljson.DigestValue(output)
	if err != nil {
		return contracts.NodeResult{}, err
	}
	return contracts.NodeResult{Status: contracts.NodeResultSucceeded, Output: output, OutputDigest: digest, EvidenceDigest: digest}, nil
}

func (executor *countExecutor) Calls() int {
	executor.mu.Lock()
	defer executor.mu.Unlock()
	return executor.calls
}

func TestControllerExecutesPinnedEntrypointAndRecoversDurableResult(t *testing.T) {
	fixture := newControllerFixture(t)
	invoked, err := fixture.controller.Invoke(t.Context(), fixture.request("run-success", "invoke-success"))
	if err != nil || invoked.Replay || invoked.Run.Status != contracts.RunAccepted {
		t.Fatalf("invoke = %#v, err = %v", invoked, err)
	}
	run, err := fixture.controller.Drive(t.Context(), invoked.Run.RunID)
	if err != nil || run.Status != contracts.RunSucceeded {
		t.Fatalf("run = %#v, err = %v", run, err)
	}
	snapshot, _, err := fixture.controller.GetSnapshot(t.Context(), run.RunID)
	if err != nil || snapshot.Result["text"] != "HELLO XGC" || !contracts.ValidDigest(snapshot.ResultDigest) {
		t.Fatalf("snapshot = %#v, err = %v", snapshot, err)
	}
	if fixture.prepare.Calls() != 1 || fixture.render.Calls() != 1 || fixture.diagnostics.Calls() != 0 {
		t.Fatalf("calls prepare=%d render=%d diagnostics=%d", fixture.prepare.Calls(), fixture.render.Calls(), fixture.diagnostics.Calls())
	}
	runs, err := fixture.controller.ListRuns(t.Context(), "", 10)
	if err != nil || len(runs) != 1 || runs[0].RunID != run.RunID {
		t.Fatalf("listed runs = %#v, err = %v", runs, err)
	}
	graph, err := fixture.controller.OwnershipGraph(t.Context(), run.RunID)
	if err != nil || graph.Revision != 1 || graph.Run.Revision != run.Revision ||
		graph.Run.Status != contracts.RunSucceeded || len(graph.Invocations) != 2 {
		t.Fatalf("ownership graph = %#v, terminal revision=%d err=%v", graph, run.Revision, err)
	}
	facts, err := ownership.ClosureFacts(graph)
	if err != nil || !facts.Satisfied() || facts.OwnershipGraphRevision != graph.Revision {
		t.Fatalf("closure facts = %#v, err=%v", facts, err)
	}
	path := fixture.path
	if err := fixture.store.Close(); err != nil {
		t.Fatal(err)
	}
	reopened, err := filestore.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = reopened.Close() })
	recovered, err := New(Config{
		Store: reopened, Nodes: fixture.registry, OwnerRef: "controller-recovered",
		LeaseDuration: time.Minute, InvocationTimeout: 10 * time.Second, Clock: fixture.clock,
	})
	if err != nil {
		t.Fatal(err)
	}
	run, err = recovered.Drive(t.Context(), run.RunID)
	if err != nil || run.Status != contracts.RunSucceeded || fixture.prepare.Calls() != 1 || fixture.render.Calls() != 1 {
		t.Fatalf("recovered run = %#v, err = %v, calls=%d/%d", run, err, fixture.prepare.Calls(), fixture.render.Calls())
	}
	if recoveredGraph, graphErr := recovered.OwnershipGraph(t.Context(), run.RunID); graphErr != nil || recoveredGraph.Revision != graph.Revision {
		t.Fatalf("recovered ownership graph = %#v, err=%v", recoveredGraph, graphErr)
	}
}

func TestControllerExpiresClaimAndReplaysOnlyPureDeterministicNode(t *testing.T) {
	fixture := newControllerFixture(t)
	invoked, err := fixture.controller.Invoke(t.Context(), fixture.request("run-recover", "invoke-recover"))
	if err != nil {
		t.Fatal(err)
	}
	injected := errors.New("simulated controller crash after durable claim")
	fixture.controller.afterClaim = func(contracts.InvocationLedger) error { return injected }
	_, err = fixture.controller.Drive(t.Context(), invoked.Run.RunID)
	if !errors.Is(err, injected) {
		t.Fatalf("drive interruption error = %v", err)
	}
	if fixture.prepare.Calls() != 0 {
		t.Fatalf("node executed before injected crash: %d", fixture.prepare.Calls())
	}
	fixture.clock.Advance(2 * time.Minute)
	recovered, err := New(Config{
		Store: fixture.store, Nodes: fixture.registry, OwnerRef: "controller-takeover",
		LeaseDuration: time.Minute, InvocationTimeout: 10 * time.Second, Clock: fixture.clock,
	})
	if err != nil {
		t.Fatal(err)
	}
	run, err := recovered.Drive(t.Context(), invoked.Run.RunID)
	if err != nil || run.Status != contracts.RunSucceeded {
		t.Fatalf("recovered run = %#v, err = %v", run, err)
	}
	if fixture.prepare.Calls() != 1 || fixture.render.Calls() != 1 {
		t.Fatalf("recovery calls prepare=%d render=%d", fixture.prepare.Calls(), fixture.render.Calls())
	}
}

func TestControllerPreparesEffectBeforeLeavingRunWaiting(t *testing.T) {
	fixture := newEffectControllerFixture(t, "run-effect")
	invoked, err := fixture.controller.Invoke(t.Context(), fixture.request)
	if err != nil {
		t.Fatal(err)
	}
	run, err := fixture.controller.Drive(t.Context(), invoked.Run.RunID)
	if err != nil || run.Status != contracts.RunWaiting {
		t.Fatalf("waiting run = %#v, err = %v", run, err)
	}
	invocationID, _ := execution.StableInvocationID(run.RunID, "launch")
	effectID, _ := effect.StableEffectID(invocationID, "start-process")
	record, err := fixture.store.GetAggregate(t.Context(), store.AggregateKey{Type: effectAggregateType, ID: effectID})
	if err != nil {
		t.Fatal(err)
	}
	var prepared contracts.EffectRecord
	if err := canonicaljson.UnmarshalStrict(record.Payload, &prepared); err != nil {
		t.Fatal(err)
	}
	if prepared.State != contracts.EffectPrepared || prepared.Intent.Intent["executableRef"] != "simulator" ||
		prepared.Intent.PreparedAttemptID == "" || prepared.CommandID != "" {
		t.Fatalf("prepared effect = %#v", prepared)
	}
	if _, err := fixture.controller.Drive(t.Context(), run.RunID); !errors.Is(err, ErrRunWaiting) {
		t.Fatalf("second drive error = %v", err)
	}
}

func TestControllerDurablyDispatchesAndObservesEffectReceipts(t *testing.T) {
	fixture := newEffectControllerFixture(t, "run-effect-dispatch")
	invoked, err := fixture.controller.Invoke(t.Context(), fixture.request)
	if err != nil {
		t.Fatal(err)
	}
	run, err := fixture.controller.Drive(t.Context(), invoked.Run.RunID)
	if err != nil || run.Status != contracts.RunWaiting {
		t.Fatalf("waiting run = %#v, err = %v", run, err)
	}
	invocationID, _ := execution.StableInvocationID(run.RunID, "launch")
	effectID, _ := effect.StableEffectID(invocationID, "start-process")
	beginRequest := BeginEffectRequest{
		EffectID: effectID, CommandID: "dispatch-process", RequestID: "request-process",
		IdempotencyKey: "private-idempotency-key", Action: "process.start",
		ActorRef: "operator", SourceRef: "test-suite", ReasonCode: "experiment.launch",
		Risk: contracts.RiskModerate,
		Fence: contracts.TargetFence{Kind: contracts.FenceGeneration, Generation: &contracts.GenerationFence{
			BindingID: "simulator", Generation: 1, FencingToken: 7,
		}},
		Deadline: fixture.clock.Now().Add(5 * time.Second), CancellationID: "cancel-process",
		CapabilityToken: "private-capability-token",
	}
	begun, err := fixture.controller.BeginEffect(t.Context(), beginRequest)
	if err != nil {
		t.Fatal(err)
	}
	if begun.Replay || begun.Effect.State != contracts.EffectApplying || begun.IntentID == "" ||
		begun.Ledger.Envelope.IdempotencyKey != "" || begun.Ledger.Envelope.CapabilityToken != "" {
		t.Fatalf("begun effect = %#v", begun)
	}
	intent, err := fixture.store.GetIntent(t.Context(), begun.IntentID)
	if err != nil || intent.Status != store.IntentPending || intent.Intent.Kind != contracts.IntentOutbox {
		t.Fatalf("outbox intent = %#v, err = %v", intent, err)
	}
	replayed, err := fixture.controller.BeginEffect(t.Context(), beginRequest)
	if err != nil || !replayed.Replay || replayed.Effect.State != contracts.EffectApplying {
		t.Fatalf("begin replay = %#v, err = %v", replayed, err)
	}
	fenceDigest, _ := effect.FenceDigest(begun.Ledger.Envelope.Fence)
	acceptedAt := fixture.clock.Now().Add(time.Second)
	acceptedID, _ := effect.StableReceiptID(beginRequest.CommandID, 1)
	accepted := contracts.CommandReceipt{
		ReceiptID: acceptedID, CommandID: beginRequest.CommandID, Sequence: 1,
		Status: contracts.ReceiptAccepted, IdentityDigest: begun.Ledger.Envelope.IdentityDigest,
		FenceDigest: fenceDigest, ProviderRef: "provider-local", ProviderDigest: testPackageDigest,
		PolicyDigest: begun.Ledger.Envelope.PolicyDigest, AuthorizationDigest: testPackageDigest,
		ExternalIdentity: "pid-123", ObservedAt: acceptedAt,
	}
	observed, err := fixture.controller.ObserveEffect(t.Context(), ObserveEffectRequest{
		EffectID: effectID, Receipt: accepted, CommandID: "observe-process-accepted",
	})
	if err != nil || observed.Effect.State != contracts.EffectApplying || len(observed.Ledger.Receipts) != 1 {
		t.Fatalf("accepted observation = %#v, err = %v", observed, err)
	}
	resultDigest, _ := canonicaljson.DigestValue(map[string]any{"pid": 123})
	succeededID, _ := effect.StableReceiptID(beginRequest.CommandID, 2)
	succeeded := accepted
	succeeded.ReceiptID = succeededID
	succeeded.Sequence = 2
	succeeded.Status = contracts.ReceiptSucceeded
	succeeded.ResultDigest = resultDigest
	succeeded.ResultArtifactRef = "artifact-process-result"
	succeeded.ObservedAt = acceptedAt.Add(time.Second)
	observed, err = fixture.controller.ObserveEffect(t.Context(), ObserveEffectRequest{
		EffectID: effectID, Receipt: succeeded, CommandID: "observe-process-succeeded",
	})
	if err != nil || observed.Effect.State != contracts.EffectApplied || len(observed.Ledger.Receipts) != 2 ||
		observed.Effect.ResultDigest != resultDigest {
		t.Fatalf("succeeded observation = %#v, err = %v", observed, err)
	}
	resolutionIntentID, _ := execution.StableIntentID(contracts.IntentWaitResolution, effectID, observed.Effect.Revision)
	resolutionIntent, err := fixture.store.GetIntent(t.Context(), resolutionIntentID)
	if err != nil || resolutionIntent.Status != store.IntentPending {
		t.Fatalf("wait resolution intent = %#v, err = %v", resolutionIntent, err)
	}
	replayedObservation, err := fixture.controller.ObserveEffect(t.Context(), ObserveEffectRequest{
		EffectID: effectID, Receipt: succeeded, CommandID: "observe-process-succeeded",
	})
	if err != nil || !replayedObservation.Replay {
		t.Fatalf("observation replay = %#v, err = %v", replayedObservation, err)
	}
	effects, err := fixture.controller.ListEffects(t.Context(), "", 10)
	if err != nil || len(effects) != 1 || effects[0].State != contracts.EffectApplied {
		t.Fatalf("listed effects = %#v, err = %v", effects, err)
	}
	resolvedRun, err := fixture.controller.ResolveEffectWait(t.Context(), effectID)
	if err != nil || resolvedRun.Status != contracts.RunSucceeded {
		t.Fatalf("resolved run = %#v, err = %v", resolvedRun, err)
	}
}

type staticEffectCredentials struct {
	idempotency string
	capability  string
}

type plannedEffectCredentials struct{}

func (plannedEffectCredentials) ResolveEffectCredentials(_ context.Context, current contracts.EffectRecord, ledger contracts.CommandLedger) (DispatchCredentials, error) {
	if strings.HasPrefix(ledger.Envelope.CommandID, "compensate-") {
		return DispatchCredentials{
			IdempotencyKey:      "compensate-idempotency-" + current.EffectID,
			CapabilityToken:     "compensate-capability-" + current.EffectID,
			AuthorizationDigest: testPackageDigest,
		}, nil
	}
	return DispatchCredentials{
		IdempotencyKey: "coordinate-idempotency", CapabilityToken: "coordinate-capability",
		AuthorizationDigest: testPackageDigest,
	}, nil
}

func (credentials staticEffectCredentials) ResolveEffectCredentials(context.Context, contracts.EffectRecord, contracts.CommandLedger) (DispatchCredentials, error) {
	return DispatchCredentials{
		IdempotencyKey: credentials.idempotency, CapabilityToken: credentials.capability,
		AuthorizationDigest: testPackageDigest,
	}, nil
}

type successfulEffectAdapter struct {
	clock       *fakeClock
	seenPrivate bool
}

func (adapter *successfulEffectAdapter) Descriptor() EffectAdapterDescriptor {
	return EffectAdapterDescriptor{Kind: "xgc.process-start/v1", ProviderRef: "provider-test", ProviderDigest: testPackageDigest}
}

func (adapter *successfulEffectAdapter) Dispatch(_ context.Context, _ contracts.EffectIntent, envelope contracts.CommandEnvelope, authorizationDigest string) (contracts.CommandLedger, error) {
	adapter.seenPrivate = envelope.IdempotencyKey != "" && envelope.CapabilityToken != ""
	descriptor := adapter.Descriptor()
	accepted, err := syntheticReceipt(envelope, descriptor, authorizationDigest, 1, contracts.ReceiptAccepted, nil, adapter.clock.Now())
	if err != nil {
		return contracts.CommandLedger{}, err
	}
	succeeded, err := syntheticReceipt(envelope, descriptor, authorizationDigest, 2, contracts.ReceiptSucceeded, nil, adapter.clock.Now())
	if err != nil {
		return contracts.CommandLedger{}, err
	}
	succeeded.ResultDigest, _ = canonicaljson.DigestValue(map[string]any{"pid": 321})
	succeeded.ExternalIdentity = "pid-321"
	envelope.IdempotencyKey, envelope.CapabilityToken = "", ""
	return contracts.CommandLedger{Envelope: envelope, Receipts: []contracts.CommandReceipt{accepted, succeeded}}, nil
}

type uncertainEffectAdapter struct{}

func (uncertainEffectAdapter) Descriptor() EffectAdapterDescriptor {
	return EffectAdapterDescriptor{Kind: "xgc.process-start/v1", ProviderRef: "provider-test", ProviderDigest: testPackageDigest}
}

func (uncertainEffectAdapter) Dispatch(context.Context, contracts.EffectIntent, contracts.CommandEnvelope, string) (contracts.CommandLedger, error) {
	return contracts.CommandLedger{}, errors.New("provider outcome is unknown")
}

func TestDurableWorkersDispatchOutboxThenResumeRun(t *testing.T) {
	fixture := newEffectControllerFixture(t, "run-effect-workers")
	invoked, err := fixture.controller.Invoke(t.Context(), fixture.request)
	if err != nil {
		t.Fatal(err)
	}
	run, err := fixture.controller.Drive(t.Context(), invoked.Run.RunID)
	if err != nil || run.Status != contracts.RunWaiting {
		t.Fatalf("waiting run = %#v, err = %v", run, err)
	}
	invocationID, _ := execution.StableInvocationID(run.RunID, "launch")
	effectID, _ := effect.StableEffectID(invocationID, "start-process")
	begin := BeginEffectRequest{
		EffectID: effectID, CommandID: "dispatch-worker-process", IdempotencyKey: "worker-idempotency-key",
		Action: "process.start", ActorRef: "operator", SourceRef: "worker-test", ReasonCode: "experiment.launch",
		Risk: contracts.RiskModerate,
		Fence: contracts.TargetFence{Kind: contracts.FenceGeneration, Generation: &contracts.GenerationFence{
			BindingID: "simulator", Generation: 1, FencingToken: 11,
		}},
		Deadline: fixture.clock.Now().Add(5 * time.Second), CancellationID: "cancel-worker-process",
		CapabilityToken: "worker-capability-token",
	}
	if _, err := fixture.controller.BeginEffect(t.Context(), begin); err != nil {
		t.Fatal(err)
	}
	adapter := &successfulEffectAdapter{clock: fixture.clock}
	outbox, err := NewEffectOutboxHandler(fixture.controller, staticEffectCredentials{
		idempotency: begin.IdempotencyKey, capability: begin.CapabilityToken,
	}, adapter)
	if err != nil {
		t.Fatal(err)
	}
	waitHandler, err := NewWaitResolutionHandler(fixture.controller)
	if err != nil {
		t.Fatal(err)
	}
	durableWorker := worker.Worker{
		Store: fixture.store, OwnerRef: "effect-worker",
		Handlers: map[contracts.DurableIntentKind]worker.Handler{
			contracts.IntentOutbox: outbox, contracts.IntentWaitResolution: waitHandler,
		},
	}
	now := fixture.clock.Now()
	outboxBatch, err := durableWorker.RunOnce(t.Context(), worker.Batch{
		Kinds: []contracts.DurableIntentKind{contracts.IntentOutbox}, LeaseToken: "outbox-worker-lease",
		Now: now, LeaseExpiresAt: now.Add(time.Minute), Limit: 10,
	})
	if err != nil || outboxBatch.Completed != 1 || !adapter.seenPrivate {
		t.Fatalf("outbox batch = %#v, private=%v, err=%v", outboxBatch, adapter.seenPrivate, err)
	}
	waitBatch, err := durableWorker.RunOnce(t.Context(), worker.Batch{
		Kinds: []contracts.DurableIntentKind{contracts.IntentWaitResolution}, LeaseToken: "wait-worker-lease",
		Now: now, LeaseExpiresAt: now.Add(time.Minute), Limit: 10,
	})
	if err != nil || waitBatch.Completed != 1 {
		t.Fatalf("wait batch = %#v, err=%v", waitBatch, err)
	}
	run, err = fixture.controller.GetRun(t.Context(), run.RunID)
	if err != nil || run.Status != contracts.RunSucceeded {
		t.Fatalf("worker-resolved run = %#v, err=%v", run, err)
	}
}

func TestWaitIntentCompletesAfterUncertainEffectMovesRunToStopping(t *testing.T) {
	fixture := newEffectControllerFixture(t, "run-effect-uncertain")
	invoked, err := fixture.controller.Invoke(t.Context(), fixture.request)
	if err != nil {
		t.Fatal(err)
	}
	run, err := fixture.controller.Drive(t.Context(), invoked.Run.RunID)
	if err != nil || run.Status != contracts.RunWaiting {
		t.Fatalf("waiting run = %#v, err=%v", run, err)
	}
	invocationID, _ := execution.StableInvocationID(run.RunID, "launch")
	effectID, _ := effect.StableEffectID(invocationID, "start-process")
	begin := BeginEffectRequest{
		EffectID: effectID, CommandID: "dispatch-uncertain-process",
		IdempotencyKey: "uncertain-idempotency", CapabilityToken: "uncertain-capability",
		Action: "process.start", ActorRef: "operator", SourceRef: "worker-test",
		ReasonCode: "experiment.launch", Risk: contracts.RiskModerate,
		Fence: contracts.TargetFence{Kind: contracts.FenceGeneration, Generation: &contracts.GenerationFence{
			BindingID: "simulator", Generation: 1, FencingToken: 13,
		}},
		Deadline: fixture.clock.Now().Add(5 * time.Second), CancellationID: "cancel-uncertain-process",
	}
	if _, err := fixture.controller.BeginEffect(t.Context(), begin); err != nil {
		t.Fatal(err)
	}
	outbox, err := NewEffectOutboxHandler(fixture.controller, staticEffectCredentials{
		idempotency: begin.IdempotencyKey, capability: begin.CapabilityToken,
	}, uncertainEffectAdapter{})
	if err != nil {
		t.Fatal(err)
	}
	waits, err := NewWaitResolutionHandler(fixture.controller)
	if err != nil {
		t.Fatal(err)
	}
	durableWorker := worker.Worker{
		Store: fixture.store, OwnerRef: "uncertain-worker",
		Handlers: map[contracts.DurableIntentKind]worker.Handler{
			contracts.IntentOutbox: outbox, contracts.IntentWaitResolution: waits,
		},
	}
	now := fixture.clock.Now()
	outboxBatch, err := durableWorker.RunOnce(t.Context(), worker.Batch{
		Kinds: []contracts.DurableIntentKind{contracts.IntentOutbox}, LeaseToken: "uncertain-outbox-lease",
		Now: now, LeaseExpiresAt: now.Add(time.Minute), Limit: 10,
	})
	if err != nil || outboxBatch.Completed != 1 {
		t.Fatalf("uncertain outbox = %#v, err=%v", outboxBatch, err)
	}
	waitBatch, err := durableWorker.RunOnce(t.Context(), worker.Batch{
		Kinds: []contracts.DurableIntentKind{contracts.IntentWaitResolution}, LeaseToken: "uncertain-wait-lease",
		Now: now, LeaseExpiresAt: now.Add(time.Minute), Limit: 10,
	})
	if err != nil || waitBatch.Completed != 1 || waitBatch.Retried != 0 {
		t.Fatalf("uncertain wait = %#v, err=%v", waitBatch, err)
	}
	run, err = fixture.controller.GetRun(t.Context(), run.RunID)
	if err != nil || run.Status != contracts.RunStopping || run.PrimaryFailure != nil {
		t.Fatalf("uncertain run = %#v, err=%v", run, err)
	}
	currentEffect, err := fixture.controller.GetEffect(t.Context(), effectID)
	if err != nil || currentEffect.State != contracts.EffectUncertain {
		t.Fatalf("uncertain effect = %#v, err=%v", currentEffect, err)
	}
	if _, err := fixture.controller.Drive(t.Context(), run.RunID); !errors.Is(err, ErrRunClosureOpen) {
		t.Fatalf("uncertain ownership closure error = %v", err)
	}
}

type staticEffectPlan struct {
	clock *fakeClock
}

type staticCompensationPlan struct{ staticEffectPlan }

func (plan staticCompensationPlan) PlanEffectCompensation(_ context.Context, current contracts.EffectRecord) (BeginEffectRequest, error) {
	return BeginEffectRequest{
		EffectID: current.EffectID, CommandID: "compensate-" + current.EffectID,
		IdempotencyKey:  "compensate-idempotency-" + current.EffectID,
		CapabilityToken: "compensate-capability-" + current.EffectID,
		Action:          "process.stop", ActorRef: "operator", SourceRef: "coordinator-test",
		ReasonCode: "workflow.compensation", Risk: contracts.RiskHigh,
		Fence: contracts.TargetFence{Kind: contracts.FenceIdempotentCreate, IdempotentCreate: &contracts.IdempotentCreateFence{
			TargetNamespace: "test-compensation", IdentityDigest: current.Intent.IntentDigest,
		}},
		Deadline: plan.clock.Now().Add(5 * time.Second), CancellationID: "cancel-compensate-" + current.EffectID,
	}, nil
}

type compensatingEffectAdapter struct {
	*successfulEffectAdapter
	mu    sync.Mutex
	order []string
}

func (adapter *compensatingEffectAdapter) Compensate(
	_ context.Context,
	applied contracts.EffectRecord,
	envelope contracts.CommandEnvelope,
	authorizationDigest string,
) (contracts.CommandLedger, error) {
	if applied.State != contracts.EffectApplied || applied.ExternalIdentity == "" {
		return contracts.CommandLedger{}, errors.New("compensation lacks an applied external identity")
	}
	adapter.mu.Lock()
	adapter.order = append(adapter.order, applied.Intent.InvocationID)
	adapter.mu.Unlock()
	descriptor := adapter.Descriptor()
	accepted, err := syntheticReceipt(envelope, descriptor, authorizationDigest, 1, contracts.ReceiptAccepted, nil, adapter.clock.Now())
	if err != nil {
		return contracts.CommandLedger{}, err
	}
	succeeded, err := syntheticReceipt(envelope, descriptor, authorizationDigest, 2, contracts.ReceiptSucceeded, nil, adapter.clock.Now())
	if err != nil {
		return contracts.CommandLedger{}, err
	}
	succeeded.ResultDigest, _ = canonicaljson.DigestValue(map[string]any{"stopped": applied.ExternalIdentity})
	succeeded.ExternalIdentity = applied.ExternalIdentity
	envelope.IdempotencyKey, envelope.CapabilityToken = "", ""
	return contracts.CommandLedger{Envelope: envelope, Receipts: []contracts.CommandReceipt{accepted, succeeded}}, nil
}

func (adapter *compensatingEffectAdapter) Order() []string {
	adapter.mu.Lock()
	defer adapter.mu.Unlock()
	return append([]string(nil), adapter.order...)
}

func TestCoordinatorCompensatesOwnedEffectsInReverseWorkflowOrder(t *testing.T) {
	fixture := newEffectControllerFixture(t, "run-effect-compensation")
	fixture.executor.owned = true
	second := fixture.request.Definition.Nodes[0]
	second.NodeID = "launch-again"
	fixture.request.Definition.Nodes = append(fixture.request.Definition.Nodes, second)
	fixture.request.Definition.Edges = []contracts.WorkflowEdge{{From: "launch", To: "launch-again", Kind: contracts.EdgeControl}}
	plan, err := workflowkernel.Compile(fixture.request.Definition)
	if err != nil {
		t.Fatal(err)
	}
	fixture.request.Action.DefinitionDigest = plan.DefinitionDigest
	invoked, err := fixture.controller.Invoke(t.Context(), fixture.request)
	if err != nil {
		t.Fatal(err)
	}
	adapter := &compensatingEffectAdapter{successfulEffectAdapter: &successfulEffectAdapter{clock: fixture.clock}}
	coordinator, err := NewCoordinator(CoordinatorConfig{
		Controller: fixture.controller, Store: fixture.store,
		Planner:     staticCompensationPlan{staticEffectPlan{clock: fixture.clock}},
		Credentials: plannedEffectCredentials{},
		Adapters:    []EffectAdapter{adapter}, OwnerRef: "compensation-coordinator", Clock: fixture.clock,
	})
	if err != nil {
		t.Fatal(err)
	}
	run, err := coordinator.AdvanceRun(t.Context(), invoked.Run.RunID)
	if err != nil || run.Status != contracts.RunSucceeded {
		t.Fatalf("compensated run = %#v, err=%v", run, err)
	}
	first, _ := execution.StableInvocationID(run.RunID, "launch")
	secondInvocation, _ := execution.StableInvocationID(run.RunID, "launch-again")
	if order := adapter.Order(); len(order) != 2 || order[0] != secondInvocation || order[1] != first {
		t.Fatalf("compensation order = %#v, want [%s %s]", order, secondInvocation, first)
	}
	effects, err := fixture.controller.ListEffects(t.Context(), "", 10)
	if err != nil || len(effects) != 2 {
		t.Fatalf("effects = %#v, err=%v", effects, err)
	}
	for _, current := range effects {
		if current.CompensationState != contracts.EffectCompensationSucceeded || current.CompensationCommandID == "" {
			t.Fatalf("effect compensation = %#v", current)
		}
	}
}

func TestExactTerminationCancelsPreparedEffectAndReplaysFrozenCommand(t *testing.T) {
	fixture := newEffectControllerFixture(t, "run-stop-prepared")
	fixture.executor.owned = true
	invoked, err := fixture.controller.Invoke(t.Context(), fixture.request)
	if err != nil {
		t.Fatal(err)
	}
	waiting, err := fixture.controller.Drive(t.Context(), invoked.Run.RunID)
	if err != nil || waiting.Status != contracts.RunWaiting {
		t.Fatalf("waiting Run = %#v, err=%v", waiting, err)
	}
	request := TerminateRunRequest{
		RunID: waiting.RunID, ExpectedRevision: waiting.Revision, Kind: contracts.TerminationStopped,
		RequestedBy: "operator", ReasonCode: "operator.stop", Reason: "stop before provider dispatch",
		CommandID: "stop-run-prepared",
	}
	terminated, err := fixture.controller.RequestRunTermination(t.Context(), request)
	if err != nil || terminated.Replay || terminated.Run.Status != contracts.RunStopping {
		t.Fatalf("termination = %#v, err=%v", terminated, err)
	}
	coordinator, err := NewCoordinator(CoordinatorConfig{
		Controller: fixture.controller, Store: fixture.store, OwnerRef: "stop-prepared-coordinator", Clock: fixture.clock,
	})
	if err != nil {
		t.Fatal(err)
	}
	stopped, err := coordinator.AdvanceRun(t.Context(), waiting.RunID)
	if err != nil || stopped.Status != contracts.RunStopped || stopped.TerminationKind != contracts.TerminationStopped {
		t.Fatalf("stopped Run = %#v, err=%v", stopped, err)
	}
	invocationID, _ := execution.StableInvocationID(waiting.RunID, "launch")
	effectID, _ := effect.StableEffectID(invocationID, "start-process")
	canceledEffect, err := fixture.controller.GetEffect(t.Context(), effectID)
	if err != nil || canceledEffect.State != contracts.EffectCanceled ||
		canceledEffect.CompensationState != contracts.EffectCompensationCanceled {
		t.Fatalf("canceled effect = %#v, err=%v", canceledEffect, err)
	}
	replay, err := fixture.controller.RequestRunTermination(t.Context(), request)
	if err != nil || !replay.Replay || replay.Run.Status != contracts.RunStopped {
		t.Fatalf("termination replay = %#v, err=%v", replay, err)
	}
	conflict := request
	conflict.Reason = "changed bytes under the same command"
	if _, err := fixture.controller.RequestRunTermination(t.Context(), conflict); !errors.Is(err, store.ErrIdentityConflict) {
		t.Fatalf("changed termination command error = %v", err)
	}
	graph, err := fixture.controller.OwnershipGraph(t.Context(), waiting.RunID)
	if err != nil {
		t.Fatal(err)
	}
	facts, err := ownership.ClosureFacts(graph)
	if err != nil || !facts.Satisfied() {
		t.Fatalf("termination closure = %#v, err=%v", facts, err)
	}
}

func TestConcurrentDuplicateTerminationHasOneDurableDecision(t *testing.T) {
	fixture := newControllerFixture(t)
	invoked, err := fixture.controller.Invoke(t.Context(), fixture.request("run-stop-concurrent", "invoke-stop-concurrent"))
	if err != nil {
		t.Fatal(err)
	}
	request := TerminateRunRequest{
		RunID: invoked.Run.RunID, ExpectedRevision: invoked.Run.Revision, Kind: contracts.TerminationStopped,
		RequestedBy: "operator", ReasonCode: "operator.stop", CommandID: "stop-run-concurrent",
	}
	start := make(chan struct{})
	results := make(chan TerminateRunResult, 2)
	errorsSeen := make(chan error, 2)
	for index := 0; index < 2; index++ {
		go func() {
			<-start
			result, terminateErr := fixture.controller.RequestRunTermination(context.Background(), request)
			results <- result
			errorsSeen <- terminateErr
		}()
	}
	close(start)
	nonReplay, replay := 0, 0
	for index := 0; index < 2; index++ {
		if terminateErr := <-errorsSeen; terminateErr != nil {
			t.Fatal(terminateErr)
		}
		result := <-results
		if result.Replay {
			replay++
		} else {
			nonReplay++
		}
	}
	if nonReplay != 1 || replay != 1 {
		t.Fatalf("termination decisions: new=%d replay=%d", nonReplay, replay)
	}
	coordinator, err := NewCoordinator(CoordinatorConfig{
		Controller: fixture.controller, Store: fixture.store,
		OwnerRef: "stop-concurrent-coordinator", Clock: fixture.clock,
	})
	if err != nil {
		t.Fatal(err)
	}
	stopped, err := coordinator.AdvanceRun(t.Context(), invoked.Run.RunID)
	if err != nil || stopped.Status != contracts.RunStopped {
		t.Fatalf("concurrently stopped Run = %#v, err=%v", stopped, err)
	}
}

func TestTerminationResumesAppliedEffectCompensationAfterControllerRestart(t *testing.T) {
	fixture := newEffectControllerFixture(t, "run-stop-restart")
	fixture.executor.owned = true
	invoked, err := fixture.controller.Invoke(t.Context(), fixture.request)
	if err != nil {
		t.Fatal(err)
	}
	waiting, err := fixture.controller.Drive(t.Context(), invoked.Run.RunID)
	if err != nil || waiting.Status != contracts.RunWaiting {
		t.Fatalf("waiting Run = %#v, err=%v", waiting, err)
	}
	adapter := &compensatingEffectAdapter{successfulEffectAdapter: &successfulEffectAdapter{clock: fixture.clock}}
	planner := staticCompensationPlan{staticEffectPlan{clock: fixture.clock}}
	coordinator, err := NewCoordinator(CoordinatorConfig{
		Controller: fixture.controller, Store: fixture.store, Planner: planner,
		Credentials: plannedEffectCredentials{},
		Adapters:    []EffectAdapter{adapter}, OwnerRef: "stop-restart-before-crash", Clock: fixture.clock,
	})
	if err != nil {
		t.Fatal(err)
	}
	invocationID, _ := execution.StableInvocationID(waiting.RunID, "launch")
	effectID, _ := effect.StableEffectID(invocationID, "start-process")
	prepared, err := fixture.controller.GetEffect(t.Context(), effectID)
	if err != nil {
		t.Fatal(err)
	}
	begin, err := planner.PlanEffectDispatch(t.Context(), prepared)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := fixture.controller.BeginEffect(t.Context(), begin); err != nil {
		t.Fatal(err)
	}
	if batch, err := coordinator.runBatch(t.Context(), contracts.IntentOutbox, "apply-before-stop"); err != nil || batch.Completed != 1 {
		t.Fatalf("outbox batch = %#v, err=%v", batch, err)
	}
	applied, err := fixture.controller.GetEffect(t.Context(), effectID)
	if err != nil || applied.State != contracts.EffectApplied || applied.ExternalIdentity == "" {
		t.Fatalf("applied effect = %#v, err=%v", applied, err)
	}
	current, err := fixture.controller.GetRun(t.Context(), waiting.RunID)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := fixture.controller.RequestRunTermination(t.Context(), TerminateRunRequest{
		RunID: current.RunID, ExpectedRevision: current.Revision, Kind: contracts.TerminationStopped,
		RequestedBy: "operator", ReasonCode: "operator.stop", CommandID: "stop-run-before-controller-crash",
	}); err != nil {
		t.Fatal(err)
	}
	stopping, err := fixture.controller.Drive(t.Context(), current.RunID)
	if !errors.Is(err, ErrRunClosureOpen) || stopping.Status != contracts.RunStopping {
		t.Fatalf("pre-crash stopping Run = %#v, err=%v", stopping, err)
	}
	pending, err := fixture.controller.GetEffect(t.Context(), effectID)
	if err != nil || pending.CompensationState != contracts.EffectCompensationPending {
		t.Fatalf("persisted compensation = %#v, err=%v", pending, err)
	}
	if err := fixture.store.Close(); err != nil {
		t.Fatal(err)
	}
	reopened, err := filestore.Open(fixture.path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = reopened.Close() })
	recoveredController, err := New(Config{
		Store: reopened, Nodes: fixture.registry, OwnerRef: "controller-after-restart",
		LeaseDuration: time.Minute, InvocationTimeout: 10 * time.Second, Clock: fixture.clock, Grants: fixture.grants,
	})
	if err != nil {
		t.Fatal(err)
	}
	recoveredCoordinator, err := NewCoordinator(CoordinatorConfig{
		Controller: recoveredController, Store: reopened, Planner: planner,
		Credentials: plannedEffectCredentials{},
		Adapters:    []EffectAdapter{adapter}, OwnerRef: "stop-restart-after-crash", Clock: fixture.clock,
	})
	if err != nil {
		t.Fatal(err)
	}
	stopped, err := recoveredCoordinator.AdvanceRun(t.Context(), current.RunID)
	if err != nil || stopped.Status != contracts.RunStopped {
		t.Fatalf("recovered stopped Run = %#v, err=%v", stopped, err)
	}
	compensated, err := recoveredController.GetEffect(t.Context(), effectID)
	if err != nil || compensated.CompensationState != contracts.EffectCompensationSucceeded {
		t.Fatalf("recovered compensation = %#v, err=%v", compensated, err)
	}
	if order := adapter.Order(); len(order) != 1 || order[0] != invocationID {
		t.Fatalf("recovered compensation order = %#v", order)
	}
	runCleanupID, _ := execution.StableIntentID(contracts.IntentCleanup, current.RunID, stopping.Revision)
	waitResolutionID, _ := execution.StableIntentID(contracts.IntentWaitResolution, effectID, applied.PrimaryTerminalRevision)
	for _, identity := range []string{runCleanupID, waitResolutionID} {
		intent, intentErr := reopened.GetIntent(t.Context(), identity)
		if intentErr != nil || intent.Status != store.IntentCompleted {
			t.Fatalf("termination intent %s = %#v, err=%v", identity, intent, intentErr)
		}
	}
	late, err := recoveredController.ResolveEffectWait(t.Context(), effectID)
	if err != nil || late.Status != contracts.RunStopped {
		t.Fatalf("late effect result after Stop = %#v, err=%v", late, err)
	}
}

func (plan staticEffectPlan) PlanEffectDispatch(_ context.Context, current contracts.EffectRecord) (BeginEffectRequest, error) {
	return BeginEffectRequest{
		EffectID: current.EffectID, CommandID: "coordinate-" + current.EffectID,
		IdempotencyKey: "coordinate-idempotency", CapabilityToken: "coordinate-capability",
		Action: "process.start", ActorRef: "operator", SourceRef: "coordinator-test",
		ReasonCode: "experiment.launch", Risk: contracts.RiskModerate,
		Fence: contracts.TargetFence{Kind: contracts.FenceGeneration, Generation: &contracts.GenerationFence{
			BindingID: current.Intent.TargetRef, Generation: 1, FencingToken: 12,
		}},
		Deadline: plan.clock.Now().Add(5 * time.Second), CancellationID: "cancel-coordinate-effect",
	}, nil
}

func TestCoordinatorOwnsCompleteEffectWaitLoop(t *testing.T) {
	fixture := newEffectControllerFixture(t, "run-effect-coordinator")
	second := fixture.request.Definition.Nodes[0]
	second.NodeID = "launch-again"
	fixture.request.Definition.Nodes = append(fixture.request.Definition.Nodes, second)
	fixture.request.Definition.Edges = []contracts.WorkflowEdge{{From: "launch", To: "launch-again", Kind: contracts.EdgeControl}}
	plan, err := workflowkernel.Compile(fixture.request.Definition)
	if err != nil {
		t.Fatal(err)
	}
	fixture.request.Action.DefinitionDigest = plan.DefinitionDigest
	invoked, err := fixture.controller.Invoke(t.Context(), fixture.request)
	if err != nil {
		t.Fatal(err)
	}
	adapter := &successfulEffectAdapter{clock: fixture.clock}
	coordinator, err := NewCoordinator(CoordinatorConfig{
		Controller: fixture.controller, Store: fixture.store,
		Planner: staticEffectPlan{clock: fixture.clock}, Credentials: staticEffectCredentials{
			idempotency: "coordinate-idempotency", capability: "coordinate-capability",
		},
		Adapters: []EffectAdapter{adapter}, OwnerRef: "coordinator-test", Clock: fixture.clock,
	})
	if err != nil {
		t.Fatal(err)
	}
	run, err := coordinator.AdvanceRun(t.Context(), invoked.Run.RunID)
	if err != nil || run.Status != contracts.RunSucceeded || !adapter.seenPrivate || fixture.executor.Calls() != 2 {
		t.Fatalf("coordinated run = %#v, private=%v calls=%d err=%v", run, adapter.seenPrivate, fixture.executor.Calls(), err)
	}
	firstInvocationID, _ := execution.StableInvocationID(run.RunID, "launch")
	firstEffectID, _ := effect.StableEffectID(firstInvocationID, "start-process")
	firstEffect, err := fixture.controller.GetEffect(t.Context(), firstEffectID)
	if err != nil {
		t.Fatal(err)
	}
	firstWaitIntentID, _ := execution.StableIntentID(contracts.IntentWaitResolution, firstEffectID, firstEffect.Revision)
	firstWaitIntent, err := fixture.store.GetIntent(t.Context(), firstWaitIntentID)
	if err != nil || firstWaitIntent.Status != store.IntentCompleted {
		t.Fatalf("first wait intent = %#v, err=%v", firstWaitIntent, err)
	}
}

func TestCompileReturnsCanonicalPlanWithoutCreatingRun(t *testing.T) {
	fixture := newControllerFixture(t)
	request := fixture.request("compile-must-not-create", "compile-command")
	plan, err := fixture.controller.Compile(request.Definition)
	if err != nil || plan.DefinitionDigest != request.Action.DefinitionDigest || !contracts.ValidDigest(plan.PlanDigest) {
		t.Fatalf("compiled plan = %#v, err=%v", plan, err)
	}
	if _, err := fixture.controller.GetRun(t.Context(), request.RunID); !errors.Is(err, store.ErrNotFound) {
		t.Fatalf("compile created a run: %v", err)
	}
	tampered := request.Definition
	tampered.Nodes[0].DescriptorDigest = testPackageDigest
	if _, err := fixture.controller.Compile(tampered); err == nil {
		t.Fatal("compile accepted a node descriptor pin that is not installed")
	}
}

func TestControllerFailsClosedInsteadOfReplayingExpiredEffectfulNode(t *testing.T) {
	fixture := newEffectControllerFixture(t, "run-effect-expired")
	invoked, err := fixture.controller.Invoke(t.Context(), fixture.request)
	if err != nil {
		t.Fatal(err)
	}
	injected := errors.New("simulated loss after effectful claim")
	fixture.controller.afterClaim = func(contracts.InvocationLedger) error { return injected }
	_, err = fixture.controller.Drive(t.Context(), invoked.Run.RunID)
	if !errors.Is(err, injected) {
		t.Fatalf("drive interruption error = %v", err)
	}
	fixture.clock.Advance(2 * time.Minute)
	recovered, err := New(Config{
		Store: fixture.store, Nodes: fixture.registry, OwnerRef: "controller-effect-takeover",
		LeaseDuration: time.Minute, InvocationTimeout: 10 * time.Second, Clock: fixture.clock, Grants: fixture.grants,
	})
	if err != nil {
		t.Fatal(err)
	}
	run, err := recovered.Drive(t.Context(), invoked.Run.RunID)
	if err != nil || run.Status != contracts.RunFailed || fixture.executor.Calls() != 0 {
		t.Fatalf("failed-closed run = %#v, err = %v, calls = %d", run, err, fixture.executor.Calls())
	}
}

type controllerFixture struct {
	controller  *Controller
	store       *filestore.Store
	path        string
	registry    *node.Registry
	clock       *fakeClock
	definition  contracts.WorkflowDefinition
	action      contracts.ActionVersion
	prepare     *countExecutor
	render      *countExecutor
	diagnostics *countExecutor
}

type effectExecutor struct {
	descriptor contracts.NodeDescriptor
	mu         sync.Mutex
	calls      int
	owned      bool
}

func (executor *effectExecutor) Descriptor() contracts.NodeDescriptor { return executor.descriptor }

func (executor *effectExecutor) Execute(_ context.Context, request contracts.NodeInvocationRequest) (contracts.NodeResult, error) {
	executor.mu.Lock()
	executor.calls++
	executor.mu.Unlock()
	intent := map[string]any{"executableRef": "simulator"}
	intentDigest, _ := canonicaljson.DigestValue(intent)
	ownership, compensation := contracts.EffectDetached, contracts.CompensationNone
	if executor.owned {
		ownership, compensation = contracts.EffectOwned, contracts.CompensationRequired
	}
	proposal := contracts.EffectProposal{
		EffectKey: "start-process", Kind: "xgc.process-start/v1", TargetRef: "simulator",
		IntentSchemaDigest: testPackageDigest, Intent: intent, IntentDigest: intentDigest,
		Ownership: ownership, CompensationPolicy: compensation,
		RequiredCapabilityRefs: []string{"process.control"}, PolicyDigest: request.CapabilityGrants[0].AuthorizationDigest,
		Deadline: request.Deadline,
	}
	evidence, _ := canonicaljson.DigestValue(proposal)
	return contracts.NodeResult{
		Status: contracts.NodeResultWaiting, Effects: []contracts.EffectProposal{proposal},
		Wait:           &contracts.NodeWait{Kind: contracts.NodeWaitEffect, SubjectRef: proposal.EffectKey, ConditionDigest: intentDigest},
		EvidenceDigest: evidence,
	}, nil
}

func (executor *effectExecutor) Resume(_ context.Context, request contracts.NodeResumeRequest) (contracts.NodeResult, error) {
	output := map[string]any{}
	digest, _ := canonicaljson.DigestValue(output)
	evidence, _ := canonicaljson.DigestValue(map[string]any{
		"resolutionDigest": request.Resolution.PayloadDigest,
		"subjectRef":       request.Resolution.SubjectRef,
	})
	return contracts.NodeResult{
		Status: contracts.NodeResultSucceeded, Output: output,
		OutputDigest: digest, EvidenceDigest: evidence,
	}, nil
}

func (executor *effectExecutor) Calls() int {
	executor.mu.Lock()
	defer executor.mu.Unlock()
	return executor.calls
}

type staticGrants struct{ clock *fakeClock }

func (resolver staticGrants) ResolveGrants(_ context.Context, _ contracts.Run, _ contracts.NodeDescriptor, deadline time.Time) ([]contracts.CapabilityGrant, error) {
	return []contracts.CapabilityGrant{{
		CapabilityRef: "process.control", Scope: "target", HandleRef: "grant-process",
		AuthorizationDigest: testPackageDigest, ExpiresAt: deadline,
	}}, nil
}

type effectControllerFixture struct {
	controller *Controller
	store      *filestore.Store
	path       string
	registry   *node.Registry
	clock      *fakeClock
	grants     staticGrants
	executor   *effectExecutor
	request    InvokeRequest
}

func newEffectControllerFixture(t *testing.T, runID string) effectControllerFixture {
	t.Helper()
	empty := contracts.Schema{Type: contracts.TypeObject, Properties: map[string]contracts.Schema{}}
	descriptor := descriptor(t, "xgc.test.effect/v1", empty, empty)
	descriptor.Mode = contracts.NodeEffectful
	descriptor.RequiredCapabilities = []contracts.CapabilityRequirement{{CapabilityRef: "process.control", Scope: "target"}}
	descriptor.AllowedEffectKinds = []string{"xgc.process-start/v1"}
	descriptor.DescriptorDigest = ""
	descriptor.DescriptorDigest, _ = node.DescriptorDigest(descriptor)
	executor := &effectExecutor{descriptor: descriptor}
	registry := node.NewRegistry()
	if err := registry.Register(executor); err != nil {
		t.Fatal(err)
	}
	if _, err := registry.Seal(); err != nil {
		t.Fatal(err)
	}
	definition := contracts.WorkflowDefinition{
		SchemaVersion: workflowkernel.SchemaVersion, WorkflowID: "effect.workflow", Version: "v1",
		InputSchema: empty, ResultSchema: empty, TriggerSchema: empty, ScopeSchema: empty,
		Entrypoints: map[string]string{"main": "launch"},
		Nodes: []contracts.WorkflowNodeDefinition{{
			NodeID: "launch", TypeRef: descriptor.TypeRef, DescriptorDigest: descriptor.DescriptorDigest,
			InputSchema: empty, OutputSchema: empty,
		}},
		ResultBindings: map[string][]contracts.ValueBinding{"main": {}},
	}
	plan, err := workflowkernel.Compile(definition)
	if err != nil {
		t.Fatal(err)
	}
	actionVersion := contracts.ActionVersion{
		ActionID: definition.WorkflowID, Version: definition.Version, DefinitionDigest: plan.DefinitionDigest,
		Entrypoint: "main", InputSchema: empty, ResultSchema: empty,
		AcceptedTriggerKinds: []contracts.TriggerKind{contracts.TriggerManual},
	}
	path := t.TempDir() + "/effects.db"
	durable, err := filestore.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = durable.Close() })
	clock := &fakeClock{now: time.Date(2026, 8, 12, 21, 0, 0, 0, time.UTC)}
	grants := staticGrants{clock: clock}
	workflowController, err := New(Config{
		Store: durable, Nodes: registry, OwnerRef: "controller-effects",
		LeaseDuration: time.Minute, InvocationTimeout: 10 * time.Second, Clock: clock, Grants: grants,
	})
	if err != nil {
		t.Fatal(err)
	}
	request := InvokeRequest{
		RunID: runID, NamespaceID: "test", Action: actionVersion, Definition: definition,
		Trigger: contracts.TriggerEvent{
			EventID: "event-" + runID, Kind: contracts.TriggerManual, Version: "v1",
			OccurredAt: clock.Now(), ReceivedAt: clock.Now(), SourceRef: "test-suite", ActorRef: "operator",
			PayloadSchemaDigest: testPackageDigest, Payload: map[string]any{},
		},
		Candidate: map[string]any{}, CandidateOrigin: contracts.OriginCaller, CandidateRef: "manual-form",
		Scope: map[string]any{}, CommandID: "invoke-" + runID,
	}
	return effectControllerFixture{
		controller: workflowController, store: durable, path: path, registry: registry, clock: clock,
		grants: grants, executor: executor, request: request,
	}
}

func newControllerFixture(t *testing.T) controllerFixture {
	t.Helper()
	stringSchema := contracts.Schema{Type: contracts.TypeString}
	emptyObject := contracts.Schema{Type: contracts.TypeObject, Properties: map[string]contracts.Schema{}}
	prepareInput := contracts.Schema{Type: contracts.TypeObject, Properties: map[string]contracts.Schema{"name": stringSchema}, Required: []string{"name"}}
	prepareOutput := contracts.Schema{Type: contracts.TypeObject, Properties: map[string]contracts.Schema{"greeting": stringSchema}, Required: []string{"greeting"}}
	renderInput := contracts.Schema{Type: contracts.TypeObject, Properties: map[string]contracts.Schema{"message": stringSchema}, Required: []string{"message"}}
	textOutput := contracts.Schema{Type: contracts.TypeObject, Properties: map[string]contracts.Schema{"text": stringSchema}, Required: []string{"text"}}
	prepare := &countExecutor{descriptor: descriptor(t, "xgc.test.prepare/v1", prepareInput, prepareOutput), function: func(input map[string]any) map[string]any {
		return map[string]any{"greeting": "hello " + input["name"].(string)}
	}}
	render := &countExecutor{descriptor: descriptor(t, "xgc.test.render/v1", renderInput, textOutput), function: func(input map[string]any) map[string]any {
		return map[string]any{"text": strings.ToUpper(input["message"].(string))}
	}}
	diagnostics := &countExecutor{descriptor: descriptor(t, "xgc.test.diagnostics/v1", emptyObject, textOutput), function: func(map[string]any) map[string]any {
		return map[string]any{"text": "DIAGNOSTICS"}
	}}
	registry := node.NewRegistry()
	for _, executor := range []nodesdk.Executor{prepare, render, diagnostics} {
		if err := registry.Register(executor); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := registry.Seal(); err != nil {
		t.Fatal(err)
	}
	definition := contracts.WorkflowDefinition{
		SchemaVersion: workflowkernel.SchemaVersion, WorkflowID: "test.workflow", Version: "v1",
		InputSchema: prepareInput, ResultSchema: textOutput, TriggerSchema: emptyObject, ScopeSchema: emptyObject,
		Entrypoints: map[string]string{"main": "prepare", "diagnostics": "diagnostics"},
		Nodes: []contracts.WorkflowNodeDefinition{
			{NodeID: "prepare", TypeRef: prepare.descriptor.TypeRef, DescriptorDigest: prepare.descriptor.DescriptorDigest, InputSchema: prepareInput, OutputSchema: prepareOutput, Bindings: []contracts.ValueBinding{{Target: "/name", Value: contracts.ValueExpr{Ref: "inputs.name"}}}},
			{NodeID: "render", TypeRef: render.descriptor.TypeRef, DescriptorDigest: render.descriptor.DescriptorDigest, InputSchema: renderInput, OutputSchema: textOutput, Bindings: []contracts.ValueBinding{{Target: "/message", Value: contracts.ValueExpr{Ref: "nodes.prepare.output.greeting"}}}},
			{NodeID: "diagnostics", TypeRef: diagnostics.descriptor.TypeRef, DescriptorDigest: diagnostics.descriptor.DescriptorDigest, InputSchema: emptyObject, OutputSchema: textOutput},
		},
		Edges: []contracts.WorkflowEdge{{From: "prepare", To: "render", Kind: contracts.EdgeData}},
		ResultBindings: map[string][]contracts.ValueBinding{
			"main":        {{Target: "/text", Value: contracts.ValueExpr{Ref: "nodes.render.output.text"}}},
			"diagnostics": {{Target: "/text", Value: contracts.ValueExpr{Ref: "nodes.diagnostics.output.text"}}},
		},
	}
	plan, err := workflowkernel.Compile(definition)
	if err != nil {
		t.Fatal(err)
	}
	actionVersion := contracts.ActionVersion{
		ActionID: definition.WorkflowID, Version: definition.Version, DefinitionDigest: plan.DefinitionDigest,
		Entrypoint: "main", InputSchema: definition.InputSchema, ResultSchema: definition.ResultSchema,
		AcceptedTriggerKinds: []contracts.TriggerKind{contracts.TriggerManual},
	}
	path := t.TempDir() + "/orchestration.db"
	durable, err := filestore.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = durable.Close() })
	clock := &fakeClock{now: time.Date(2026, 8, 12, 20, 0, 0, 0, time.UTC)}
	workflowController, err := New(Config{
		Store: durable, Nodes: registry, OwnerRef: "controller-test",
		LeaseDuration: time.Minute, InvocationTimeout: 10 * time.Second, Clock: clock,
	})
	if err != nil {
		t.Fatal(err)
	}
	return controllerFixture{
		controller: workflowController, store: durable, path: path, registry: registry, clock: clock,
		definition: definition, action: actionVersion, prepare: prepare, render: render, diagnostics: diagnostics,
	}
}

func (fixture controllerFixture) request(runID, commandID string) InvokeRequest {
	return InvokeRequest{
		RunID: runID, NamespaceID: "test", Action: fixture.action, Definition: fixture.definition,
		Trigger: contracts.TriggerEvent{
			EventID: "event-" + runID, Kind: contracts.TriggerManual, Version: "v1",
			OccurredAt: fixture.clock.Now(), ReceivedAt: fixture.clock.Now(), SourceRef: "test-suite", ActorRef: "operator",
			PayloadSchemaDigest: testPackageDigest, Payload: map[string]any{},
		},
		Candidate: map[string]any{"name": "XGC"}, CandidateOrigin: contracts.OriginCaller,
		CandidateRef: "manual-form", Scope: map[string]any{}, CommandID: commandID,
	}
}

func descriptor(t *testing.T, typeRef string, input, output contracts.Schema) contracts.NodeDescriptor {
	t.Helper()
	descriptor := contracts.NodeDescriptor{
		SchemaVersion: node.DescriptorSchemaVersion, TypeRef: typeRef, DisplayName: typeRef,
		PackageRef: "xgc-nodes-test", PackageDigest: testPackageDigest,
		InputSchema: input, OutputSchema: output, Mode: contracts.NodePure, Determinism: contracts.NodeDeterministic,
		MaxInputBytes: 1 << 20, MaxOutputBytes: 1 << 20,
	}
	digest, err := node.DescriptorDigest(descriptor)
	if err != nil {
		t.Fatal(err)
	}
	descriptor.DescriptorDigest = digest
	return descriptor
}
