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

func fixtureStoreCommandScope(run contracts.Run, operation string) store.CommandScope {
	return store.CommandScope{
		SchemaVersion: store.CommandScopeSchemaVersion, Operation: operation,
		NamespaceID: run.NamespaceID, ResourceType: runAggregateType, ResourceID: run.RunID,
		AuthorityRef: run.AdmissionPolicyRef, AuthorityDigest: run.AdmissionPolicyDigest,
	}
}

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
	failures   int
	function   func(map[string]any) map[string]any
}

func (executor *countExecutor) Descriptor() contracts.NodeDescriptor { return executor.descriptor }

func (executor *countExecutor) Execute(_ context.Context, request contracts.NodeInvocationRequest) (contracts.NodeResult, error) {
	executor.mu.Lock()
	executor.calls++
	if executor.failures > 0 {
		executor.failures--
		executor.mu.Unlock()
		failure := contracts.StructuredFailure{Class: contracts.FailureTransient, Code: "test.transient", Message: "retry after a transient test failure"}
		evidence, _ := canonicaljson.DigestValue(failure)
		return contracts.NodeResult{SchemaVersion: node.ResultSchemaVersion, Status: contracts.NodeResultFailed, Failure: &failure, EvidenceDigest: evidence}, nil
	}
	executor.mu.Unlock()
	output := executor.function(request.Input)
	digest, err := canonicaljson.DigestValue(output)
	if err != nil {
		return contracts.NodeResult{}, err
	}
	return contracts.NodeResult{SchemaVersion: node.ResultSchemaVersion, Status: contracts.NodeResultSucceeded, Output: output, OutputDigest: digest, EvidenceDigest: digest}, nil
}

func TestCoordinatorPreservesRetryWaitUntilItsDurableDeadline(t *testing.T) {
	fixture := newControllerFixture(t)
	fixture.prepare.failures = 1
	invoked, err := fixture.controller.Invoke(t.Context(), fixture.request("run-retry-wait", "invoke-retry-wait"))
	if err != nil {
		t.Fatal(err)
	}
	coordinator, err := NewCoordinator(CoordinatorConfig{
		Controller: fixture.controller, Store: fixture.store,
		OwnerRef: "retry-wait-coordinator", Clock: fixture.clock,
	})
	if err != nil {
		t.Fatal(err)
	}
	waiting, err := coordinator.AdvanceRun(t.Context(), invoked.Run.RunID)
	if !errors.Is(err, ErrRunWaiting) || waiting.Status != contracts.RunWaiting {
		t.Fatalf("retry wait run = %#v, err=%v", waiting, err)
	}
	snapshot, _, err := fixture.controller.GetSnapshot(t.Context(), invoked.Run.RunID)
	if err != nil || snapshot.RetryWait == nil || snapshot.Waiting != nil {
		t.Fatalf("retry wait snapshot = %#v, err=%v", snapshot.RetryWait, err)
	}
	fixture.clock.Advance(time.Second)
	succeeded, err := coordinator.AdvanceRun(t.Context(), invoked.Run.RunID)
	if err != nil || succeeded.Status != contracts.RunSucceeded || fixture.prepare.Calls() != 2 {
		t.Fatalf("retried run = %#v, err=%v, calls=%d", succeeded, err, fixture.prepare.Calls())
	}
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
	if err != nil || graph.Revision != 1 || graph.TerminalRun.Revision != run.Revision ||
		graph.TerminalRun.Status != contracts.RunSucceeded || graph.ClosureBase.Run.Revision+1 != run.Revision ||
		len(graph.ClosureBase.Invocations) != 2 {
		t.Fatalf("ownership graph = %#v, terminal revision=%d err=%v", graph, run.Revision, err)
	}
	facts, err := ownership.ClosureFacts(graph)
	if err != nil || !facts.Satisfied() || facts.OwnershipGraphRevision != graph.Revision ||
		facts.RunRevision != graph.ClosureBase.Run.Revision || facts.RunRevision == graph.TerminalRun.Revision {
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
	} else {
		before, beforeErr := canonicaljson.Marshal(graph)
		after, afterErr := canonicaljson.Marshal(recoveredGraph)
		if beforeErr != nil || afterErr != nil || string(before) != string(after) {
			t.Fatalf("recovered ownership graph is not the exact persisted snapshot: before=%s after=%s errors=%v/%v", before, after, beforeErr, afterErr)
		}
	}
}

func TestOwnershipGraphReadFailsClosedWhenTerminalRunAggregateDiffers(t *testing.T) {
	fixture := newControllerFixture(t)
	invoked, err := fixture.controller.Invoke(t.Context(), fixture.request("run-ownership-tamper", "invoke-ownership-tamper"))
	if err != nil {
		t.Fatal(err)
	}
	terminal, err := fixture.controller.Drive(t.Context(), invoked.Run.RunID)
	if err != nil || terminal.Status != contracts.RunSucceeded {
		t.Fatalf("terminal run = %#v err=%v", terminal, err)
	}
	graphRecord, err := fixture.store.GetAggregate(t.Context(), ownershipGraphKey(terminal.RunID))
	if err != nil {
		t.Fatal(err)
	}
	runRecord, err := fixture.store.GetAggregate(t.Context(), runKey(terminal.RunID))
	if err != nil {
		t.Fatal(err)
	}
	tampered := terminal
	tampered.ResultRef = "result-tampered"
	runMutation, err := aggregateMutation(runKey(terminal.RunID), runRecord.Revision, tampered)
	if err != nil {
		t.Fatal(err)
	}
	at := terminal.UpdatedAt.Add(time.Second)
	event, err := aggregateEvent(runMutation, "run.test-tampered", "tamper-terminal-run", at, map[string]any{"runId": terminal.RunID})
	if err != nil {
		t.Fatal(err)
	}
	outcome, err := canonicaljson.Marshal(tampered)
	if err != nil {
		t.Fatal(err)
	}
	identity, err := canonicaljson.DigestValue(map[string]any{"commandId": "tamper-terminal-run", "mutation": runMutation})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := fixture.store.Commit(t.Context(), store.Transaction{
		CommandScope: fixtureStoreCommandScope(terminal, "fixture.tamper"), CommandID: "tamper-terminal-run", IdentityDigest: identity,
		Expected:  []store.ExpectedRevision{{Key: runMutation.Key, Revision: runRecord.Revision}},
		Mutations: []store.AggregateRecord{runMutation}, Events: []contracts.DomainEvent{event},
		Outcome: outcome, At: at,
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := fixture.controller.OwnershipGraph(t.Context(), terminal.RunID); err == nil {
		t.Fatal("ownership graph dynamically accepted a different terminal Run aggregate")
	}
	if unchanged, err := fixture.store.GetAggregate(t.Context(), ownershipGraphKey(terminal.RunID)); err != nil ||
		unchanged.Revision != graphRecord.Revision || unchanged.PayloadDigest != graphRecord.PayloadDigest {
		t.Fatalf("ownership graph changed during failed read: %#v err=%v", unchanged, err)
	}
}

func TestPreexistingOwnershipGraphPreventsTerminalTransition(t *testing.T) {
	fixture := newControllerFixture(t)
	invoked, err := fixture.controller.Invoke(t.Context(), fixture.request("run-forged-ownership", "invoke-forged-ownership"))
	if err != nil {
		t.Fatal(err)
	}
	run, err := fixture.controller.GetRun(t.Context(), invoked.Run.RunID)
	if err != nil {
		t.Fatal(err)
	}
	forged := map[string]any{"forged": true, "runId": run.RunID}
	mutation, err := aggregateMutation(ownershipGraphKey(run.RunID), 0, forged)
	if err != nil {
		t.Fatal(err)
	}
	at := run.UpdatedAt.Add(time.Second)
	event, err := aggregateEvent(mutation, "ownership-graph.forged", "forge-ownership-graph", at, map[string]any{"runId": run.RunID})
	if err != nil {
		t.Fatal(err)
	}
	outcome, err := canonicaljson.Marshal(forged)
	if err != nil {
		t.Fatal(err)
	}
	identity, err := canonicaljson.DigestValue(map[string]any{"commandId": "forge-ownership-graph", "mutation": mutation})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := fixture.store.Commit(t.Context(), store.Transaction{
		CommandScope: fixtureStoreCommandScope(run, "fixture.forge"), CommandID: "forge-ownership-graph", IdentityDigest: identity,
		Expected: []store.ExpectedRevision{{Key: mutation.Key, Revision: 0}}, Mutations: []store.AggregateRecord{mutation},
		Events: []contracts.DomainEvent{event}, Outcome: outcome, At: at,
	}); err != nil {
		t.Fatal(err)
	}
	var driveErr error
	for range 10 {
		_, driveErr = fixture.controller.Drive(t.Context(), run.RunID)
		if driveErr != nil {
			break
		}
	}
	if driveErr == nil || (!strings.Contains(driveErr.Error(), "immutable ownership closure proof") &&
		!strings.Contains(driveErr.Error(), "durable ownership graph is invalid")) {
		t.Fatalf("forged graph drive error = %v", driveErr)
	}
	after, err := fixture.controller.GetRun(t.Context(), run.RunID)
	if err != nil {
		t.Fatal(err)
	}
	if after.Status.Terminal() {
		t.Fatalf("forged graph allowed terminal transition: %#v", after)
	}
	record, err := fixture.store.GetAggregate(t.Context(), ownershipGraphKey(run.RunID))
	if err != nil || record.Revision != 1 || record.PayloadDigest != mutation.PayloadDigest {
		t.Fatalf("forged graph was overwritten: %#v err=%v", record, err)
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
	snapshot, _, err := fixture.controller.GetSnapshot(t.Context(), run.RunID)
	if err != nil || snapshot.SchemaVersion != RunSnapshotSchemaVersion || snapshot.Waiting == nil ||
		snapshot.Waiting.InvocationID != invocationID || snapshot.Waiting.WaitGeneration != 1 {
		t.Fatalf("waiting occurrence projection = %#v err=%v", snapshot, err)
	}
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
	if err := fixture.store.Close(); err != nil {
		t.Fatal(err)
	}
	reopened, err := filestore.Open(fixture.path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = reopened.Close() })
	recovered, err := New(Config{
		Store: reopened, Nodes: fixture.registry, OwnerRef: "waiting-projection-recovery",
		LeaseDuration: time.Minute, InvocationTimeout: 10 * time.Second, Clock: fixture.clock, Grants: fixture.grants,
	})
	if err != nil {
		t.Fatal(err)
	}
	recoveredSnapshot, _, err := recovered.GetSnapshot(t.Context(), run.RunID)
	if err != nil || recoveredSnapshot.Waiting == nil || recoveredSnapshot.Waiting.InvocationID != invocationID ||
		recoveredSnapshot.Waiting.WaitGeneration != snapshot.Waiting.WaitGeneration {
		t.Fatalf("recovered waiting projection = %#v err=%v", recoveredSnapshot, err)
	}
}

func TestSnapshotWaitingOccurrenceFailsClosedOnLedgerMismatch(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(*RunSnapshot)
	}{
		{name: "legacy-v1", mutate: func(snapshot *RunSnapshot) {
			snapshot.SchemaVersion = "xgc.run-snapshot/v1"
		}},
		{name: "wrong-run", mutate: func(snapshot *RunSnapshot) {
			snapshot.RunID = "another-run"
		}},
		{name: "missing-wait", mutate: func(snapshot *RunSnapshot) {
			snapshot.Waiting = nil
		}},
		{name: "ledger-generation-mismatch", mutate: func(snapshot *RunSnapshot) {
			next := *snapshot.Waiting
			next.WaitGeneration++
			snapshot.Waiting = &next
		}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			fixture := newEffectControllerFixture(t, "run-wait-corrupt-"+test.name)
			invoked, err := fixture.controller.Invoke(t.Context(), fixture.request)
			if err != nil {
				t.Fatal(err)
			}
			waiting, err := fixture.controller.Drive(t.Context(), invoked.Run.RunID)
			if err != nil || waiting.Status != contracts.RunWaiting {
				t.Fatalf("waiting Run = %#v err=%v", waiting, err)
			}
			snapshot, snapshotRevision, err := fixture.controller.GetSnapshot(t.Context(), waiting.RunID)
			if err != nil || snapshot.Waiting == nil {
				t.Fatalf("waiting Snapshot = %#v err=%v", snapshot, err)
			}
			test.mutate(&snapshot)
			commitSnapshotTamper(t, fixture.store, waiting, snapshotRevision, snapshot, "corrupt-"+test.name)
			if _, _, err := fixture.controller.GetSnapshot(t.Context(), waiting.RunID); !errors.Is(err, store.ErrCorrupt) {
				t.Fatalf("corrupt waiting occurrence read error = %v", err)
			}
		})
	}
}

func commitSnapshotTamper(
	t *testing.T, durable store.Store, run contracts.Run, expected uint64, snapshot RunSnapshot, commandID string,
) {
	t.Helper()
	mutation, err := aggregateMutation(snapshotKey(run.RunID), expected, snapshot)
	if err != nil {
		t.Fatal(err)
	}
	at := run.UpdatedAt.Add(time.Second)
	event, err := aggregateEvent(mutation, "snapshot.test-corrupted", commandID, at, map[string]any{"runId": run.RunID})
	if err != nil {
		t.Fatal(err)
	}
	outcome, err := canonicaljson.Marshal(snapshot)
	if err != nil {
		t.Fatal(err)
	}
	identity, err := canonicaljson.DigestValue(map[string]any{"commandId": commandID, "mutation": mutation})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := durable.Commit(t.Context(), store.Transaction{
		CommandScope: fixtureStoreCommandScope(run, "fixture.snapshot-tamper"), CommandID: commandID, IdentityDigest: identity,
		Expected:  []store.ExpectedRevision{{Key: mutation.Key, Revision: expected}},
		Mutations: []store.AggregateRecord{mutation}, Events: []contracts.DomainEvent{event},
		Outcome: outcome, At: at,
	}); err != nil {
		t.Fatal(err)
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

func (adapter *compensatingEffectAdapter) RecoverCompensation(
	_ context.Context,
	applied contracts.EffectRecord,
	ledger contracts.CommandLedger,
	authorizationDigest string,
) (contracts.CommandLedger, error) {
	if applied.State != contracts.EffectApplied || applied.ExternalIdentity == "" ||
		len(ledger.Receipts) != 1 || ledger.Receipts[0].Status != contracts.ReceiptAccepted {
		return contracts.CommandLedger{}, errors.New("compensation recovery lacks exact accepted evidence")
	}
	adapter.mu.Lock()
	adapter.order = append(adapter.order, applied.Intent.InvocationID)
	adapter.mu.Unlock()
	descriptor := adapter.Descriptor()
	succeeded, err := syntheticReceipt(
		ledger.Envelope, descriptor, authorizationDigest, 2,
		contracts.ReceiptSucceeded, nil, adapter.clock.Now(),
	)
	if err != nil {
		return contracts.CommandLedger{}, err
	}
	succeeded.ResultDigest, _ = canonicaljson.DigestValue(map[string]any{"stopped": applied.ExternalIdentity})
	succeeded.ExternalIdentity = applied.ExternalIdentity
	return contracts.CommandLedger{
		Envelope: ledger.Envelope,
		Receipts: append(append([]contracts.CommandReceipt(nil), ledger.Receipts...), succeeded),
	}, nil
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
	stoppedSnapshot, _, err := fixture.controller.GetSnapshot(t.Context(), waiting.RunID)
	if err != nil || stoppedSnapshot.Waiting != nil {
		t.Fatalf("terminal Snapshot retained waiting occurrence: %#v err=%v", stoppedSnapshot, err)
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

type intentLookupStore struct {
	store.Store
	missing map[string]bool
	dead    map[string]bool
}

func (durable intentLookupStore) GetIntent(ctx context.Context, identity string) (store.IntentRecord, error) {
	if durable.missing[identity] {
		return store.IntentRecord{}, store.ErrNotFound
	}
	record, err := durable.Store.GetIntent(ctx, identity)
	if err == nil && durable.dead[identity] {
		record.Status = store.IntentDead
	}
	return record, err
}

func TestTerminationRequiredIntentLedgerFailsClosedOnMissingOrDeadIdentity(t *testing.T) {
	fixture := newControllerFixture(t)
	invoked, err := fixture.controller.Invoke(t.Context(), fixture.request("run-stop-intent-ledger", "invoke-stop-intent-ledger"))
	if err != nil {
		t.Fatal(err)
	}
	stopping, err := fixture.controller.RequestRunTermination(t.Context(), TerminateRunRequest{
		RunID: invoked.Run.RunID, ExpectedRevision: invoked.Run.Revision, Kind: contracts.TerminationStopped,
		RequestedBy: "operator", ReasonCode: "operator.stop", CommandID: "stop-intent-ledger",
	})
	if err != nil {
		t.Fatal(err)
	}
	snapshot, _, err := fixture.controller.GetSnapshot(t.Context(), invoked.Run.RunID)
	if err != nil {
		t.Fatal(err)
	}
	identities, err := requiredTerminationIntentIDs(t.Context(), fixture.store, stopping.Run, snapshot)
	if err != nil || len(identities) != 1 {
		t.Fatalf("required intent ledger = %#v err=%v", identities, err)
	}
	missing := *fixture.controller
	missing.store = intentLookupStore{Store: fixture.store, missing: map[string]bool{identities[0]: true}}
	if _, err := missing.terminationIntentsSettled(t.Context(), stopping.Run, snapshot); err == nil || !strings.Contains(err.Error(), "missing") {
		t.Fatalf("missing required intent error = %v", err)
	}
	dead := *fixture.controller
	dead.store = intentLookupStore{Store: fixture.store, dead: map[string]bool{identities[0]: true}}
	if _, err := dead.terminationIntentsSettled(t.Context(), stopping.Run, snapshot); err == nil || !strings.Contains(err.Error(), "dead") {
		t.Fatalf("dead required intent error = %v", err)
	}
}

func TestFailedAndCanceledRunsCannotBypassIntentSettlement(t *testing.T) {
	for _, kind := range []contracts.TerminationKind{contracts.TerminationFailed, contracts.TerminationCanceled} {
		t.Run(string(kind), func(t *testing.T) {
			fixture := newControllerFixture(t)
			invoked, err := fixture.controller.Invoke(t.Context(), fixture.request("run-settlement-"+string(kind), "invoke-settlement-"+string(kind)))
			if err != nil {
				t.Fatal(err)
			}
			request := TerminateRunRequest{
				RunID: invoked.Run.RunID, ExpectedRevision: invoked.Run.Revision, Kind: kind,
				RequestedBy: "operator", ReasonCode: "operator." + string(kind), CommandID: "terminate-settlement-" + string(kind),
			}
			if kind == contracts.TerminationFailed {
				request.PrimaryFailure = &contracts.StructuredFailure{Class: contracts.FailurePermanent, Code: "operator.failed", Message: "operator marked the run failed"}
			}
			stopping, err := fixture.controller.RequestRunTermination(t.Context(), request)
			if err != nil {
				t.Fatal(err)
			}
			cleanupID, err := execution.StableIntentID(contracts.IntentCleanup, stopping.Run.RunID, stopping.Run.Revision)
			if err != nil {
				t.Fatal(err)
			}
			closed := *fixture.controller
			closed.store = intentLookupStore{Store: fixture.store, missing: map[string]bool{cleanupID: true}}
			if terminal, err := closed.Drive(t.Context(), stopping.Run.RunID); err == nil || !strings.Contains(err.Error(), "missing") || terminal.Status.Terminal() {
				t.Fatalf("%s settlement bypass = %#v err=%v", kind, terminal, err)
			}
		})
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

func TestTerminationRecoversAcceptedEffectCompensationAfterControllerRestart(t *testing.T) {
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
	compensationRequest, err := planner.PlanEffectCompensation(t.Context(), pending)
	if err != nil {
		t.Fatal(err)
	}
	begun, err := fixture.controller.BeginEffectCompensation(t.Context(), compensationRequest)
	if err != nil {
		t.Fatal(err)
	}
	accepted, err := syntheticReceipt(
		begun.Ledger.Envelope, adapter.Descriptor(), testPackageDigest, 1,
		contracts.ReceiptAccepted, nil, fixture.clock.Now(),
	)
	if err != nil {
		t.Fatal(err)
	}
	acceptedResult, err := fixture.controller.ObserveEffectCompensation(t.Context(), ObserveEffectCompensationRequest{
		EffectID: effectID, Receipt: accepted, CommandID: "persist-accepted-before-controller-crash",
	})
	if err != nil || acceptedResult.Effect.CompensationState != contracts.EffectCompensationRunning {
		t.Fatalf("accepted compensation = %#v, err=%v", acceptedResult, err)
	}
	fixture.clock.Advance(10 * time.Second)
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

func TestRequiredCompensationFailureNeedsExactReconciliationProofToCloseRun(t *testing.T) {
	fixture := newEffectControllerFixture(t, "run-stop-reconcile-proof")
	fixture.executor.owned = true
	invoked, err := fixture.controller.Invoke(t.Context(), fixture.request)
	if err != nil {
		t.Fatal(err)
	}
	waiting, err := fixture.controller.Drive(t.Context(), invoked.Run.RunID)
	if err != nil || waiting.Status != contracts.RunWaiting {
		t.Fatalf("waiting Run = %#v err=%v", waiting, err)
	}
	planner := staticCompensationPlan{staticEffectPlan{clock: fixture.clock}}
	adapter := &compensatingEffectAdapter{successfulEffectAdapter: &successfulEffectAdapter{clock: fixture.clock}}
	coordinator, err := NewCoordinator(CoordinatorConfig{
		Controller: fixture.controller, Store: fixture.store, Planner: planner,
		Credentials: plannedEffectCredentials{}, Adapters: []EffectAdapter{adapter},
		OwnerRef: "proof-coordinator", Clock: fixture.clock,
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
	if batch, err := coordinator.runBatch(t.Context(), contracts.IntentOutbox, "apply-before-proof-stop"); err != nil || batch.Completed != 1 {
		t.Fatalf("primary outbox = %#v err=%v", batch, err)
	}
	current, err := fixture.controller.GetRun(t.Context(), waiting.RunID)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := fixture.controller.RequestRunTermination(t.Context(), TerminateRunRequest{
		RunID: current.RunID, ExpectedRevision: current.Revision, Kind: contracts.TerminationStopped,
		RequestedBy: "operator", ReasonCode: "operator.stop", CommandID: "stop-before-proof",
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := fixture.controller.Drive(t.Context(), current.RunID); !errors.Is(err, ErrRunClosureOpen) {
		t.Fatalf("schedule compensation error = %v", err)
	}
	pending, err := fixture.controller.GetEffect(t.Context(), effectID)
	if err != nil || pending.CompensationState != contracts.EffectCompensationPending {
		t.Fatalf("pending compensation = %#v err=%v", pending, err)
	}
	compensationRequest, err := planner.PlanEffectCompensation(t.Context(), pending)
	if err != nil {
		t.Fatal(err)
	}
	begun, err := fixture.controller.BeginEffectCompensation(t.Context(), compensationRequest)
	if err != nil {
		t.Fatal(err)
	}
	descriptor := adapter.Descriptor()
	accepted, err := syntheticReceipt(begun.Ledger.Envelope, descriptor, testPackageDigest, 1, contracts.ReceiptAccepted, nil, fixture.clock.Now())
	if err != nil {
		t.Fatal(err)
	}
	acceptedResult, err := fixture.controller.ObserveEffectCompensation(t.Context(), ObserveEffectCompensationRequest{
		EffectID: effectID, Receipt: accepted, CommandID: "observe-proof-accepted",
	})
	if err != nil {
		t.Fatal(err)
	}
	failure := &contracts.StructuredFailure{Class: contracts.FailurePermanent, Code: "cleanup.failed", Message: "cleanup was not proved"}
	failedReceipt, err := syntheticReceipt(acceptedResult.Ledger.Envelope, descriptor, testPackageDigest, 2, contracts.ReceiptFailed, failure, fixture.clock.Now())
	if err != nil {
		t.Fatal(err)
	}
	failedResult, err := fixture.controller.ObserveEffectCompensation(t.Context(), ObserveEffectCompensationRequest{
		EffectID: effectID, Receipt: failedReceipt, CommandID: "observe-proof-failed",
	})
	if err != nil || failedResult.Effect.CompensationState != contracts.EffectCompensationFailed {
		t.Fatalf("failed compensation = %#v err=%v", failedResult.Effect, err)
	}
	stoppingRun, err := fixture.controller.GetRun(t.Context(), current.RunID)
	if err != nil {
		t.Fatal(err)
	}
	base, err := fixture.controller.deriveOwnershipClosure(t.Context(), stoppingRun)
	if err != nil {
		t.Fatal(err)
	}
	facts, err := ownership.DeriveClosureFacts(base, 1)
	if err != nil || facts.OpenEffectCompensationCount != 1 {
		t.Fatalf("failed Required closure = %#v err=%v", facts, err)
	}
	request := ReconcileEffectCompensationRequest{
		EffectID: effectID, ExpectedRevision: failedResult.Effect.Revision,
		EvidenceDigest: testPackageDigest, ReconciledBy: "cleanup-reconciler",
		ReasonCode: "provider.cleanup-observed", CommandID: "reconcile-required-cleanup",
	}
	reconciled, err := fixture.controller.ReconcileEffectCompensation(t.Context(), request)
	if err != nil || reconciled.Replay || reconciled.Effect.CompensationState != contracts.EffectCompensationReconciled {
		t.Fatalf("reconciled compensation = %#v err=%v", reconciled, err)
	}
	replay, err := fixture.controller.ReconcileEffectCompensation(t.Context(), request)
	if err != nil || !replay.Replay {
		t.Fatalf("reconciliation replay = %#v err=%v", replay, err)
	}
	conflict := request
	conflict.EvidenceDigest = "sha256:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
	if _, err := fixture.controller.ReconcileEffectCompensation(t.Context(), conflict); !errors.Is(err, store.ErrIdentityConflict) {
		t.Fatalf("reconciliation identity conflict = %v", err)
	}
	conflict = request
	conflict.ExpectedRevision++
	if _, err := fixture.controller.ReconcileEffectCompensation(t.Context(), conflict); !errors.Is(err, store.ErrIdentityConflict) {
		t.Fatalf("reconciliation revision changed under one command = %v", err)
	}
	base, err = fixture.controller.deriveOwnershipClosure(t.Context(), stoppingRun)
	if err != nil {
		t.Fatal(err)
	}
	facts, err = ownership.DeriveClosureFacts(base, 1)
	if err != nil || facts.OpenEffectCompensationCount != 0 {
		t.Fatalf("reconciled Required closure = %#v err=%v", facts, err)
	}
	stopped, err := coordinator.AdvanceRun(t.Context(), current.RunID)
	if err != nil || stopped.Status != contracts.RunStopped {
		t.Fatalf("proof-closed Run = %#v err=%v", stopped, err)
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

func TestInvokeRejectsTriggerSchemaDigestOutsidePinnedDefinition(t *testing.T) {
	fixture := newControllerFixture(t)
	request := fixture.request("trigger-schema-mismatch", "invoke-trigger-schema-mismatch")
	request.Trigger.PayloadSchemaDigest = "sha256:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"

	if _, err := fixture.controller.Invoke(t.Context(), request); err == nil ||
		!strings.Contains(err.Error(), "trigger payload schema digest") {
		t.Fatalf("mismatched trigger schema digest error = %v", err)
	}
	if _, err := fixture.controller.GetRun(t.Context(), request.RunID); !errors.Is(err, store.ErrNotFound) {
		t.Fatalf("mismatched trigger schema digest created a Run: %v", err)
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
	stopping, err := recovered.Drive(t.Context(), invoked.Run.RunID)
	if !errors.Is(err, ErrRunClosureOpen) || stopping.Status != contracts.RunStopping || fixture.executor.Calls() != 0 {
		t.Fatalf("failed-closed run before settlement = %#v, err = %v, calls = %d", stopping, err, fixture.executor.Calls())
	}
	coordinator, err := NewCoordinator(CoordinatorConfig{
		Controller: recovered, Store: fixture.store,
		OwnerRef: "expired-effect-cleanup", Clock: fixture.clock,
	})
	if err != nil {
		t.Fatal(err)
	}
	run, err := coordinator.AdvanceRun(t.Context(), invoked.Run.RunID)
	if err != nil || run.Status != contracts.RunFailed || fixture.executor.Calls() != 0 {
		t.Fatalf("failed-closed run after settlement = %#v, err = %v, calls = %d", run, err, fixture.executor.Calls())
	}
}

type controllerFixture struct {
	controller          *Controller
	store               *filestore.Store
	path                string
	registry            *node.Registry
	clock               *fakeClock
	definition          contracts.WorkflowDefinition
	action              contracts.ActionVersion
	triggerSchemaDigest string
	prepare             *countExecutor
	render              *countExecutor
	diagnostics         *countExecutor
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
		SchemaVersion: node.ResultSchemaVersion,
		Status:        contracts.NodeResultWaiting, Effects: []contracts.EffectProposal{proposal},
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
		SchemaVersion: node.ResultSchemaVersion,
		Status:        contracts.NodeResultSucceeded, Output: output,
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
			PayloadSchemaDigest: mustTriggerSchemaDigest(t, definition.TriggerSchema), Payload: map[string]any{},
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
			{NodeID: "prepare", TypeRef: prepare.descriptor.TypeRef, DescriptorDigest: prepare.descriptor.DescriptorDigest, InputSchema: prepareInput, OutputSchema: prepareOutput, Bindings: []contracts.ValueBinding{{Target: "/name", Value: contracts.ValueExpr{Ref: "inputs.name"}}}, Retry: &contracts.NodeRetryPolicy{MaxAttempts: 2, InitialBackoffMillis: 1000, MaxBackoffMillis: 1000}},
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
		definition: definition, action: actionVersion, triggerSchemaDigest: mustTriggerSchemaDigest(t, definition.TriggerSchema),
		prepare: prepare, render: render, diagnostics: diagnostics,
	}
}

func (fixture controllerFixture) request(runID, commandID string) InvokeRequest {
	return InvokeRequest{
		RunID: runID, NamespaceID: "test", Action: fixture.action, Definition: fixture.definition,
		Trigger: contracts.TriggerEvent{
			EventID: "event-" + runID, Kind: contracts.TriggerManual, Version: "v1",
			OccurredAt: fixture.clock.Now(), ReceivedAt: fixture.clock.Now(), SourceRef: "test-suite", ActorRef: "operator",
			PayloadSchemaDigest: fixture.triggerSchemaDigest, Payload: map[string]any{},
		},
		Candidate: map[string]any{"name": "XGC"}, CandidateOrigin: contracts.OriginCaller,
		CandidateRef: "manual-form", Scope: map[string]any{}, CommandID: commandID,
	}
}

func mustTriggerSchemaDigest(t *testing.T, schema contracts.Schema) string {
	t.Helper()
	digest, err := canonicaljson.DigestValue(schema)
	if err != nil {
		t.Fatal(err)
	}
	return digest
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
