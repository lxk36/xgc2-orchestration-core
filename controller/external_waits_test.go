package controller

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/lxk36/xgc2-orchestration-core/durable/filestore"
	"github.com/lxk36/xgc2-orchestration-core/durable/store"
	"github.com/lxk36/xgc2-orchestration-core/kernel/canonicaljson"
	"github.com/lxk36/xgc2-orchestration-core/kernel/execution"
	protocol "github.com/lxk36/xgc2-orchestration-core/kernel/node"
	workflowkernel "github.com/lxk36/xgc2-orchestration-core/kernel/workflow"
	"github.com/lxk36/xgc2-orchestration-core/sdk/go/contracts"
)

type signalWaitExecutor struct{ descriptor contracts.NodeDescriptor }

func newSignalWaitExecutor() *signalWaitExecutor {
	stringSchema := contracts.Schema{Type: contracts.TypeString}
	schema := contracts.Schema{
		Type: contracts.TypeObject, Properties: map[string]contracts.Schema{"signalId": stringSchema},
		Required: []string{"signalId"},
	}
	descriptor := contracts.NodeDescriptor{
		SchemaVersion: protocol.DescriptorSchemaVersion,
		TypeRef:       "xgc.test.signal-wait/v1", DisplayName: "Signal wait fixture",
		PackageRef: "xgc-test-signals", PackageDigest: testPackageDigest,
		InputSchema: schema, OutputSchema: schema,
		Mode: contracts.NodeWaiting, Determinism: contracts.NodeRecorded,
		MaxInputBytes: 4096, MaxOutputBytes: 4096,
	}
	descriptor.DescriptorDigest, _ = protocol.DescriptorDigest(descriptor)
	return &signalWaitExecutor{descriptor: descriptor}
}

func (executor *signalWaitExecutor) Descriptor() contracts.NodeDescriptor { return executor.descriptor }

func (executor *signalWaitExecutor) Execute(_ context.Context, request contracts.NodeInvocationRequest) (contracts.NodeResult, error) {
	signalID, _ := request.Input["signalId"].(string)
	condition, err := canonicaljson.DigestValue(map[string]any{"signalId": signalID})
	if err != nil {
		return contracts.NodeResult{}, err
	}
	evidence, err := canonicaljson.DigestValue(map[string]any{"wait": signalID, "condition": condition})
	if err != nil {
		return contracts.NodeResult{}, err
	}
	return contracts.NodeResult{
		Status: contracts.NodeResultWaiting,
		Wait: &contracts.NodeWait{
			Kind: contracts.NodeWaitEvent, SubjectRef: signalID,
			ConditionDigest: condition, ExpiresAt: timePointerForWait(request.Deadline),
		},
		EvidenceDigest: evidence,
	}, nil
}

func (executor *signalWaitExecutor) Resume(_ context.Context, request contracts.NodeResumeRequest) (contracts.NodeResult, error) {
	signalID, _ := request.Input["signalId"].(string)
	if request.Resolution.Status != contracts.NodeWaitResolvedSucceeded ||
		request.Resolution.Payload["signalId"] != signalID {
		return contracts.NodeResult{}, errors.New("signal resolution does not match the wait input")
	}
	output := map[string]any{"signalId": signalID}
	digest, err := canonicaljson.DigestValue(output)
	if err != nil {
		return contracts.NodeResult{}, err
	}
	return contracts.NodeResult{
		Status: contracts.NodeResultSucceeded, Output: output,
		OutputDigest: digest, EvidenceDigest: request.Resolution.PayloadDigest,
	}, nil
}

type externalWaitCommitBarrierStore struct {
	store.Store
	entered chan struct{}
	release chan struct{}

	mu      sync.Mutex
	results []store.CommitResult
}

func newExternalWaitCommitBarrierStore(durable store.Store) *externalWaitCommitBarrierStore {
	return &externalWaitCommitBarrierStore{
		Store: durable, entered: make(chan struct{}, 2), release: make(chan struct{}),
	}
}

func (barrier *externalWaitCommitBarrierStore) Commit(ctx context.Context, transaction store.Transaction) (store.CommitResult, error) {
	isResolution := false
	for _, mutation := range transaction.Mutations {
		if mutation.Key.Type == externalWaitResolutionType {
			isResolution = true
			break
		}
	}
	if !isResolution {
		return barrier.Store.Commit(ctx, transaction)
	}
	barrier.entered <- struct{}{}
	select {
	case <-barrier.release:
	case <-ctx.Done():
		return store.CommitResult{}, ctx.Err()
	}
	result, err := barrier.Store.Commit(ctx, transaction)
	if err == nil {
		barrier.mu.Lock()
		barrier.results = append(barrier.results, result)
		barrier.mu.Unlock()
	}
	return result, err
}

func (barrier *externalWaitCommitBarrierStore) resolutionResults() []store.CommitResult {
	barrier.mu.Lock()
	defer barrier.mu.Unlock()
	return append([]store.CommitResult(nil), barrier.results...)
}

func TestCoordinatorLeavesExternalWaitUntilExactSignalResolution(t *testing.T) {
	path := t.TempDir() + "/signals.db"
	durable, err := filestore.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	closed := false
	t.Cleanup(func() {
		if !closed {
			_ = durable.Close()
		}
	})
	barrier := newExternalWaitCommitBarrierStore(durable)
	executor := newSignalWaitExecutor()
	registry := protocol.NewRegistry()
	if err := registry.Register(executor); err != nil {
		t.Fatal(err)
	}
	if _, err := registry.Seal(); err != nil {
		t.Fatal(err)
	}
	clock := &fakeClock{now: time.Date(2026, 8, 13, 10, 0, 0, 0, time.UTC)}
	orchestrator, err := New(Config{
		Store: barrier, Nodes: registry, OwnerRef: "signal-controller",
		Clock: clock, LeaseDuration: 2 * time.Hour, InvocationTimeout: time.Hour,
	})
	if err != nil {
		t.Fatal(err)
	}
	coordinator, err := NewCoordinator(CoordinatorConfig{
		Controller: orchestrator, Store: barrier, OwnerRef: "signal-coordinator", Clock: clock,
	})
	if err != nil {
		t.Fatal(err)
	}
	definition, action := signalWaitAction(t, executor.Descriptor(), "hold")
	trigger := contracts.TriggerEvent{
		EventID: "signal-start-event", Kind: contracts.TriggerManual, Version: "v1",
		OccurredAt: clock.Now(), ReceivedAt: clock.Now(), SourceRef: "signal-test", ActorRef: "operator",
		PayloadSchemaDigest: testPackageDigest, Payload: map[string]any{},
	}
	invoked, err := orchestrator.Invoke(t.Context(), InvokeRequest{
		RunID: "signal-run", NamespaceID: "test", Action: action, Definition: definition,
		Trigger: trigger, Candidate: map[string]any{}, CandidateOrigin: contracts.OriginCaller,
		CandidateRef: "test", Scope: map[string]any{}, CommandID: "invoke-signal-run",
	})
	if err != nil {
		t.Fatal(err)
	}
	waiting, err := coordinator.AdvanceRun(t.Context(), invoked.Run.RunID)
	if !errors.Is(err, ErrRunWaiting) || waiting.Status != contracts.RunWaiting {
		t.Fatalf("external wait run = %+v err=%v", waiting, err)
	}
	resolution := currentSignalResolution(t, orchestrator, waiting.RunID, "resolve-exact-signal", clock.Now().Add(time.Second))
	wrong := resolution
	wrong.SubjectRef = "wrong-signal"
	wrong.Payload = map[string]any{"signalId": "wrong-signal"}
	wrong.CommandID = "resolve-wrong-signal"
	if _, err := orchestrator.ResolveExternalWait(t.Context(), wrong); err == nil {
		t.Fatal("mismatched signal resolution was accepted")
	}
	type outcome struct {
		run contracts.Run
		err error
	}
	gate := make(chan struct{})
	outcomes := make(chan outcome, 2)
	for range 2 {
		go func() {
			<-gate
			run, resolveErr := orchestrator.ResolveExternalWait(t.Context(), resolution)
			outcomes <- outcome{run: run, err: resolveErr}
		}()
	}
	close(gate)
	released := false
	defer func() {
		if !released {
			close(barrier.release)
		}
	}()
	for range 2 {
		select {
		case <-barrier.entered:
		case <-time.After(5 * time.Second):
			t.Fatal("concurrent resolutions did not both freeze before commit")
		}
	}
	close(barrier.release)
	released = true
	for range 2 {
		result := <-outcomes
		if result.err != nil || result.run.RunID != waiting.RunID {
			t.Fatalf("concurrent external wait = %+v err=%v", result.run, result.err)
		}
	}
	completed, err := orchestrator.GetRun(t.Context(), waiting.RunID)
	if err != nil || completed.Status != contracts.RunSucceeded {
		t.Fatalf("resolved external wait = %+v err=%v", completed, err)
	}
	commitResults := barrier.resolutionResults()
	if len(commitResults) != 2 {
		t.Fatalf("resolution commits = %d, want 2", len(commitResults))
	}
	replayed := 0
	for _, result := range commitResults {
		if result.Replay {
			replayed++
		}
	}
	if replayed != 1 {
		t.Fatalf("replayed resolution commits = %d, want exactly 1", replayed)
	}
	assertOneSnapshotResolutionEvent(t, durable, waiting.RunID, resolution.CommandID)

	terminalReplay, err := orchestrator.ResolveExternalWait(t.Context(), resolution)
	if err != nil || terminalReplay.RunID != completed.RunID || terminalReplay.Status != contracts.RunSucceeded {
		t.Fatalf("terminal resolution replay = %+v err=%v", terminalReplay, err)
	}
	identityConflicts := []struct {
		name   string
		mutate func(*ResolveExternalWaitRequest)
	}{
		{name: "run", mutate: func(value *ResolveExternalWaitRequest) { value.RunID = "other-signal-run" }},
		{name: "invocation", mutate: func(value *ResolveExternalWaitRequest) { value.InvocationID = "inv-other-signal" }},
		{name: "generation", mutate: func(value *ResolveExternalWaitRequest) { value.WaitGeneration++ }},
		{name: "condition", mutate: func(value *ResolveExternalWaitRequest) { value.ConditionDigest = testPackageDigest }},
		{name: "payload", mutate: func(value *ResolveExternalWaitRequest) { value.Payload = map[string]any{"signalId": "changed"} }},
		{name: "status", mutate: func(value *ResolveExternalWaitRequest) { value.Status = contracts.NodeWaitResolvedCanceled }},
		{name: "time", mutate: func(value *ResolveExternalWaitRequest) { value.ObservedAt = value.ObservedAt.Add(time.Second) }},
		{name: "subject", mutate: func(value *ResolveExternalWaitRequest) { value.SubjectRef = "other-signal" }},
		{name: "artifact", mutate: func(value *ResolveExternalWaitRequest) { value.PayloadArtifactRef = "other-artifact" }},
		{name: "failure", mutate: func(value *ResolveExternalWaitRequest) {
			value.Failure = &contracts.StructuredFailure{Class: contracts.FailurePermanent, Code: "signal.changed", Message: "changed"}
		}},
	}
	for _, candidate := range identityConflicts {
		t.Run("same-command-different-"+candidate.name, func(t *testing.T) {
			changed := resolution
			candidate.mutate(&changed)
			if _, err := orchestrator.ResolveExternalWait(t.Context(), changed); !errors.Is(err, store.ErrIdentityConflict) {
				t.Fatalf("changed resolution error = %v, want identity conflict", err)
			}
		})
	}
	newCommand := resolution
	newCommand.CommandID = "resolve-consumed-signal"
	if _, err := orchestrator.ResolveExternalWait(t.Context(), newCommand); !errors.Is(err, ErrExternalWaitConsumed) {
		t.Fatalf("new terminal command error = %v, want consumed occurrence", err)
	}
	unseenTerminal := resolution
	unseenTerminal.InvocationID = "inv-unseen-terminal-wait"
	unseenTerminal.CommandID = "resolve-unseen-terminal-wait"
	if _, err := orchestrator.ResolveExternalWait(t.Context(), unseenTerminal); !errors.Is(err, ErrExternalWaitConsumed) {
		t.Fatalf("unseen terminal command error = %v, want terminal state error", err)
	}

	if err := durable.Close(); err != nil {
		t.Fatal(err)
	}
	closed = true
	recovered, err := filestore.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = recovered.Close() })
	recoveredController, err := New(Config{
		Store: recovered, Nodes: registry, OwnerRef: "signal-controller",
		Clock: clock, LeaseDuration: 2 * time.Hour, InvocationTimeout: time.Hour,
	})
	if err != nil {
		t.Fatal(err)
	}
	restartedReplay, err := recoveredController.ResolveExternalWait(t.Context(), resolution)
	if err != nil || restartedReplay.RunID != completed.RunID || restartedReplay.Status != contracts.RunSucceeded {
		t.Fatalf("restarted exact replay = %+v err=%v", restartedReplay, err)
	}
	assertOneSnapshotResolutionEvent(t, recovered, waiting.RunID, resolution.CommandID)
}

func TestOldExternalWaitOccurrenceCannotUnlockSameNamedNewWait(t *testing.T) {
	durable, err := filestore.Open(t.TempDir() + "/repeated-signals.db")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = durable.Close() })
	executor := newSignalWaitExecutor()
	registry := protocol.NewRegistry()
	if err := registry.Register(executor); err != nil {
		t.Fatal(err)
	}
	if _, err := registry.Seal(); err != nil {
		t.Fatal(err)
	}
	clock := &fakeClock{now: time.Date(2026, 8, 13, 11, 0, 0, 0, time.UTC)}
	orchestrator, err := New(Config{
		Store: durable, Nodes: registry, OwnerRef: "signal-controller",
		Clock: clock, LeaseDuration: 2 * time.Hour, InvocationTimeout: time.Hour,
	})
	if err != nil {
		t.Fatal(err)
	}
	coordinator, err := NewCoordinator(CoordinatorConfig{
		Controller: orchestrator, Store: durable, OwnerRef: "signal-coordinator", Clock: clock,
	})
	if err != nil {
		t.Fatal(err)
	}
	definition, action := signalWaitAction(t, executor.Descriptor(), "hold-one", "hold-two")
	trigger := contracts.TriggerEvent{
		EventID: "repeat-signal-start", Kind: contracts.TriggerManual, Version: "v1",
		OccurredAt: clock.Now(), ReceivedAt: clock.Now(), SourceRef: "signal-test", ActorRef: "operator",
		PayloadSchemaDigest: testPackageDigest, Payload: map[string]any{},
	}
	invoked, err := orchestrator.Invoke(t.Context(), InvokeRequest{
		RunID: "repeat-signal-run", NamespaceID: "test", Action: action, Definition: definition,
		Trigger: trigger, Candidate: map[string]any{}, CandidateOrigin: contracts.OriginCaller,
		CandidateRef: "test", Scope: map[string]any{}, CommandID: "invoke-repeat-signal-run",
	})
	if err != nil {
		t.Fatal(err)
	}
	waiting, err := coordinator.AdvanceRun(t.Context(), invoked.Run.RunID)
	if !errors.Is(err, ErrRunWaiting) || waiting.Status != contracts.RunWaiting {
		t.Fatalf("first repeated wait = %+v err=%v", waiting, err)
	}
	first := currentSignalResolution(t, orchestrator, waiting.RunID, "resolve-first-occurrence", clock.Now().Add(time.Second))
	clock.now = first.ObservedAt
	if _, err := orchestrator.ResolveExternalWait(t.Context(), first); err != nil {
		t.Fatal(err)
	}
	secondSnapshot, secondRevision, err := orchestrator.GetSnapshot(t.Context(), waiting.RunID)
	if err != nil || secondSnapshot.NextNode != 1 || secondSnapshot.Waiting == nil || secondSnapshot.Waiting.Wait == nil {
		t.Fatalf("second repeated wait snapshot = %+v revision=%d err=%v", secondSnapshot, secondRevision, err)
	}
	if secondSnapshot.Waiting.Wait.SubjectRef != first.SubjectRef || secondSnapshot.Waiting.Wait.ConditionDigest != first.ConditionDigest {
		t.Fatal("fixture did not produce a same-named second wait")
	}
	if replayed, err := orchestrator.ResolveExternalWait(t.Context(), first); err != nil || replayed.Status != contracts.RunWaiting {
		t.Fatalf("old exact replay at new wait = %+v err=%v", replayed, err)
	}
	staleNewCommand := first
	staleNewCommand.CommandID = "resolve-stale-first-occurrence"
	if _, err := orchestrator.ResolveExternalWait(t.Context(), staleNewCommand); !errors.Is(err, ErrExternalWaitConsumed) {
		t.Fatalf("stale occurrence command error = %v, want consumed", err)
	}
	afterStale, afterStaleRevision, err := orchestrator.GetSnapshot(t.Context(), waiting.RunID)
	if err != nil || afterStaleRevision != secondRevision || afterStale.NextNode != secondSnapshot.NextNode {
		t.Fatalf("stale occurrence changed second wait: revision=%d snapshot=%+v err=%v", afterStaleRevision, afterStale, err)
	}
	wrongOccurrence := first
	wrongOccurrence.CommandID = "resolve-wrong-second-occurrence"
	wrongOccurrence.InvocationID = "inv-wrong-occurrence"
	if _, err := orchestrator.ResolveExternalWait(t.Context(), wrongOccurrence); err == nil ||
		errors.Is(err, ErrExternalWaitConsumed) || errors.Is(err, store.ErrIdentityConflict) {
		t.Fatalf("wrong waiting occurrence error = %v, want exact durable wait rejection", err)
	}
	second := currentSignalResolution(t, orchestrator, waiting.RunID, "resolve-second-occurrence", clock.Now().Add(2*time.Second))
	if second.InvocationID == first.InvocationID {
		t.Fatal("repeated waits did not have distinct durable occurrences")
	}
	clock.now = second.ObservedAt
	completed, err := orchestrator.ResolveExternalWait(t.Context(), second)
	if err != nil || completed.Status != contracts.RunSucceeded {
		t.Fatalf("second occurrence resolution = %+v err=%v", completed, err)
	}
}

func currentSignalResolution(
	t *testing.T, orchestrator *Controller, runID, commandID string, observedAt time.Time,
) ResolveExternalWaitRequest {
	t.Helper()
	snapshot, _, err := orchestrator.GetSnapshot(t.Context(), runID)
	if err != nil || snapshot.Waiting == nil || snapshot.Waiting.Wait == nil ||
		snapshot.NextNode < 0 || snapshot.NextNode >= len(snapshot.NodeOrder) {
		t.Fatalf("external wait snapshot = %+v err=%v", snapshot, err)
	}
	invocationID, err := execution.StableInvocationID(runID, snapshot.NodeOrder[snapshot.NextNode])
	if err != nil {
		t.Fatal(err)
	}
	record, err := orchestrator.store.GetAggregate(t.Context(), store.AggregateKey{Type: invocationType, ID: invocationID})
	if err != nil {
		t.Fatal(err)
	}
	ledger, err := decodeLedger(record)
	if err != nil {
		t.Fatal(err)
	}
	wait := *snapshot.Waiting.Wait
	return ResolveExternalWaitRequest{
		RunID: runID, InvocationID: invocationID, WaitGeneration: ledger.Invocation.WaitGeneration,
		SubjectRef: wait.SubjectRef, ConditionDigest: wait.ConditionDigest,
		Status: contracts.NodeWaitResolvedSucceeded, Payload: map[string]any{"signalId": wait.SubjectRef},
		ObservedAt: observedAt, CommandID: commandID,
	}
}

func assertOneSnapshotResolutionEvent(t *testing.T, durable store.Store, runID, commandID string) {
	t.Helper()
	events, err := durable.EventsAfter(t.Context(), snapshotKey(runID), 0, 100)
	if err != nil {
		t.Fatal(err)
	}
	count := 0
	for _, event := range events {
		if event.CommandID == commandID && (event.Type == "snapshot.node-resumed" || event.Type == "snapshot.node-failed") {
			count++
		}
	}
	if count != 1 {
		t.Fatalf("snapshot resolution events = %d, want exactly 1", count)
	}
}

func signalWaitAction(t *testing.T, descriptor contracts.NodeDescriptor, nodeIDs ...string) (contracts.WorkflowDefinition, contracts.ActionVersion) {
	t.Helper()
	if len(nodeIDs) == 0 {
		t.Fatal("signal wait action requires at least one node")
	}
	empty := contracts.Schema{Type: contracts.TypeObject, Properties: map[string]contracts.Schema{}}
	nodes := make([]contracts.WorkflowNodeDefinition, 0, len(nodeIDs))
	edges := make([]contracts.WorkflowEdge, 0, len(nodeIDs)-1)
	for index, nodeID := range nodeIDs {
		nodes = append(nodes, contracts.WorkflowNodeDefinition{
			NodeID: nodeID, TypeRef: descriptor.TypeRef, DescriptorDigest: descriptor.DescriptorDigest,
			InputSchema: descriptor.InputSchema, OutputSchema: descriptor.OutputSchema,
			FixedInputs: map[string]any{"signalId": "stop-signal"},
		})
		if index > 0 {
			edges = append(edges, contracts.WorkflowEdge{From: nodeIDs[index-1], To: nodeID, Kind: contracts.EdgeControl})
		}
	}
	definition := contracts.WorkflowDefinition{
		SchemaVersion: workflowkernel.SchemaVersion, WorkflowID: "signal-action", Version: "v1",
		InputSchema: empty, ResultSchema: descriptor.OutputSchema, TriggerSchema: empty, ScopeSchema: empty,
		Entrypoints: map[string]string{"main": nodeIDs[0]}, Nodes: nodes, Edges: edges,
		ResultBindings: map[string][]contracts.ValueBinding{"main": {{
			Target: "/signalId", Value: contracts.ValueExpr{Ref: "nodes." + nodeIDs[len(nodeIDs)-1] + ".output.signalId"},
		}}},
	}
	plan, err := workflowkernel.Compile(definition)
	if err != nil {
		t.Fatal(err)
	}
	return definition, contracts.ActionVersion{
		ActionID: definition.WorkflowID, Version: definition.Version, DefinitionDigest: plan.DefinitionDigest,
		Entrypoint: "main", InputSchema: definition.InputSchema, ResultSchema: definition.ResultSchema,
		AcceptedTriggerKinds: []contracts.TriggerKind{contracts.TriggerManual},
	}
}

func timePointerForWait(value time.Time) *time.Time { return &value }
