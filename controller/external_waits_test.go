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
		PayloadSchemaDigest: mustTriggerSchemaDigest(t, definition.TriggerSchema), Payload: map[string]any{},
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
		result ResolveExternalWaitResult
		err    error
	}
	gate := make(chan struct{})
	outcomes := make(chan outcome, 2)
	for range 2 {
		go func() {
			<-gate
			result, resolveErr := orchestrator.ResolveExternalWaitResult(t.Context(), resolution)
			outcomes <- outcome{result: result, err: resolveErr}
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
	apiReplays := 0
	for range 2 {
		result := <-outcomes
		if result.err != nil || result.result.Run.RunID != waiting.RunID {
			t.Fatalf("concurrent external wait = %+v err=%v", result.result, result.err)
		}
		if result.result.Replay {
			apiReplays++
		}
	}
	if apiReplays != 1 {
		t.Fatalf("replay-aware concurrent results = %d, want exactly 1", apiReplays)
	}
	completed, err := orchestrator.GetRun(t.Context(), waiting.RunID)
	if err != nil || completed.Status != contracts.RunSucceeded {
		t.Fatalf("resolved external wait = %+v err=%v", completed, err)
	}
	completedSnapshot, _, err := orchestrator.GetSnapshot(t.Context(), waiting.RunID)
	if err != nil || completedSnapshot.Waiting != nil {
		t.Fatalf("terminal external wait Snapshot = %+v err=%v", completedSnapshot, err)
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

	terminalReplay, err := orchestrator.ResolveExternalWaitResult(t.Context(), resolution)
	if err != nil || !terminalReplay.Replay || terminalReplay.Run.RunID != completed.RunID || terminalReplay.Run.Status != contracts.RunSucceeded {
		t.Fatalf("terminal resolution replay = %+v err=%v", terminalReplay, err)
	}
	identityConflicts := []struct {
		name   string
		mutate func(*ResolveExternalWaitRequest)
	}{
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
	differentRun := resolution
	differentRun.RunID = "other-signal-run"
	if _, err := orchestrator.ResolveExternalWait(t.Context(), differentRun); !errors.Is(err, store.ErrNotFound) {
		t.Fatalf("different Run scope error = %v, want not found", err)
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
	restartedReplay, err := recoveredController.ResolveExternalWaitResult(t.Context(), resolution)
	if err != nil || !restartedReplay.Replay || restartedReplay.Run.RunID != completed.RunID || restartedReplay.Run.Status != contracts.RunSucceeded {
		t.Fatalf("restarted exact replay = %+v err=%v", restartedReplay, err)
	}
	assertOneSnapshotResolutionEvent(t, recovered, waiting.RunID, resolution.CommandID)
}

func TestActiveOwnerExternalWaitRequiresExactKeyAndReplaysConcurrentCommand(t *testing.T) {
	durable, err := filestore.Open(t.TempDir() + "/active-signals.db")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = durable.Close() })
	barrier := newExternalWaitCommitBarrierStore(durable)
	executor := newSignalWaitExecutor()
	registry := protocol.NewRegistry()
	if err := registry.Register(executor); err != nil {
		t.Fatal(err)
	}
	if _, err := registry.Seal(); err != nil {
		t.Fatal(err)
	}
	policy, permit, err := NewReservedIngressPolicy(ReservedIngressPolicySpec{
		PolicyRef: "signal-builder-v1", NamespaceID: "xgc2-experiments",
		TriggerKind: contracts.TriggerProductBuilder, TriggerVersion: "v1", SourceRef: "xgc2-experiment-builder",
		CandidateOrigin: contracts.OriginProductBuilder, RootOnly: true, RequireActiveOwner: true,
		ActiveOwnerKind: experimentOwnerKind, ActiveOwnerIdentityFields: []string{"branch", "domain", "resourceId"},
	})
	if err != nil {
		t.Fatal(err)
	}
	clock := &fakeClock{now: time.Date(2026, 8, 13, 12, 0, 0, 0, time.UTC)}
	orchestrator, err := New(Config{
		Store: barrier, Nodes: registry, OwnerRef: "active-signal-controller",
		Clock: clock, LeaseDuration: 2 * time.Hour, InvocationTimeout: time.Hour,
		ReservedIngressPolicies: []*ReservedIngressPolicy{policy},
	})
	if err != nil {
		t.Fatal(err)
	}
	coordinator, err := NewCoordinator(CoordinatorConfig{
		Controller: orchestrator, Store: barrier, OwnerRef: "active-signal-coordinator", Clock: clock,
	})
	if err != nil {
		t.Fatal(err)
	}
	definition, action := signalWaitAction(t, executor.Descriptor(), "hold")
	action.AcceptedTriggerKinds = []contracts.TriggerKind{contracts.TriggerProductBuilder}
	key := contracts.ActiveOwnerKey{
		NamespaceID: "xgc2-experiments", Kind: experimentOwnerKind,
		Identity: map[string]string{"domain": "default", "resourceId": "experiment-signal", "branch": "main"},
	}
	invoked, err := orchestrator.Invoke(t.Context(), InvokeRequest{
		RunID: "active-signal-run", NamespaceID: key.NamespaceID, Action: action, Definition: definition,
		Trigger: contracts.TriggerEvent{
			EventID: "active-signal-start", Kind: contracts.TriggerProductBuilder, Version: "v1",
			OccurredAt: clock.Now(), ReceivedAt: clock.Now(), SourceRef: "xgc2-experiment-builder", ActorRef: "operator",
			PayloadSchemaDigest: mustTriggerSchemaDigest(t, definition.TriggerSchema), Payload: map[string]any{},
		},
		Candidate: map[string]any{}, CandidateOrigin: contracts.OriginProductBuilder,
		CandidateRef: "experiment-signal-main", Scope: map[string]any{}, CommandID: "invoke-active-signal",
		IngressPermit: permit, ActiveOwnerKey: &key,
	})
	if err != nil {
		t.Fatal(err)
	}
	waiting, err := coordinator.AdvanceRun(t.Context(), invoked.Run.RunID)
	if !errors.Is(err, ErrRunWaiting) || waiting.Status != contracts.RunWaiting {
		t.Fatalf("active external wait Run = %+v err=%v", waiting, err)
	}
	resolution := currentSignalResolution(
		t, orchestrator, waiting.RunID, "resolve-active-signal", clock.Now().Add(time.Second),
	)
	before, beforeRevision, err := orchestrator.GetSnapshot(t.Context(), waiting.RunID)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := orchestrator.ResolveExternalWait(t.Context(), resolution); !errors.Is(err, ErrReservedIngressDenied) {
		t.Fatalf("generic owner-backed resolution error = %v", err)
	}
	foreignKey := key
	foreignKey.Identity = map[string]string{"domain": "default", "resourceId": "experiment-signal", "branch": "foreign"}
	if _, err := orchestrator.ResolveActiveExternalWait(t.Context(), foreignKey, resolution); !errors.Is(err, ErrReservedIngressDenied) {
		t.Fatalf("foreign owner key resolution error = %v", err)
	}
	manualAction := action
	manualAction.AcceptedTriggerKinds = []contracts.TriggerKind{contracts.TriggerManual}
	manualInvoked, err := orchestrator.Invoke(t.Context(), InvokeRequest{
		RunID: "foreign-signal-run", NamespaceID: "test", Action: manualAction, Definition: definition,
		Trigger: contracts.TriggerEvent{
			EventID: "foreign-signal-start", Kind: contracts.TriggerManual, Version: "v1",
			OccurredAt: clock.Now(), ReceivedAt: clock.Now(), SourceRef: "signal-test", ActorRef: "operator",
			PayloadSchemaDigest: mustTriggerSchemaDigest(t, definition.TriggerSchema), Payload: map[string]any{},
		},
		Candidate: map[string]any{}, CandidateOrigin: contracts.OriginCaller,
		CandidateRef: "foreign-signal", Scope: map[string]any{}, CommandID: "invoke-foreign-signal",
	})
	if err != nil {
		t.Fatal(err)
	}
	foreignWaiting, err := coordinator.AdvanceRun(t.Context(), manualInvoked.Run.RunID)
	if !errors.Is(err, ErrRunWaiting) || foreignWaiting.Status != contracts.RunWaiting {
		t.Fatalf("foreign external wait Run = %+v err=%v", foreignWaiting, err)
	}
	foreignResolution := currentSignalResolution(
		t, orchestrator, foreignWaiting.RunID, "resolve-foreign-run", clock.Now().Add(time.Second),
	)
	if _, err := orchestrator.ResolveActiveExternalWait(t.Context(), key, foreignResolution); !errors.Is(err, ErrReservedIngressDenied) {
		t.Fatalf("active key with foreign Run ID error = %v", err)
	}
	foreignAfter, err := orchestrator.GetRun(t.Context(), foreignWaiting.RunID)
	if err != nil || foreignAfter.Status != contracts.RunWaiting {
		t.Fatalf("foreign Run changed after denied active resolution: %+v err=%v", foreignAfter, err)
	}
	afterDenied, afterDeniedRevision, err := orchestrator.GetSnapshot(t.Context(), waiting.RunID)
	if err != nil || afterDeniedRevision != beforeRevision || afterDenied.NextNode != before.NextNode ||
		len(barrier.resolutionResults()) != 0 {
		t.Fatalf("denied ingress changed wait: before=%+v/%d after=%+v/%d commits=%d err=%v",
			before, beforeRevision, afterDenied, afterDeniedRevision, len(barrier.resolutionResults()), err)
	}

	type outcome struct {
		result ResolveExternalWaitResult
		err    error
	}
	gate := make(chan struct{})
	outcomes := make(chan outcome, 2)
	for range 2 {
		go func() {
			<-gate
			result, resolveErr := orchestrator.ResolveActiveExternalWaitResult(t.Context(), key, resolution)
			outcomes <- outcome{result: result, err: resolveErr}
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
			t.Fatal("concurrent active resolutions did not both freeze before commit")
		}
	}
	close(barrier.release)
	released = true
	apiReplays := 0
	for range 2 {
		result := <-outcomes
		if result.err != nil || result.result.Run.RunID != waiting.RunID || result.result.Run.Status != contracts.RunSucceeded {
			t.Fatalf("concurrent active external wait = %+v err=%v", result.result, result.err)
		}
		if result.result.Replay {
			apiReplays++
		}
	}
	if apiReplays != 1 {
		t.Fatalf("active replay-aware concurrent results = %d, want exactly 1", apiReplays)
	}
	commitResults := barrier.resolutionResults()
	if len(commitResults) != 2 {
		t.Fatalf("active resolution commits = %d, want 2", len(commitResults))
	}
	replayed := 0
	for _, result := range commitResults {
		if result.Replay {
			replayed++
		}
	}
	if replayed != 1 {
		t.Fatalf("active resolution replays = %d, want exactly 1", replayed)
	}
	owner, err := orchestrator.GetActiveRunOwner(t.Context(), key)
	if err != nil || owner.State != contracts.ActiveRunOwnerReleased || owner.RunID != waiting.RunID {
		t.Fatalf("terminal active owner = %+v err=%v", owner, err)
	}
	exactReplay, err := orchestrator.ResolveActiveExternalWaitResult(t.Context(), key, resolution)
	if err != nil || !exactReplay.Replay || exactReplay.Run.RunID != waiting.RunID || exactReplay.Run.Status != contracts.RunSucceeded {
		t.Fatalf("terminal active resolution replay = %+v err=%v", exactReplay, err)
	}
	if _, err := orchestrator.ResolveExternalWait(t.Context(), resolution); !errors.Is(err, ErrReservedIngressDenied) {
		t.Fatalf("generic terminal replay error = %v", err)
	}
	changed := resolution
	changed.Payload = map[string]any{"signalId": "changed"}
	if _, err := orchestrator.ResolveActiveExternalWait(t.Context(), key, changed); !errors.Is(err, store.ErrIdentityConflict) {
		t.Fatalf("changed active resolution error = %v, want identity conflict", err)
	}
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
		PayloadSchemaDigest: mustTriggerSchemaDigest(t, definition.TriggerSchema), Payload: map[string]any{},
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
	if err != nil || secondSnapshot.NextNode != 1 || secondSnapshot.Waiting == nil ||
		secondSnapshot.Waiting.Result.Wait == nil {
		t.Fatalf("second repeated wait snapshot = %+v revision=%d err=%v", secondSnapshot, secondRevision, err)
	}
	if secondSnapshot.Waiting.Result.Wait.SubjectRef != first.SubjectRef ||
		secondSnapshot.Waiting.Result.Wait.ConditionDigest != first.ConditionDigest {
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
	if err != nil || snapshot.Waiting == nil || snapshot.Waiting.Result.Wait == nil {
		t.Fatalf("external wait snapshot = %+v err=%v", snapshot, err)
	}
	wait := *snapshot.Waiting.Result.Wait
	return ResolveExternalWaitRequest{
		RunID: runID, InvocationID: snapshot.Waiting.InvocationID,
		WaitGeneration: snapshot.Waiting.WaitGeneration,
		SubjectRef:     wait.SubjectRef, ConditionDigest: wait.ConditionDigest,
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
