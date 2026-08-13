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
	"github.com/lxk36/xgc2-orchestration-core/kernel/canonicaljson"
	"github.com/lxk36/xgc2-orchestration-core/kernel/effect"
	"github.com/lxk36/xgc2-orchestration-core/kernel/execution"
	"github.com/lxk36/xgc2-orchestration-core/kernel/node"
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
}

func (executor *effectExecutor) Descriptor() contracts.NodeDescriptor { return executor.descriptor }

func (executor *effectExecutor) Execute(_ context.Context, request contracts.NodeInvocationRequest) (contracts.NodeResult, error) {
	executor.mu.Lock()
	executor.calls++
	executor.mu.Unlock()
	intent := map[string]any{"executableRef": "simulator"}
	intentDigest, _ := canonicaljson.DigestValue(intent)
	proposal := contracts.EffectProposal{
		EffectKey: "start-process", Kind: "xgc.process-start/v1", TargetRef: "simulator",
		IntentSchemaDigest: testPackageDigest, Intent: intent, IntentDigest: intentDigest,
		Ownership: contracts.EffectOwned, CompensationPolicy: contracts.CompensationRequired,
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
	descriptor.CompensationTypeRef = "xgc.test.effect-compensate/v1"
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
		controller: workflowController, store: durable, registry: registry, clock: clock,
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
