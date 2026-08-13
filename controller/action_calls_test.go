package controller

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/lxk36/xgc2-orchestration-core/durable/filestore"
	"github.com/lxk36/xgc2-orchestration-core/durable/worker"
	"github.com/lxk36/xgc2-orchestration-core/kernel/execution"
	"github.com/lxk36/xgc2-orchestration-core/kernel/node"
	workflowkernel "github.com/lxk36/xgc2-orchestration-core/kernel/workflow"
	"github.com/lxk36/xgc2-orchestration-core/sdk/go/contracts"
	nodesdk "github.com/lxk36/xgc2-orchestration-core/sdk/go/node"
)

type actionCatalogFixture struct {
	action     contracts.ActionVersion
	definition contracts.WorkflowDefinition
	err        error
}

func (catalog actionCatalogFixture) ResolveAction(_ context.Context, _ string, ref contracts.ActionRef) (contracts.ActionVersion, contracts.WorkflowDefinition, error) {
	if catalog.err != nil {
		return contracts.ActionVersion{}, contracts.WorkflowDefinition{}, catalog.err
	}
	if !catalog.action.Ref().Equal(ref) {
		return contracts.ActionVersion{}, contracts.WorkflowDefinition{}, errors.New("unknown exact Action pin")
	}
	return catalog.action, catalog.definition, nil
}

type unreachableCallExecutor struct {
	descriptor contracts.NodeDescriptor
	mu         sync.Mutex
	calls      int
}

func (executor *unreachableCallExecutor) Descriptor() contracts.NodeDescriptor {
	return executor.descriptor
}

func (executor *unreachableCallExecutor) Execute(context.Context, contracts.NodeInvocationRequest) (contracts.NodeResult, error) {
	executor.mu.Lock()
	executor.calls++
	executor.mu.Unlock()
	return contracts.NodeResult{}, errors.New("Action call executor must not run")
}

func (executor *unreachableCallExecutor) count() int {
	executor.mu.Lock()
	defer executor.mu.Unlock()
	return executor.calls
}

func TestControllerExecutesPinnedChildActionWithExplicitContextAndResultMaps(t *testing.T) {
	fixture := newActionCallFixture(t, nil)
	invoked, err := fixture.controller.Invoke(context.Background(), fixture.request)
	if err != nil {
		t.Fatal(err)
	}
	waiting, err := fixture.controller.Drive(context.Background(), invoked.Run.RunID)
	if !errors.Is(err, ErrRunWaiting) || waiting.Status != contracts.RunWaiting {
		t.Fatalf("parent wait = %+v err=%v", waiting, err)
	}
	handler, err := NewChildResolutionHandler(fixture.controller)
	if err != nil {
		t.Fatal(err)
	}
	now := fixture.request.Trigger.OccurredAt
	batch, err := (worker.Worker{
		Store: fixture.controller.store, OwnerRef: "child-resolution-test",
		Handlers: map[contracts.DurableIntentKind]worker.Handler{contracts.IntentChildResolution: handler},
	}).RunOnce(context.Background(), worker.Batch{
		Kinds:      []contracts.DurableIntentKind{contracts.IntentChildResolution},
		LeaseToken: "child-resolution-lease", Now: now, LeaseExpiresAt: now.Add(time.Minute), Limit: 10,
	})
	if err != nil || batch.Claimed != 1 || batch.Completed != 1 {
		t.Fatalf("child resolution batch = %+v err=%v", batch, err)
	}
	completed, err := fixture.controller.GetRun(context.Background(), invoked.Run.RunID)
	if err != nil || completed.Status != contracts.RunSucceeded {
		t.Fatalf("parent completion = %+v failure=%+v err=%v", completed, completed.PrimaryFailure, err)
	}
	if fixture.callExecutor.count() != 0 {
		t.Fatalf("extension executor ran %d times for a kernel-owned child call", fixture.callExecutor.count())
	}
	parentSnapshot, _, err := fixture.controller.GetSnapshot(context.Background(), completed.RunID)
	if err != nil || parentSnapshot.ActionCall != nil || parentSnapshot.Result["text"] != "HELLO ADA" {
		t.Fatalf("parent snapshot = %+v err=%v", parentSnapshot, err)
	}
	invocationID, _ := execution.StableInvocationID(completed.RunID, "call-child")
	childRunID, _ := execution.StableChildRunID(invocationID, fixture.childAction.Ref())
	child, err := fixture.controller.GetRun(context.Background(), childRunID)
	if err != nil || child.Status != contracts.RunSucceeded || child.Parent == nil ||
		child.Parent.ParentRunID != completed.RunID || child.Parent.ParentInvocationID != invocationID ||
		child.Parent.CallNodeID != "call-child" || child.RootRunID != completed.RunID {
		t.Fatalf("child lineage = %+v err=%v", child, err)
	}
	graph, err := fixture.controller.OwnershipGraph(context.Background(), completed.RunID)
	if err != nil || len(graph.ChildRuns) != 1 || graph.ChildRuns[0].RunID != childRunID {
		t.Fatalf("parent ownership graph = %+v err=%v", graph, err)
	}
}

func TestControllerFailsClosedWhenChildActionCannotResolve(t *testing.T) {
	fixture := newActionCallFixture(t, errors.New("catalog unavailable"))
	invoked, err := fixture.controller.Invoke(context.Background(), fixture.request)
	if err != nil {
		t.Fatal(err)
	}
	completed, err := fixture.controller.Drive(context.Background(), invoked.Run.RunID)
	if err != nil || completed.Status != contracts.RunFailed || completed.PrimaryFailure == nil ||
		completed.PrimaryFailure.Code != "action-call.resolve" || !strings.Contains(completed.PrimaryFailure.Message, "catalog unavailable") {
		t.Fatalf("failed parent = %+v err=%v", completed, err)
	}
	runs, err := fixture.controller.ListRuns(context.Background(), "", 100)
	if err != nil || len(runs) != 1 {
		t.Fatalf("unresolved call created child Runs: %+v err=%v", runs, err)
	}
}

func TestCoordinatorAdvancesEffectfulChildActionBeforeJoiningParent(t *testing.T) {
	fixture := newActionCallFixture(t, nil)
	empty := contracts.Schema{Type: contracts.TypeObject, Properties: map[string]contracts.Schema{}}
	effectDescriptor := descriptor(t, "xgc.test.child-effect/v1", empty, empty)
	effectDescriptor.Mode = contracts.NodeEffectful
	effectDescriptor.RequiredCapabilities = []contracts.CapabilityRequirement{{CapabilityRef: "process.control", Scope: "target"}}
	effectDescriptor.AllowedEffectKinds = []string{"xgc.process-start/v1"}
	effectDescriptor.DescriptorDigest = ""
	effectDescriptor.DescriptorDigest, _ = node.DescriptorDigest(effectDescriptor)
	effectExecutor := &effectExecutor{descriptor: effectDescriptor}
	callDescriptor := fixture.callExecutor.descriptor

	registry := node.NewRegistry()
	for _, executor := range []nodesdk.Executor{effectExecutor, fixture.callExecutor} {
		if err := registry.Register(executor); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := registry.Seal(); err != nil {
		t.Fatal(err)
	}
	childDefinition := contracts.WorkflowDefinition{
		SchemaVersion: workflowkernel.SchemaVersion, WorkflowID: "child.effect-workflow", Version: "v1",
		InputSchema: empty, ResultSchema: empty, TriggerSchema: empty, ScopeSchema: empty,
		Entrypoints: map[string]string{"main": "launch"},
		Nodes: []contracts.WorkflowNodeDefinition{{
			NodeID: "launch", TypeRef: effectDescriptor.TypeRef, DescriptorDigest: effectDescriptor.DescriptorDigest,
			InputSchema: empty, OutputSchema: empty,
		}},
		Edges:          []contracts.WorkflowEdge{},
		ResultBindings: map[string][]contracts.ValueBinding{"main": {}},
	}
	childPlan, err := workflowkernel.Compile(childDefinition)
	if err != nil {
		t.Fatal(err)
	}
	childAction := contracts.ActionVersion{
		ActionID: childDefinition.WorkflowID, Version: childDefinition.Version,
		DefinitionDigest: childPlan.DefinitionDigest, Entrypoint: "main",
		InputSchema: empty, ResultSchema: empty,
		AcceptedTriggerKinds: []contracts.TriggerKind{contracts.TriggerActionCall},
	}
	parentDefinition := fixture.request.Definition
	parentDefinition.ResultSchema = empty
	parentDefinition.Nodes[0].TypeRef = callDescriptor.TypeRef
	parentDefinition.Nodes[0].DescriptorDigest = callDescriptor.DescriptorDigest
	parentDefinition.Nodes[0].OutputSchema = empty
	parentDefinition.Nodes[0].CallAction = &contracts.CallAction{
		TargetActionRef: childAction.Ref(), InputSchema: empty, TriggerSchema: empty,
		ScopeSchema: empty, ResultSchema: empty,
		InputMap: []contracts.ValueBinding{}, TriggerMap: []contracts.ValueBinding{},
		ScopeMap: []contracts.ValueBinding{}, ResultMap: []contracts.ResultBinding{},
	}
	parentDefinition.ResultBindings = map[string][]contracts.ValueBinding{"main": {}}
	parentPlan, err := workflowkernel.Compile(parentDefinition)
	if err != nil {
		t.Fatal(err)
	}
	fixture.request.Definition = parentDefinition
	fixture.request.Action.DefinitionDigest = parentPlan.DefinitionDigest
	fixture.request.Action.ResultSchema = empty

	clock := fixture.controller.clock.(*fakeClock)
	grants := staticGrants{clock: clock}
	workflowController, err := New(Config{
		Store: fixture.controller.store, Nodes: registry, OwnerRef: "controller-child-effects",
		LeaseDuration: time.Minute, InvocationTimeout: 10 * time.Second, Clock: clock,
		Grants: grants, Actions: actionCatalogFixture{action: childAction, definition: childDefinition},
	})
	if err != nil {
		t.Fatal(err)
	}
	invoked, err := workflowController.Invoke(t.Context(), fixture.request)
	if err != nil {
		t.Fatal(err)
	}
	adapter := &successfulEffectAdapter{clock: clock}
	coordinator, err := NewCoordinator(CoordinatorConfig{
		Controller: workflowController, Store: fixture.controller.store,
		Planner: staticEffectPlan{clock: clock}, Credentials: staticEffectCredentials{
			idempotency: "coordinate-idempotency", capability: "coordinate-capability",
		},
		Adapters: []EffectAdapter{adapter}, OwnerRef: "child-effect-coordinator", Clock: clock,
	})
	if err != nil {
		t.Fatal(err)
	}
	completed, err := coordinator.AdvanceRun(t.Context(), invoked.Run.RunID)
	if err != nil || completed.Status != contracts.RunSucceeded || effectExecutor.Calls() != 1 || !adapter.seenPrivate {
		t.Fatalf("effectful child parent = %+v failure=%+v calls=%d private=%v err=%v", completed, completed.PrimaryFailure, effectExecutor.Calls(), adapter.seenPrivate, err)
	}
	invocationID, _ := execution.StableInvocationID(completed.RunID, "call-child")
	childRunID, _ := execution.StableChildRunID(invocationID, childAction.Ref())
	child, err := workflowController.GetRun(t.Context(), childRunID)
	if err != nil || child.Status != contracts.RunSucceeded {
		t.Fatalf("effectful child = %+v err=%v", child, err)
	}
}

type actionCallFixture struct {
	controller   *Controller
	callExecutor *unreachableCallExecutor
	childAction  contracts.ActionVersion
	request      InvokeRequest
}

func newActionCallFixture(t *testing.T, catalogErr error) actionCallFixture {
	t.Helper()
	stringSchema := contracts.Schema{Type: contracts.TypeString}
	empty := contracts.Schema{Type: contracts.TypeObject, Properties: map[string]contracts.Schema{}}
	parentInput := contracts.Schema{Type: contracts.TypeObject, Properties: map[string]contracts.Schema{"name": stringSchema}, Required: []string{"name"}}
	parentTrigger := contracts.Schema{Type: contracts.TypeObject, Properties: map[string]contracts.Schema{"origin": stringSchema}, Required: []string{"origin"}}
	parentScope := contracts.Schema{Type: contracts.TypeObject, Properties: map[string]contracts.Schema{"site": stringSchema}, Required: []string{"site"}}
	childInput := contracts.Schema{Type: contracts.TypeObject, Properties: map[string]contracts.Schema{"message": stringSchema}, Required: []string{"message"}}
	childTrigger := contracts.Schema{Type: contracts.TypeObject, Properties: map[string]contracts.Schema{"origin": stringSchema}, Required: []string{"origin"}}
	childScope := contracts.Schema{Type: contracts.TypeObject, Properties: map[string]contracts.Schema{"site": stringSchema}, Required: []string{"site"}}
	textOutput := contracts.Schema{Type: contracts.TypeObject, Properties: map[string]contracts.Schema{"text": stringSchema}, Required: []string{"text"}}

	render := &countExecutor{descriptor: descriptor(t, "xgc.test.child-render/v1", childInput, textOutput), function: func(input map[string]any) map[string]any {
		return map[string]any{"text": strings.ToUpper(input["message"].(string))}
	}}
	childDefinition := contracts.WorkflowDefinition{
		SchemaVersion: workflowkernel.SchemaVersion, WorkflowID: "child.workflow", Version: "v1",
		InputSchema: childInput, ResultSchema: textOutput, TriggerSchema: childTrigger, ScopeSchema: childScope,
		Entrypoints: map[string]string{"main": "render"},
		Nodes: []contracts.WorkflowNodeDefinition{{
			NodeID: "render", TypeRef: render.descriptor.TypeRef, DescriptorDigest: render.descriptor.DescriptorDigest,
			InputSchema: childInput, OutputSchema: textOutput,
			Bindings: []contracts.ValueBinding{{Target: "/message", Value: contracts.ValueExpr{Ref: "inputs.message"}}},
		}},
		Edges:          []contracts.WorkflowEdge{},
		ResultBindings: map[string][]contracts.ValueBinding{"main": {{Target: "/text", Value: contracts.ValueExpr{Ref: "nodes.render.output.text"}}}},
	}
	childPlan, err := workflowkernel.Compile(childDefinition)
	if err != nil {
		t.Fatal(err)
	}
	childAction := contracts.ActionVersion{
		ActionID: childDefinition.WorkflowID, Version: childDefinition.Version, DefinitionDigest: childPlan.DefinitionDigest,
		Entrypoint: "main", InputSchema: childInput, ResultSchema: textOutput,
		AcceptedTriggerKinds: []contracts.TriggerKind{contracts.TriggerActionCall},
	}

	callDescriptor := descriptor(t, "xgc.test.action-call/v1", empty, empty)
	callDescriptor.Mode = contracts.NodeWaiting
	callDescriptor.Determinism = contracts.NodeRecorded
	callDescriptor.SchemaMode = contracts.NodeSchemaCallAction
	callDescriptor.DescriptorDigest = ""
	callDescriptor.DescriptorDigest, err = node.DescriptorDigest(callDescriptor)
	if err != nil {
		t.Fatal(err)
	}
	callExecutor := &unreachableCallExecutor{descriptor: callDescriptor}
	registry := node.NewRegistry()
	for _, executor := range []nodesdk.Executor{render, callExecutor} {
		if err := registry.Register(executor); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := registry.Seal(); err != nil {
		t.Fatal(err)
	}
	hello := json.RawMessage(`"hello "`)
	call := contracts.CallAction{
		TargetActionRef: childAction.Ref(), InputSchema: childInput, TriggerSchema: childTrigger,
		ScopeSchema: childScope, ResultSchema: textOutput,
		InputMap: []contracts.ValueBinding{{Target: "/message", Value: contracts.ValueExpr{Op: "concat", Args: []contracts.ValueExpr{
			{Literal: &hello}, {Ref: "inputs.name"},
		}}}},
		TriggerMap: []contracts.ValueBinding{{Target: "/origin", Value: contracts.ValueExpr{Ref: "trigger.origin"}}},
		ScopeMap:   []contracts.ValueBinding{{Target: "/site", Value: contracts.ValueExpr{Ref: "scope.site"}}},
		ResultMap:  []contracts.ResultBinding{{Source: "/text", Target: "/text"}},
	}
	parentDefinition := contracts.WorkflowDefinition{
		SchemaVersion: workflowkernel.SchemaVersion, WorkflowID: "parent.workflow", Version: "v1",
		InputSchema: parentInput, ResultSchema: textOutput, TriggerSchema: parentTrigger, ScopeSchema: parentScope,
		Entrypoints: map[string]string{"main": "call-child"},
		Nodes: []contracts.WorkflowNodeDefinition{{
			NodeID: "call-child", TypeRef: callDescriptor.TypeRef, DescriptorDigest: callDescriptor.DescriptorDigest,
			InputSchema: empty, OutputSchema: textOutput, CallAction: &call,
		}},
		Edges:          []contracts.WorkflowEdge{},
		ResultBindings: map[string][]contracts.ValueBinding{"main": {{Target: "/text", Value: contracts.ValueExpr{Ref: "nodes.call-child.output.text"}}}},
	}
	parentPlan, err := workflowkernel.Compile(parentDefinition)
	if err != nil {
		t.Fatal(err)
	}
	parentAction := contracts.ActionVersion{
		ActionID: parentDefinition.WorkflowID, Version: parentDefinition.Version, DefinitionDigest: parentPlan.DefinitionDigest,
		Entrypoint: "main", InputSchema: parentInput, ResultSchema: textOutput,
		AcceptedTriggerKinds: []contracts.TriggerKind{contracts.TriggerManual},
	}
	durable, err := filestore.Open(t.TempDir() + "/action-calls.db")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = durable.Close() })
	clock := &fakeClock{now: time.Date(2026, 8, 13, 1, 0, 0, 0, time.UTC)}
	controller, err := New(Config{
		Store: durable, Nodes: registry, OwnerRef: "controller-action-calls",
		LeaseDuration: time.Minute, InvocationTimeout: 10 * time.Second, Clock: clock,
		Actions: actionCatalogFixture{action: childAction, definition: childDefinition, err: catalogErr},
	})
	if err != nil {
		t.Fatal(err)
	}
	request := InvokeRequest{
		RunID: "run-parent-action", NamespaceID: "test", Action: parentAction, Definition: parentDefinition,
		Trigger: contracts.TriggerEvent{
			EventID: "event-parent-action", Kind: contracts.TriggerManual, Version: "v1",
			OccurredAt: clock.Now(), ReceivedAt: clock.Now(), SourceRef: "test-suite", ActorRef: "operator",
			PayloadSchemaDigest: testPackageDigest, Payload: map[string]any{"origin": "panel"},
		},
		Candidate: map[string]any{"name": "Ada"}, CandidateOrigin: contracts.OriginCaller,
		CandidateRef: "manual-form", Scope: map[string]any{"site": "lab-a"}, CommandID: "invoke-parent-action",
	}
	return actionCallFixture{controller: controller, callExecutor: callExecutor, childAction: childAction, request: request}
}
