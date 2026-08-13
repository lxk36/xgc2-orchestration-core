package controller

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/lxk36/xgc2-orchestration-core/durable/filestore"
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

func TestCoordinatorLeavesExternalWaitUntilExactSignalResolution(t *testing.T) {
	durable, err := filestore.Open(t.TempDir() + "/signals.db")
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
	clock := &fakeClock{now: time.Date(2026, 8, 13, 10, 0, 0, 0, time.UTC)}
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
	definition, action := signalWaitAction(t, executor.Descriptor())
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
	snapshot, _, err := orchestrator.GetSnapshot(t.Context(), waiting.RunID)
	if err != nil || snapshot.Waiting == nil || snapshot.Waiting.Wait == nil {
		t.Fatalf("external wait snapshot = %+v err=%v", snapshot, err)
	}
	wait := *snapshot.Waiting.Wait
	if _, err := orchestrator.ResolveExternalWait(t.Context(), ResolveExternalWaitRequest{
		RunID: waiting.RunID, SubjectRef: "wrong-signal", ConditionDigest: wait.ConditionDigest,
		Status: contracts.NodeWaitResolvedSucceeded, Payload: map[string]any{"signalId": "wrong-signal"},
		ObservedAt: clock.Now().Add(time.Second), CommandID: "resolve-wrong-signal",
	}); err == nil {
		t.Fatal("mismatched signal resolution was accepted")
	}
	completed, err := orchestrator.ResolveExternalWait(t.Context(), ResolveExternalWaitRequest{
		RunID: waiting.RunID, SubjectRef: wait.SubjectRef, ConditionDigest: wait.ConditionDigest,
		Status: contracts.NodeWaitResolvedSucceeded, Payload: map[string]any{"signalId": wait.SubjectRef},
		ObservedAt: clock.Now().Add(time.Second), CommandID: "resolve-exact-signal",
	})
	if err != nil || completed.Status != contracts.RunSucceeded {
		t.Fatalf("resolved external wait = %+v err=%v", completed, err)
	}
	terminalReplay, err := orchestrator.ResolveExternalWait(t.Context(), ResolveExternalWaitRequest{
		RunID: waiting.RunID, SubjectRef: wait.SubjectRef, ConditionDigest: wait.ConditionDigest,
		Status: contracts.NodeWaitResolvedSucceeded, Payload: map[string]any{"signalId": wait.SubjectRef},
		ObservedAt: clock.Now().Add(2 * time.Second), CommandID: "resolve-exact-signal-replay",
	})
	if err != nil || terminalReplay.RunID != completed.RunID || terminalReplay.Status != contracts.RunSucceeded {
		t.Fatalf("terminal resolution replay = %+v err=%v", terminalReplay, err)
	}
}

func signalWaitAction(t *testing.T, descriptor contracts.NodeDescriptor) (contracts.WorkflowDefinition, contracts.ActionVersion) {
	t.Helper()
	empty := contracts.Schema{Type: contracts.TypeObject, Properties: map[string]contracts.Schema{}}
	definition := contracts.WorkflowDefinition{
		SchemaVersion: workflowkernel.SchemaVersion, WorkflowID: "signal-action", Version: "v1",
		InputSchema: empty, ResultSchema: descriptor.OutputSchema, TriggerSchema: empty, ScopeSchema: empty,
		Entrypoints: map[string]string{"main": "hold"},
		Nodes: []contracts.WorkflowNodeDefinition{{
			NodeID: "hold", TypeRef: descriptor.TypeRef, DescriptorDigest: descriptor.DescriptorDigest,
			InputSchema: descriptor.InputSchema, OutputSchema: descriptor.OutputSchema,
			FixedInputs: map[string]any{"signalId": "stop-signal"},
		}},
		Edges: []contracts.WorkflowEdge{},
		ResultBindings: map[string][]contracts.ValueBinding{"main": {{
			Target: "/signalId", Value: contracts.ValueExpr{Ref: "nodes.hold.output.signalId"},
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
