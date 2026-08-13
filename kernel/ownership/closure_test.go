package ownership

import (
	"testing"
	"time"

	"github.com/lxk36/xgc2-orchestration-core/kernel/effect"
	"github.com/lxk36/xgc2-orchestration-core/kernel/execution"
	"github.com/lxk36/xgc2-orchestration-core/kernel/runtime"
	"github.com/lxk36/xgc2-orchestration-core/sdk/go/contracts"
)

const digest = "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"

func TestClosureFactsAreDerivedFromExactOwnershipGraph(t *testing.T) {
	t0 := time.Date(2026, 8, 13, 0, 0, 0, 0, time.UTC)
	run := admittedRun(t, "run-root", nil, "run-root", t0)
	invocationID, _ := execution.StableInvocationID(run.RunID, "node-1")
	invocation, err := execution.ActivateInvocation(execution.ActivateInvocationCommand{
		InvocationID: invocationID, NamespaceID: "lab", RunID: run.RunID, NodeID: "node-1",
		TypeRef: "xgc.test-node/v1", DescriptorDigest: digest, ResolvedInputDigest: digest,
		InputRefsDigest: digest, Compensatable: true, CommandID: "activate-1", At: t0,
	})
	if err != nil {
		t.Fatal(err)
	}
	child := admittedRun(t, "run-child", &contracts.ParentRunLink{
		ParentRunID: run.RunID, ParentInvocationID: invocationID, CallNodeID: "node-1", MappingDigest: digest,
	}, run.RunID, t0)
	effectID, _ := effect.StableEffectID(invocationID, "external-call")
	effectDecision, err := effect.Prepare(effect.PrepareCommand{
		Intent: contracts.EffectIntent{
			EffectID: effectID, NamespaceID: "lab", RunID: run.RunID, InvocationID: invocationID,
			PreparedAttemptID: "attempt-1", EffectKey: "external-call", Kind: "xgc.test-effect/v1", TargetRef: "target-1",
			IntentSchemaDigest: digest, IntentDigest: digest, Ownership: contracts.EffectOwned,
			CompensationPolicy: contracts.CompensationRequired, PolicyDigest: digest, DescriptorDigest: digest, Deadline: t0.Add(time.Hour),
		},
		CommandID: "prepare-effect", At: t0,
	})
	if err != nil {
		t.Fatal(err)
	}
	runtimeID, _ := runtime.StableBindingID(invocationID, "simulator")
	runtimeDecision, err := runtime.Prepare(runtime.PrepareBindingCommand{
		BindingID: runtimeID, NamespaceID: "lab", RunID: run.RunID, InvocationID: invocationID, NodeID: "node-1",
		RuntimeKey: "simulator", Kind: "xgc.process-runtime/v1", SpecDigest: digest, ProviderRef: "local-process",
		ProviderDigest: digest, Ownership: contracts.EffectOwned, CleanupPolicy: contracts.RuntimeCleanupOnRunClose,
		Generation: 1, FencingToken: 1, LeaseOwner: "controller-1", LeaseToken: "lease-1",
		LeaseExpiresAt: t0.Add(time.Hour), CommandID: "prepare-runtime", At: t0,
	})
	if err != nil {
		t.Fatal(err)
	}
	facts, err := ClosureFacts(contracts.OwnershipGraph{
		Run: run, Revision: 9, Invocations: []contracts.InvocationLedger{invocation.Ledger},
		ChildRuns: []contracts.Run{child}, Effects: []contracts.EffectRecord{effectDecision.Effect},
		Runtimes:  []contracts.RuntimeBinding{runtimeDecision.Binding},
		Resources: []contracts.ResourceOwnershipFact{{BindingID: "gpu-lease", RunID: run.RunID, Ownership: contracts.EffectOwned, State: contracts.ResourceBindingActive}},
	})
	if err != nil {
		t.Fatal(err)
	}
	if facts.RunRevision != run.Revision || facts.OwnershipGraphRevision != 9 || facts.ActiveInvocationCount != 1 ||
		facts.OpenChildCount != 1 || facts.OpenEffectCount != 1 || facts.OpenEffectCompensationCount != 0 ||
		facts.OpenOwnedRuntimeCount != 1 || facts.OpenOwnedResourceCount != 1 || facts.Satisfied() {
		t.Fatalf("closure facts = %#v", facts)
	}

	empty, err := ClosureFacts(contracts.OwnershipGraph{Run: run, Revision: 10})
	if err != nil {
		t.Fatal(err)
	}
	if !empty.Satisfied() {
		t.Fatalf("empty ownership closure = %#v", empty)
	}
}

func admittedRun(t *testing.T, id string, parent *contracts.ParentRunLink, root string, at time.Time) contracts.Run {
	t.Helper()
	decision, err := execution.AdmitRun(execution.AdmitRunCommand{
		RunID: id, NamespaceID: "lab", ActionRef: contracts.ActionRef{ActionID: "workflow", Version: "v1", Digest: digest},
		ExecutionPlanRef: "plan-1", PlanDigest: digest, TriggerRef: "trigger-1", TriggerDigest: digest,
		InputDigest: digest, Parent: parent, RootRunID: root, ActorRef: "controller", SourceRef: "orchestrator",
		CommandID: "admit-" + id, At: at,
	})
	if err != nil {
		t.Fatal(err)
	}
	return decision.Run
}
