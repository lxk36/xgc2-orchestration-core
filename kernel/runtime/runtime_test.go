package runtime

import (
	"errors"
	"testing"
	"time"

	"github.com/lxk36/xgc2-orchestration-core/sdk/go/contracts"
)

const testDigest = "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"

func TestOwnedRuntimeTakeoverFencesOldOwnerAndRequiresStoppedProof(t *testing.T) {
	t0 := time.Date(2026, 8, 12, 21, 0, 0, 0, time.UTC)
	binding := prepareBinding(t, t0, contracts.EffectOwned)
	original := binding
	running := observe(t, binding, "lease-1", t0.Add(time.Second), "process-42", contracts.RuntimeObservedRunning, contracts.RuntimeHealthHealthy, nil)
	if original.State != contracts.RuntimeBindingPreparing || original.ExternalIdentity != "" {
		t.Fatal("observation mutated the input binding")
	}
	if running.State != contracts.RuntimeBindingActive || running.ActivatedAt == nil {
		t.Fatalf("active binding = %#v", running)
	}

	_, err := Takeover(running, TakeoverCommand{
		BindingID: running.BindingID, ExpectedRevision: running.Revision, Generation: 2, FencingToken: 11,
		LeaseOwner: "controller-2", LeaseToken: "lease-2", LeaseExpiresAt: t0.Add(time.Minute),
		CommandID: "takeover-early", At: t0.Add(9 * time.Second),
	})
	if !errors.Is(err, ErrFenceConflict) {
		t.Fatalf("early takeover error = %v", err)
	}
	taken, err := Takeover(running, TakeoverCommand{
		BindingID: running.BindingID, ExpectedRevision: running.Revision, Generation: 2, FencingToken: 11,
		LeaseOwner: "controller-2", LeaseToken: "lease-2", LeaseExpiresAt: t0.Add(time.Minute),
		CommandID: "takeover", At: t0.Add(10 * time.Second),
	})
	if err != nil {
		t.Fatal(err)
	}
	if taken.Binding.State != contracts.RuntimeBindingPreparing || taken.Binding.Generation != 2 || len(taken.Intents) != 1 || taken.Intents[0].Kind != contracts.IntentReconcile {
		t.Fatalf("takeover decision = %#v", taken)
	}
	stale := ObserveCommand{
		Fence:            BindingFence{BindingID: taken.Binding.BindingID, ExpectedRevision: taken.Binding.Revision, Generation: 1, FencingToken: 10, LeaseOwner: "controller-1", LeaseToken: "lease-1", At: t0.Add(11 * time.Second)},
		ExternalIdentity: "process-42", ObservedState: contracts.RuntimeObservedRunning, Health: contracts.RuntimeHealthHealthy, CommandID: "stale-observation",
	}
	stale.ObservationDigest, _ = ObservationDigest(taken.Binding.BindingID, 1, 10, stale.ExternalIdentity, stale.ObservedState, stale.Health, nil)
	if _, err := Observe(taken.Binding, stale); !errors.Is(err, ErrFenceConflict) {
		t.Fatalf("stale owner observation error = %v", err)
	}

	binding = observe(t, taken.Binding, "lease-2", t0.Add(11*time.Second), "process-42", contracts.RuntimeObservedRunning, contracts.RuntimeHealthHealthy, nil)
	release, err := BeginRelease(binding, ReleaseCommand{
		Fence: fence(binding, "lease-2", t0.Add(12*time.Second)), CommandID: "begin-release",
	})
	if err != nil {
		t.Fatal(err)
	}
	if release.Binding.State != contracts.RuntimeBindingStopping || len(release.Intents) != 1 || release.Intents[0].Kind != contracts.IntentCleanup {
		t.Fatalf("release decision = %#v", release)
	}
	if _, err := FinishRelease(release.Binding, ReleaseCommand{Fence: fence(release.Binding, "lease-2", t0.Add(13*time.Second)), CommandID: "finish-too-early"}); !errors.Is(err, ErrInvalidTransition) {
		t.Fatalf("finish without stopped proof = %v", err)
	}
	stopped := observe(t, release.Binding, "lease-2", t0.Add(13*time.Second), "process-42", contracts.RuntimeObservedStopped, contracts.RuntimeHealthUnknown, nil)
	finished, err := FinishRelease(stopped, ReleaseCommand{Fence: fence(stopped, "lease-2", t0.Add(14*time.Second)), CommandID: "finish-release"})
	if err != nil {
		t.Fatal(err)
	}
	if finished.Binding.State != contracts.RuntimeBindingReleased || finished.Binding.FinishedAt == nil || finished.Binding.LeaseOwner != "" || !finished.Binding.LeaseExpiresAt.IsZero() {
		t.Fatalf("released binding = %#v", finished.Binding)
	}
}

func TestAttachedRuntimeReleaseNeverDestroysExternalObject(t *testing.T) {
	t0 := time.Date(2026, 8, 12, 22, 0, 0, 0, time.UTC)
	binding := prepareBinding(t, t0, contracts.EffectAttached)
	binding = observe(t, binding, "lease-1", t0.Add(time.Second), "shared-process", contracts.RuntimeObservedRunning, contracts.RuntimeHealthHealthy, nil)
	released, err := BeginRelease(binding, ReleaseCommand{Fence: fence(binding, "lease-1", t0.Add(2*time.Second)), CommandID: "detach"})
	if err != nil {
		t.Fatal(err)
	}
	if released.Binding.State != contracts.RuntimeBindingReleased || len(released.Intents) != 0 || released.Binding.ObservedState != contracts.RuntimeObservedRunning {
		t.Fatalf("attached release = %#v", released)
	}
}

func TestLostObservationClosesLeaseWithoutPretendingStopped(t *testing.T) {
	t0 := time.Date(2026, 8, 12, 23, 0, 0, 0, time.UTC)
	binding := prepareBinding(t, t0, contracts.EffectOwned)
	failure := &contracts.StructuredFailure{Class: contracts.FailureUncertain, Code: "provider.lost", Message: "provider can no longer identify the runtime"}
	lost := observe(t, binding, "lease-1", t0.Add(time.Second), "", contracts.RuntimeObservedLost, contracts.RuntimeHealthLost, failure)
	if lost.State != contracts.RuntimeBindingLost || lost.ObservedState == contracts.RuntimeObservedStopped || lost.PrimaryFailure == nil || lost.LeaseOwner != "" {
		t.Fatalf("lost binding = %#v", lost)
	}
}

func prepareBinding(t *testing.T, at time.Time, ownership contracts.EffectOwnership) contracts.RuntimeBinding {
	t.Helper()
	id, err := StableBindingID("invocation-1", "simulator")
	if err != nil {
		t.Fatal(err)
	}
	cleanup := contracts.RuntimeCleanupNone
	if ownership == contracts.EffectOwned {
		cleanup = contracts.RuntimeCleanupOnRunClose
	}
	decision, err := Prepare(PrepareBindingCommand{
		BindingID: id, NamespaceID: "lab", RunID: "run-1", InvocationID: "invocation-1", NodeID: "start-simulator",
		RuntimeKey: "simulator", Kind: "xgc.process-runtime/v1", SpecDigest: testDigest,
		ProviderRef: "local-process", ProviderDigest: testDigest, Ownership: ownership, CleanupPolicy: cleanup,
		Generation: 1, FencingToken: 10, LeaseOwner: "controller-1", LeaseToken: "lease-1",
		LeaseExpiresAt: at.Add(10 * time.Second), CommandID: "prepare-runtime", At: at,
	})
	if err != nil {
		t.Fatal(err)
	}
	return decision.Binding
}

func observe(t *testing.T, binding contracts.RuntimeBinding, token string, at time.Time, external string, state contracts.RuntimeObservedState, health contracts.RuntimeHealth, failure *contracts.StructuredFailure) contracts.RuntimeBinding {
	t.Helper()
	digest, err := ObservationDigest(binding.BindingID, binding.Generation, binding.FencingToken, external, state, health, failure)
	if err != nil {
		t.Fatal(err)
	}
	decision, err := Observe(binding, ObserveCommand{
		Fence: fence(binding, token, at), ExternalIdentity: external, ObservedState: state,
		Health: health, ObservationDigest: digest, Failure: failure, CommandID: "observe-" + string(state),
	})
	if err != nil {
		t.Fatal(err)
	}
	return decision.Binding
}

func fence(binding contracts.RuntimeBinding, token string, at time.Time) BindingFence {
	return BindingFence{
		BindingID: binding.BindingID, ExpectedRevision: binding.Revision, Generation: binding.Generation,
		FencingToken: binding.FencingToken, LeaseOwner: binding.LeaseOwner, LeaseToken: token, At: at,
	}
}
