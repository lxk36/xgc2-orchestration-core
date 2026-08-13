package worker

import (
	"context"
	"encoding/json"
	"path/filepath"
	"testing"
	"time"

	"github.com/lxk36/xgc2-orchestration-core/durable/filestore"
	"github.com/lxk36/xgc2-orchestration-core/durable/store"
	"github.com/lxk36/xgc2-orchestration-core/kernel/canonicaljson"
	"github.com/lxk36/xgc2-orchestration-core/kernel/execution"
	"github.com/lxk36/xgc2-orchestration-core/sdk/go/contracts"
)

const testDigest = "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"

func TestWorkerRequiresExplicitSafeDisposition(t *testing.T) {
	ctx := context.Background()
	durable, err := filestore.Open(filepath.Join(t.TempDir(), "worker.wal"))
	if err != nil {
		t.Fatal(err)
	}
	defer durable.Close()
	t0 := time.Date(2026, 8, 12, 20, 0, 0, 0, time.UTC)
	decision, err := execution.AdmitRun(execution.AdmitRunCommand{
		RunID: "run-worker", NamespaceID: "lab", ActionRef: contracts.ActionRef{ActionID: "workflow", Version: "v1", Digest: testDigest},
		ExecutionPlanRef: "plan-1", PlanDigest: testDigest, TriggerRef: "trigger-1", TriggerDigest: testDigest,
		InputDigest: testDigest, ActorRef: "operator", SourceRef: "api", CommandID: "admit-worker", At: t0,
	})
	if err != nil {
		t.Fatal(err)
	}
	payload, _ := canonicaljson.Marshal(decision.Run)
	payloadDigest, _ := canonicaljson.Digest(payload)
	identityDigest, _ := canonicaljson.DigestValue(map[string]any{"commandId": "admit-worker"})
	intentPayload := map[string]any{"runId": decision.Run.RunID}
	intentDigest, _ := canonicaljson.DigestValue(intentPayload)
	seeds := []store.IntentSeed{
		{Intent: contracts.DurableIntent{Kind: contracts.IntentCleanup, Identity: "cleanup-complete", AggregateID: decision.Run.RunID, PayloadDigest: intentDigest, Payload: intentPayload}},
		{Intent: contracts.DurableIntent{Kind: contracts.IntentOutbox, Identity: "outbox-retry", AggregateID: decision.Run.RunID, PayloadDigest: intentDigest, Payload: intentPayload}},
		{Intent: contracts.DurableIntent{Kind: contracts.IntentReconcile, Identity: "reconcile-uncertain", AggregateID: decision.Run.RunID, PayloadDigest: intentDigest, Payload: intentPayload}},
	}
	outcome, _ := json.Marshal(map[string]any{"runId": decision.Run.RunID})
	_, err = durable.Commit(ctx, store.Transaction{
		CommandID: "admit-worker", IdentityDigest: identityDigest,
		Expected:  []store.ExpectedRevision{{Key: store.AggregateKey{Type: "run", ID: decision.Run.RunID}, Revision: 0}},
		Mutations: []store.AggregateRecord{{Key: store.AggregateKey{Type: "run", ID: decision.Run.RunID}, Revision: 1, PayloadDigest: payloadDigest, Payload: payload}},
		Events:    decision.Events, Intents: seeds, Outcome: outcome, At: t0,
	})
	if err != nil {
		t.Fatal(err)
	}
	retryAt := t0.Add(10 * time.Second)
	transient := &contracts.StructuredFailure{Class: contracts.FailureTransient, Code: "provider.busy", Message: "provider busy"}
	uncertain := &contracts.StructuredFailure{Class: contracts.FailureUncertain, Code: "provider.unknown", Message: "provider outcome unknown"}
	runner := Worker{
		Store: durable, OwnerRef: "worker-1",
		Handlers: map[contracts.DurableIntentKind]Handler{
			contracts.IntentCleanup: HandlerFunc(func(context.Context, store.ClaimedIntent) Result { return Result{Disposition: Complete} }),
			contracts.IntentOutbox: HandlerFunc(func(context.Context, store.ClaimedIntent) Result {
				return Result{Disposition: Retry, Failure: transient, AvailableAt: &retryAt}
			}),
			contracts.IntentReconcile: HandlerFunc(func(context.Context, store.ClaimedIntent) Result {
				return Result{Disposition: Retry, Failure: uncertain, AvailableAt: &retryAt}
			}),
		},
	}
	batch, err := runner.RunOnce(ctx, Batch{LeaseToken: "batch-lease", Now: t0.Add(time.Second), LeaseExpiresAt: t0.Add(5 * time.Second), Limit: 10})
	if err == nil {
		t.Fatal("uncertain automatic retry was not rejected")
	}
	if batch.Claimed != 3 || batch.Completed != 1 || batch.Retried != 1 || batch.Left != 1 {
		t.Fatalf("batch result = %#v, err %v", batch, err)
	}
	completed, _ := durable.GetIntent(ctx, "cleanup-complete")
	retried, _ := durable.GetIntent(ctx, "outbox-retry")
	left, _ := durable.GetIntent(ctx, "reconcile-uncertain")
	if completed.Status != store.IntentCompleted || retried.Status != store.IntentPending || left.Status != store.IntentLeased {
		t.Fatalf("intent states: complete=%s retry=%s left=%s", completed.Status, retried.Status, left.Status)
	}
}
