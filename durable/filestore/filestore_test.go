package filestore

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/lxk36/xgc2-orchestration-core/durable/store"
	"github.com/lxk36/xgc2-orchestration-core/kernel/canonicaljson"
	"github.com/lxk36/xgc2-orchestration-core/kernel/execution"
	"github.com/lxk36/xgc2-orchestration-core/sdk/go/contracts"
)

const fixtureDigest = "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"

func TestAtomicCommitReplayLeaseAndRestart(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "orchestration.wal")
	durable, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = durable.Close() })
	info, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	if info.Mode().Perm() != 0o600 {
		t.Fatalf("store permissions = %o", info.Mode().Perm())
	}

	t0 := time.Date(2026, 8, 12, 17, 0, 0, 0, time.UTC)
	admitted, err := execution.AdmitRun(execution.AdmitRunCommand{
		RunID: "run-durable", NamespaceID: "lab", ActionRef: contracts.ActionRef{ActionID: "experiment", Version: "v1", Digest: fixtureDigest},
		ExecutionPlanRef: "plan-1", PlanDigest: fixtureDigest, TriggerRef: "trigger-1", TriggerDigest: fixtureDigest,
		InputDigest: fixtureDigest, ActorRef: "operator", SourceRef: "panel", CommandID: "admit-durable", At: t0,
	})
	if err != nil {
		t.Fatal(err)
	}
	transaction := runTransaction(t, admitted, 0, "admit-durable", t0)
	committed, err := durable.Commit(ctx, transaction)
	if err != nil {
		t.Fatal(err)
	}
	if committed.Replay || committed.OutcomeDigest == "" {
		t.Fatalf("commit result = %#v", committed)
	}

	replayRequest := store.Transaction{CommandID: transaction.CommandID, IdentityDigest: transaction.IdentityDigest}
	replayed, err := durable.Commit(ctx, replayRequest)
	if err != nil {
		t.Fatal(err)
	}
	if !replayed.Replay || string(replayed.Outcome) != string(committed.Outcome) {
		t.Fatalf("replayed result = %#v", replayed)
	}
	replayRequest.IdentityDigest = fixtureDigest
	if replayRequest.IdentityDigest == transaction.IdentityDigest {
		replayRequest.IdentityDigest = "sha256:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
	}
	if _, err := durable.Commit(ctx, replayRequest); !errors.Is(err, store.ErrIdentityConflict) {
		t.Fatalf("command identity conflict = %v", err)
	}

	key := store.AggregateKey{Type: "run", ID: admitted.Run.RunID}
	aggregate, err := durable.GetAggregate(ctx, key)
	if err != nil || aggregate.Revision != 1 {
		t.Fatalf("aggregate = %#v, err %v", aggregate, err)
	}
	events, err := durable.EventsAfter(ctx, key, 0, 10)
	if err != nil || len(events) != 1 || events[0].Type != "run.accepted" {
		t.Fatalf("events = %#v, err %v", events, err)
	}

	failure := &contracts.StructuredFailure{Class: contracts.FailureCanceled, Code: "operator.stop", Message: "operator requested stop"}
	stopping, err := execution.TransitionRun(admitted.Run, execution.RunTransitionCommand{
		RunID: admitted.Run.RunID, ExpectedRevision: admitted.Run.Revision, To: contracts.RunStopping,
		Termination: &contracts.TerminationIntent{Kind: contracts.TerminationStopped, RequestedBy: "operator", ReasonCode: "operator.stop", PrimaryFailure: nil, CommandID: "stop-intent", RequestedAt: t0.Add(time.Second)},
		CommandID:   "stop-durable", At: t0.Add(time.Second),
	})
	_ = failure
	if err != nil {
		t.Fatal(err)
	}
	stopTransaction := runTransaction(t, stopping, 1, "stop-durable", t0.Add(time.Second))
	if _, err := durable.Commit(ctx, stopTransaction); err != nil {
		t.Fatal(err)
	}
	if len(stopping.Intents) != 1 {
		t.Fatal("stopping did not emit cleanup intent")
	}

	claims, err := durable.ClaimIntents(ctx, store.ClaimRequest{
		Kinds: []contracts.DurableIntentKind{contracts.IntentCleanup}, OwnerRef: "cleanup-worker", LeaseToken: "cleanup-lease-1",
		Now: t0.Add(2 * time.Second), LeaseExpiresAt: t0.Add(time.Minute), Limit: 10,
	})
	if err != nil || len(claims) != 1 {
		t.Fatalf("claims = %#v, err %v", claims, err)
	}
	fence := store.IntentFence{
		IntentID: claims[0].Record.Intent.Identity, ExpectedRevision: claims[0].Record.Revision,
		OwnerRef: "cleanup-worker", LeaseToken: "wrong-token", At: t0.Add(3 * time.Second),
	}
	if _, err := durable.CompleteIntent(ctx, fence); !errors.Is(err, store.ErrLeaseConflict) {
		t.Fatalf("wrong lease completion = %v", err)
	}
	fence.LeaseToken = claims[0].LeaseToken
	completed, err := durable.CompleteIntent(ctx, fence)
	if err != nil || completed.Status != store.IntentCompleted {
		t.Fatalf("completed intent = %#v, err %v", completed, err)
	}

	inbox := store.InboxRecord{SourceRef: "provider-1", MessageID: "receipt-1", PayloadDigest: fixtureDigest, ObservedAt: t0.Add(4 * time.Second)}
	wasReplay, err := durable.RecordInbox(ctx, inbox)
	if err != nil || wasReplay {
		t.Fatalf("first inbox replay = %v, err %v", wasReplay, err)
	}
	wasReplay, err = durable.RecordInbox(ctx, inbox)
	if err != nil || !wasReplay {
		t.Fatalf("second inbox replay = %v, err %v", wasReplay, err)
	}
	inbox.PayloadDigest = "sha256:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
	if _, err := durable.RecordInbox(ctx, inbox); !errors.Is(err, store.ErrIdentityConflict) {
		t.Fatalf("inbox identity conflict = %v", err)
	}

	if err := durable.Close(); err != nil {
		t.Fatal(err)
	}
	durable, err = Open(path)
	if err != nil {
		t.Fatal(err)
	}
	aggregate, err = durable.GetAggregate(ctx, key)
	if err != nil || aggregate.Revision != 2 {
		t.Fatalf("recovered aggregate = %#v, err %v", aggregate, err)
	}
	recoveredIntent, err := durable.GetIntent(ctx, completed.Intent.Identity)
	if err != nil || recoveredIntent.Status != store.IntentCompleted {
		t.Fatalf("recovered intent = %#v, err %v", recoveredIntent, err)
	}
}

func TestExpiredLeaseIsAdoptedAndRetryIsScheduled(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "lease.wal")
	durable, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer durable.Close()
	t0 := time.Date(2026, 8, 12, 18, 0, 0, 0, time.UTC)
	decision, err := execution.AdmitRun(execution.AdmitRunCommand{
		RunID: "run-lease", NamespaceID: "lab", ActionRef: contracts.ActionRef{ActionID: "workflow", Version: "v1", Digest: fixtureDigest},
		ExecutionPlanRef: "plan-1", PlanDigest: fixtureDigest, TriggerRef: "trigger-1", TriggerDigest: fixtureDigest,
		InputDigest: fixtureDigest, ActorRef: "operator", SourceRef: "api", CommandID: "admit-lease", At: t0,
	})
	if err != nil {
		t.Fatal(err)
	}
	transaction := runTransaction(t, decision, 0, "admit-lease", t0)
	payload := map[string]any{"runId": decision.Run.RunID}
	payloadDigest, _ := canonicaljson.DigestValue(payload)
	transaction.Intents = []store.IntentSeed{{Intent: contracts.DurableIntent{Kind: contracts.IntentOutbox, Identity: "outbox-lease", AggregateID: decision.Run.RunID, PayloadDigest: payloadDigest, Payload: payload}}}
	if _, err := durable.Commit(ctx, transaction); err != nil {
		t.Fatal(err)
	}
	first, err := durable.ClaimIntents(ctx, store.ClaimRequest{OwnerRef: "worker-1", LeaseToken: "lease-1", Now: t0.Add(time.Second), LeaseExpiresAt: t0.Add(5 * time.Second), Limit: 1})
	if err != nil || len(first) != 1 {
		t.Fatalf("first claim = %#v, err %v", first, err)
	}
	none, err := durable.ClaimIntents(ctx, store.ClaimRequest{OwnerRef: "worker-2", LeaseToken: "lease-2", Now: t0.Add(4 * time.Second), LeaseExpiresAt: t0.Add(8 * time.Second), Limit: 1})
	if err != nil || len(none) != 0 {
		t.Fatalf("claim before expiry = %#v, err %v", none, err)
	}
	adopted, err := durable.ClaimIntents(ctx, store.ClaimRequest{OwnerRef: "worker-2", LeaseToken: "lease-2", Now: t0.Add(5 * time.Second), LeaseExpiresAt: t0.Add(9 * time.Second), Limit: 1})
	if err != nil || len(adopted) != 1 || adopted[0].Record.AttemptCount != 2 {
		t.Fatalf("adopted claim = %#v, err %v", adopted, err)
	}
	oldFence := store.IntentFence{IntentID: first[0].Record.Intent.Identity, ExpectedRevision: first[0].Record.Revision, OwnerRef: "worker-1", LeaseToken: "lease-1", At: t0.Add(6 * time.Second)}
	if _, err := durable.CompleteIntent(ctx, oldFence); !errors.Is(err, store.ErrLeaseConflict) {
		t.Fatalf("stale claimant completion = %v", err)
	}
	retryAt := t0.Add(12 * time.Second)
	failed, err := durable.FailIntent(ctx, store.IntentFailure{
		Fence:   store.IntentFence{IntentID: adopted[0].Record.Intent.Identity, ExpectedRevision: adopted[0].Record.Revision, OwnerRef: "worker-2", LeaseToken: "lease-2", At: t0.Add(6 * time.Second)},
		Failure: contracts.StructuredFailure{Class: contracts.FailureTransient, Code: "transport.timeout", Message: "temporary timeout"}, AvailableAt: &retryAt,
	})
	if err != nil || failed.Status != store.IntentPending || !failed.AvailableAt.Equal(retryAt) {
		t.Fatalf("failed intent = %#v, err %v", failed, err)
	}
	none, err = durable.ClaimIntents(ctx, store.ClaimRequest{OwnerRef: "worker-3", LeaseToken: "lease-3", Now: t0.Add(11 * time.Second), LeaseExpiresAt: t0.Add(20 * time.Second), Limit: 1})
	if err != nil || len(none) != 0 {
		t.Fatalf("claim before retry due = %#v, err %v", none, err)
	}
}

func TestFileLockAndIncompleteTailRecovery(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "recovery.wal")
	durable, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := Open(path); !errors.Is(err, store.ErrLocked) {
		t.Fatalf("second open error = %v", err)
	}
	t0 := time.Date(2026, 8, 12, 19, 0, 0, 0, time.UTC)
	decision, err := execution.AdmitRun(execution.AdmitRunCommand{
		RunID: "run-recovery", NamespaceID: "lab", ActionRef: contracts.ActionRef{ActionID: "workflow", Version: "v1", Digest: fixtureDigest},
		ExecutionPlanRef: "plan-1", PlanDigest: fixtureDigest, TriggerRef: "trigger-1", TriggerDigest: fixtureDigest,
		InputDigest: fixtureDigest, ActorRef: "operator", SourceRef: "api", CommandID: "admit-recovery", At: t0,
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := durable.Commit(ctx, runTransaction(t, decision, 0, "admit-recovery", t0)); err != nil {
		t.Fatal(err)
	}
	if err := durable.Close(); err != nil {
		t.Fatal(err)
	}
	before, _ := os.Stat(path)
	file, err := os.OpenFile(path, os.O_WRONLY|os.O_APPEND, 0)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := file.Write([]byte{0, 0, 0, 0}); err != nil {
		t.Fatal(err)
	}
	_ = file.Close()
	durable, err = Open(path)
	if err != nil {
		t.Fatal(err)
	}
	after, _ := os.Stat(path)
	if after.Size() != before.Size() {
		t.Fatalf("recovered size = %d, want %d", after.Size(), before.Size())
	}
	key := store.AggregateKey{Type: "run", ID: decision.Run.RunID}
	if _, err := durable.GetAggregate(ctx, key); err != nil {
		t.Fatal(err)
	}
	_ = durable.Close()

	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	raw[len(raw)-1] ^= 0xff
	if err := os.WriteFile(path, raw, 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := Open(path); !errors.Is(err, store.ErrCorrupt) {
		t.Fatalf("checksum corruption error = %v", err)
	}
}

func TestListAggregatesUsesStableTypeScopedCursor(t *testing.T) {
	ctx := context.Background()
	durable, err := Open(t.TempDir() + "/list.db")
	if err != nil {
		t.Fatal(err)
	}
	defer durable.Close()
	t0 := time.Date(2026, 8, 12, 20, 0, 0, 0, time.UTC)
	for index, runID := range []string{"run-c", "run-a", "run-b"} {
		decision, err := execution.AdmitRun(execution.AdmitRunCommand{
			RunID: runID, NamespaceID: "lab", ActionRef: contracts.ActionRef{ActionID: "workflow", Version: "v1", Digest: fixtureDigest},
			ExecutionPlanRef: "plan-1", PlanDigest: fixtureDigest, TriggerRef: "trigger-1", TriggerDigest: fixtureDigest,
			InputDigest: fixtureDigest, ActorRef: "operator", SourceRef: "api", CommandID: "admit-" + runID, At: t0.Add(time.Duration(index) * time.Second),
		})
		if err != nil {
			t.Fatal(err)
		}
		if _, err := durable.Commit(ctx, runTransaction(t, decision, 0, "admit-"+runID, t0.Add(time.Duration(index)*time.Second))); err != nil {
			t.Fatal(err)
		}
	}
	first, err := durable.ListAggregates(ctx, "run", "", 2)
	if err != nil || len(first) != 2 || first[0].Key.ID != "run-a" || first[1].Key.ID != "run-b" {
		t.Fatalf("first page = %#v, err = %v", first, err)
	}
	second, err := durable.ListAggregates(ctx, "run", first[1].Key.ID, 2)
	if err != nil || len(second) != 1 || second[0].Key.ID != "run-c" {
		t.Fatalf("second page = %#v, err = %v", second, err)
	}
	if _, err := durable.ListAggregates(ctx, "run", "", 0); err == nil {
		t.Fatal("invalid list limit was accepted")
	}
}

func TestDurableFrameExceedsSingleProtocolValueLimitAndRecovers(t *testing.T) {
	path := filepath.Join(t.TempDir(), "large-frame.wal")
	durable, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	t0 := time.Date(2026, 8, 13, 6, 30, 0, 0, time.UTC)
	payload, err := canonicaljson.Marshal(map[string]any{"data": strings.Repeat("x", 2048)})
	if err != nil {
		t.Fatal(err)
	}
	payloadDigest, err := canonicaljson.Digest(payload)
	if err != nil {
		t.Fatal(err)
	}
	transaction := store.Transaction{
		CommandID: "large-frame-commit", IdentityDigest: fixtureDigest,
		Outcome: json.RawMessage(`{"stored":true}`), At: t0,
	}
	for index := 0; index < 700; index++ {
		key := store.AggregateKey{Type: "fixture", ID: fmt.Sprintf("item-%04d", index)}
		transaction.Expected = append(transaction.Expected, store.ExpectedRevision{Key: key})
		transaction.Mutations = append(transaction.Mutations, store.AggregateRecord{
			Key: key, Revision: 1, PayloadDigest: payloadDigest, Payload: payload,
		})
		eventPayload := map[string]any{"itemId": key.ID}
		eventPayloadDigest, digestErr := canonicaljson.DigestValue(eventPayload)
		if digestErr != nil {
			t.Fatal(digestErr)
		}
		eventID, identityErr := execution.StableEventID(key.Type, key.ID, 1)
		if identityErr != nil {
			t.Fatal(identityErr)
		}
		transaction.Events = append(transaction.Events, contracts.DomainEvent{
			EventID: eventID, AggregateType: key.Type, AggregateID: key.ID, AggregateRevision: 1,
			Type: "fixture.created", CommandID: transaction.CommandID,
			PayloadSchemaDigest: fixtureDigest, PayloadDigest: eventPayloadDigest,
			Payload: eventPayload, OccurredAt: t0,
		})
	}
	if _, err := durable.Commit(t.Context(), transaction); err != nil {
		t.Fatal(err)
	}
	info, err := os.Stat(path)
	if err != nil || info.Size() <= canonicaljson.DefaultMaxInputBytes {
		t.Fatalf("durable frame size = %d, err=%v", info.Size(), err)
	}
	if err := durable.Close(); err != nil {
		t.Fatal(err)
	}
	durable, err = Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer durable.Close()
	recovered, err := durable.ListAggregates(t.Context(), "fixture", "", 1000)
	if err != nil || len(recovered) != 700 {
		t.Fatalf("recovered aggregates = %d, err=%v", len(recovered), err)
	}
}

func runTransaction(t *testing.T, decision execution.RunDecision, expected uint64, commandID string, at time.Time) store.Transaction {
	t.Helper()
	payload, err := canonicaljson.Marshal(decision.Run)
	if err != nil {
		t.Fatal(err)
	}
	payloadDigest, err := canonicaljson.Digest(payload)
	if err != nil {
		t.Fatal(err)
	}
	identityDigest, err := canonicaljson.DigestValue(map[string]any{"commandId": commandID})
	if err != nil {
		t.Fatal(err)
	}
	seeds := make([]store.IntentSeed, len(decision.Intents))
	for index := range decision.Intents {
		seeds[index] = store.IntentSeed{Intent: decision.Intents[index]}
	}
	outcome, _ := json.Marshal(map[string]any{"runId": decision.Run.RunID, "revision": decision.Run.Revision})
	return store.Transaction{
		CommandID: commandID, IdentityDigest: identityDigest,
		Expected:  []store.ExpectedRevision{{Key: store.AggregateKey{Type: "run", ID: decision.Run.RunID}, Revision: expected}},
		Mutations: []store.AggregateRecord{{Key: store.AggregateKey{Type: "run", ID: decision.Run.RunID}, Revision: decision.Run.Revision, PayloadDigest: payloadDigest, Payload: payload}},
		Events:    decision.Events, Intents: seeds, Outcome: outcome, At: at,
	}
}
