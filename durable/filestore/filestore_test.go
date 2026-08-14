package filestore

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"syscall"
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
	lockInfo, err := os.Stat(path + lockSuffix)
	if err != nil {
		t.Fatal(err)
	}
	if lockInfo.Mode().Perm() != 0o600 {
		t.Fatalf("sidecar permissions = %o", lockInfo.Mode().Perm())
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

func TestOpenRejectsNonCanonicalAndAliasedAuthorities(t *testing.T) {
	t.Run("relative", func(t *testing.T) {
		if _, err := Open("relative.db"); err == nil {
			t.Fatal("relative store path was accepted")
		}
	})
	t.Run("unclean", func(t *testing.T) {
		path := t.TempDir() + "/nested/../data.db"
		if _, err := Open(path); err == nil {
			t.Fatal("unclean store path was accepted")
		}
	})
	t.Run("symlink-parent", func(t *testing.T) {
		root := t.TempDir()
		realParent := filepath.Join(root, "real")
		if err := os.Mkdir(realParent, 0o700); err != nil {
			t.Fatal(err)
		}
		aliasParent := filepath.Join(root, "alias")
		if err := os.Symlink(realParent, aliasParent); err != nil {
			t.Fatal(err)
		}
		if _, err := Open(filepath.Join(aliasParent, "data.db")); !errors.Is(err, errUnsafePath) {
			t.Fatalf("symlink parent error = %v", err)
		}
	})
	for _, name := range []string{"data-symlink", "lock-symlink"} {
		t.Run(name, func(t *testing.T) {
			root := t.TempDir()
			path := filepath.Join(root, "data.db")
			target := filepath.Join(root, "target")
			if err := os.WriteFile(target, nil, 0o600); err != nil {
				t.Fatal(err)
			}
			link := path
			if name == "lock-symlink" {
				link += lockSuffix
			}
			if err := os.Symlink(target, link); err != nil {
				t.Fatal(err)
			}
			if _, err := Open(path); !errors.Is(err, errUnsafePath) {
				t.Fatalf("symlink error = %v", err)
			}
		})
	}
	for _, name := range []string{"data-hardlink", "lock-hardlink"} {
		t.Run(name, func(t *testing.T) {
			root := t.TempDir()
			path := filepath.Join(root, "data.db")
			target := filepath.Join(root, "target")
			if err := os.WriteFile(target, nil, 0o600); err != nil {
				t.Fatal(err)
			}
			link := path
			if name == "lock-hardlink" {
				link += lockSuffix
			}
			if err := os.Link(target, link); err != nil {
				t.Fatal(err)
			}
			if _, err := Open(path); !errors.Is(err, errUnsafePath) {
				t.Fatalf("hardlink error = %v", err)
			}
		})
	}
	t.Run("non-regular", func(t *testing.T) {
		root := t.TempDir()
		path := filepath.Join(root, "data.db")
		if err := os.Mkdir(path, 0o700); err != nil {
			t.Fatal(err)
		}
		if _, err := Open(path); !errors.Is(err, errUnsafePath) {
			t.Fatalf("directory error = %v", err)
		}
	})
	t.Run("non-regular-lock", func(t *testing.T) {
		path := filepath.Join(t.TempDir(), "data.db")
		if err := os.Mkdir(path+lockSuffix, 0o700); err != nil {
			t.Fatal(err)
		}
		if _, err := Open(path); !errors.Is(err, errUnsafePath) {
			t.Fatalf("lock directory error = %v", err)
		}
	})
	t.Run("data-lock-inode-collision", func(t *testing.T) {
		path := filepath.Join(t.TempDir(), "data.db")
		if err := os.WriteFile(path, nil, 0o600); err != nil {
			t.Fatal(err)
		}
		if err := os.Link(path, path+lockSuffix); err != nil {
			t.Fatal(err)
		}
		if _, err := Open(path); !errors.Is(err, errUnsafePath) {
			t.Fatalf("data/lock inode collision error = %v", err)
		}
	})
}

func TestStableSidecarLockRejectsStaleAndReplacementOpens(t *testing.T) {
	path := filepath.Join(t.TempDir(), "authority.db")
	durable, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer durable.Close()

	// Replacing the data path cannot shed the independent lifecycle lock.
	if err := os.Rename(path, path+".stale"); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, nil, 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := Open(path); !errors.Is(err, store.ErrLocked) {
		t.Fatalf("data replacement bypassed sidecar lock: %v", err)
	}
	if err := durable.append(emptyState()); !errors.Is(err, errUnsafePath) {
		t.Fatalf("data replacement did not poison owner: %v", err)
	}
	if err := durable.append(emptyState()); !errors.Is(err, errUnsafePath) {
		t.Fatalf("replaced authority became usable again: %v", err)
	}
}

func TestOpenAcquiresSidecarBeforeRecoveringData(t *testing.T) {
	path := filepath.Join(t.TempDir(), "locked-corrupt.db")
	if err := os.WriteFile(path, []byte("committed-looking corruption"), 0o600); err != nil {
		t.Fatal(err)
	}
	lockFile, err := os.OpenFile(path+lockSuffix, os.O_CREATE|os.O_RDWR, 0o600)
	if err != nil {
		t.Fatal(err)
	}
	defer lockFile.Close()
	if err := syscall.Flock(int(lockFile.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); err != nil {
		t.Fatal(err)
	}
	defer syscall.Flock(int(lockFile.Fd()), syscall.LOCK_UN)
	if _, err := Open(path); !errors.Is(err, store.ErrLocked) {
		t.Fatalf("Open recovered data before acquiring sidecar: %v", err)
	}
}

func TestStableSidecarDetectsLockAndParentReplacement(t *testing.T) {
	for _, replacement := range []string{"lock", "parent"} {
		t.Run(replacement, func(t *testing.T) {
			root := t.TempDir()
			parent := filepath.Join(root, "store")
			if err := os.Mkdir(parent, 0o700); err != nil {
				t.Fatal(err)
			}
			path := filepath.Join(parent, "data.db")
			durable, err := Open(path)
			if err != nil {
				t.Fatal(err)
			}
			defer durable.Close()
			if replacement == "lock" {
				if err := os.Rename(path+lockSuffix, path+lockSuffix+".stale"); err != nil {
					t.Fatal(err)
				}
				if err := os.WriteFile(path+lockSuffix, nil, 0o600); err != nil {
					t.Fatal(err)
				}
			} else {
				if err := os.Rename(parent, parent+".stale"); err != nil {
					t.Fatal(err)
				}
				if err := os.Mkdir(parent, 0o700); err != nil {
					t.Fatal(err)
				}
			}
			if err := durable.append(emptyState()); !errors.Is(err, errUnsafePath) {
				t.Fatalf("%s replacement error = %v", replacement, err)
			}
		})
	}
}

func TestParentReplacementDuringOpenFailsClosed(t *testing.T) {
	root := t.TempDir()
	parent := filepath.Join(root, "store")
	if err := os.Mkdir(parent, 0o700); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(parent, "data.db")
	hooks := &ioHooks{sync: func(operation string, file *os.File) error {
		if operation == "lock-sync" {
			if err := os.Rename(parent, parent+".stale"); err != nil {
				return err
			}
			if err := os.Mkdir(parent, 0o700); err != nil {
				return err
			}
		}
		return file.Sync()
	}}
	if durable, err := openWithHooks(path, hooks); durable != nil || !errors.Is(err, errUnsafePath) {
		if durable != nil {
			_ = durable.Close()
		}
		t.Fatalf("Open parent replacement = %#v, %v", durable, err)
	}
}

func TestCanonicalAncestorSymlinkBackReplacementFailsClosed(t *testing.T) {
	for _, replacement := range []string{"direct-parent", "ancestor"} {
		t.Run(replacement, func(t *testing.T) {
			root := t.TempDir()
			ancestor := filepath.Join(root, "authority")
			parent := filepath.Join(ancestor, "store")
			if err := os.MkdirAll(parent, 0o700); err != nil {
				t.Fatal(err)
			}
			path := filepath.Join(parent, "data.db")
			durable, err := Open(path)
			if err != nil {
				t.Fatal(err)
			}
			defer durable.Close()

			replaced := parent
			if replacement == "ancestor" {
				replaced = ancestor
			}
			stale := replaced + ".stale"
			if err := os.Rename(replaced, stale); err != nil {
				t.Fatal(err)
			}
			if err := os.Symlink(stale, replaced); err != nil {
				t.Fatal(err)
			}

			_, err = durable.GetAggregate(t.Context(), store.AggregateKey{Type: "run", ID: "missing"})
			if !errors.Is(err, errUnsafePath) {
				t.Fatalf("%s symlink-back replacement error = %v", replacement, err)
			}
		})
	}
}

func TestOpenStoreDetectsNewHardlinkAliases(t *testing.T) {
	for _, target := range []string{"data", "lock"} {
		t.Run(target, func(t *testing.T) {
			root := t.TempDir()
			path := filepath.Join(root, "data.db")
			durable, err := Open(path)
			if err != nil {
				t.Fatal(err)
			}
			defer durable.Close()
			targetPath := path
			if target == "lock" {
				targetPath += lockSuffix
			}
			if err := os.Link(targetPath, filepath.Join(root, target+"-alias")); err != nil {
				t.Fatal(err)
			}
			if err := durable.append(emptyState()); !errors.Is(err, errUnsafePath) {
				t.Fatalf("new %s hardlink error = %v", target, err)
			}
		})
	}
}

func TestV2FrameLayoutAndNoLegacyFallback(t *testing.T) {
	frame, err := encodeFrame(emptyState())
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(frame[:8], frameMagic[:]) || !bytes.Equal(frame[len(frame)-frameFooter:len(frame)-frameFooter+8], frameCommitMagic[:]) {
		t.Fatalf("v2 magic is absent")
	}
	if binary.BigEndian.Uint16(frame[8:10]) != frameFormatVersion {
		t.Fatalf("frame version = %d", binary.BigEndian.Uint16(frame[8:10]))
	}
	payloadLength := binary.BigEndian.Uint64(frame[framePayloadLength : framePayloadLength+8])
	footer := frame[frameHeader+payloadLength:]
	if footerPayload := binary.BigEndian.Uint64(footer[footerPayloadLength : footerPayloadLength+8]); footerPayload != payloadLength {
		t.Fatalf("footer payload length = %d, header = %d", footerPayload, payloadLength)
	}
	encodedLength := binary.BigEndian.Uint64(frame[frameEncodedLength : frameEncodedLength+8])
	if footerEncoded := binary.BigEndian.Uint64(footer[footerEncodedLength : footerEncodedLength+8]); footerEncoded != encodedLength || encodedLength != uint64(len(frame)) {
		t.Fatalf("repeated encoded lengths header=%d footer=%d actual=%d", encodedLength, footerEncoded, len(frame))
	}
	legacyState := emptyState()
	legacyState.Version = 1
	legacyPayload, err := canonicaljson.MarshalWithLimits(legacyState, frameJSONLimits())
	if err != nil {
		t.Fatal(err)
	}
	legacy := make([]byte, 8+len(legacyPayload)+sha256.Size)
	binary.BigEndian.PutUint64(legacy[:8], uint64(len(legacyPayload)))
	copy(legacy[8:], legacyPayload)
	sum := sha256.Sum256(legacyPayload)
	copy(legacy[8+len(legacyPayload):], sum[:])
	path := filepath.Join(t.TempDir(), "legacy.db")
	if err := os.WriteFile(path, legacy, 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := Open(path); !errors.Is(err, store.ErrCorrupt) {
		t.Fatalf("legacy v1 fallback error = %v", err)
	}
	v1PayloadInV2Frame, err := encodeFrame(legacyState)
	if err != nil {
		t.Fatal(err)
	}
	path = filepath.Join(t.TempDir(), "v1-state-v2-frame.db")
	if err := os.WriteFile(path, v1PayloadInV2Frame, 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := Open(path); !errors.Is(err, store.ErrCorrupt) {
		t.Fatalf("v1 state inside v2 frame error = %v", err)
	}
}

func TestV2FrameLengthAndCorruptionMatrix(t *testing.T) {
	frame, err := encodeFrame(emptyState())
	if err != nil {
		t.Fatal(err)
	}
	payloadLength := int(binary.BigEndian.Uint64(frame[framePayloadLength : framePayloadLength+8]))
	footerStart := frameHeader + payloadLength
	tests := []struct {
		name   string
		mutate func([]byte) []byte
	}{
		{"header-magic", flipAt(0)},
		{"header-version", flipAt(9)},
		{"header-length", flipAt(11)},
		{"header-payload-length-zero", setUint64(framePayloadLength, 0)},
		{"header-payload-length-minus-one", setUint64(framePayloadLength, uint64(payloadLength-1))},
		{"header-payload-length-plus-one", setUint64(framePayloadLength, uint64(payloadLength+1))},
		{"header-payload-length-over-max", setUint64(framePayloadLength, maxFrameBytes+1)},
		{"header-frame-length", flipAt(frameEncodedLength + 7)},
		{"header-payload-digest", flipAt(framePayloadDigest)},
		{"header-digest", flipAt(frameHeaderCore)},
		{"payload", flipAt(frameHeader)},
		{"footer-magic", flipAt(footerStart)},
		{"footer-version", flipAt(footerStart + 9)},
		{"footer-length", flipAt(footerStart + 11)},
		{"footer-payload-length", flipAt(footerStart + footerPayloadLength + 7)},
		{"footer-frame-length", flipAt(footerStart + footerEncodedLength + 7)},
		{"footer-payload-digest", flipAt(footerStart + footerPayloadDigest)},
		{"footer-header-digest", flipAt(footerStart + footerHeaderDigest)},
		{"footer-digest", flipAt(len(frame) - 1)},
		{"only-minus-one", func(raw []byte) []byte { return append([]byte(nil), raw[:len(raw)-1]...) }},
		{"tail-plus-one", func(raw []byte) []byte { return append(append([]byte(nil), raw...), 0) }},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "frame.db")
			raw := test.mutate(append([]byte(nil), frame...))
			if err := os.WriteFile(path, raw, 0o600); err != nil {
				t.Fatal(err)
			}
			durable, err := Open(path)
			if test.name == "only-minus-one" || test.name == "tail-plus-one" {
				if err != nil {
					t.Fatalf("recoverable crash tail = %v", err)
				}
				_ = durable.Close()
				info, statErr := os.Stat(path)
				wantSize := int64(0)
				if test.name == "tail-plus-one" {
					wantSize = int64(len(frame))
				}
				if statErr != nil || info.Size() != wantSize {
					t.Fatalf("recovered bytes=%d want=%d err=%v", info.Size(), wantSize, statErr)
				}
				return
			}
			if !errors.Is(err, store.ErrCorrupt) {
				if durable != nil {
					_ = durable.Close()
				}
				t.Fatalf("corruption error = %v", err)
			}
		})
	}
}

func TestV2OnlyAndLatestDeclaredLengthMatrix(t *testing.T) {
	frame, err := encodeFrame(emptyState())
	if err != nil {
		t.Fatal(err)
	}
	payloadLength := binary.BigEndian.Uint64(frame[framePayloadLength : framePayloadLength+8])
	tests := []struct {
		name          string
		payloadLength uint64
		encodedLength uint64
	}{
		{"minus-one", payloadLength - 1, uint64(len(frame) - 1)},
		{"zero", 0, frameHeader + frameFooter},
		{"plus-one", payloadLength + 1, uint64(len(frame) + 1)},
		{"over-max", maxFrameBytes + 1, maxEncodedFrameBytes + 1},
	}
	for _, position := range []string{"only", "latest"} {
		for _, test := range tests {
			t.Run(position+"-"+test.name, func(t *testing.T) {
				mutated := append([]byte(nil), frame...)
				binary.BigEndian.PutUint64(mutated[framePayloadLength:framePayloadLength+8], test.payloadLength)
				binary.BigEndian.PutUint64(mutated[frameEncodedLength:frameEncodedLength+8], test.encodedLength)
				headerSum := sha256.Sum256(mutated[:frameHeaderCore])
				copy(mutated[frameHeaderCore:frameHeader], headerSum[:])
				raw := mutated
				if position == "latest" {
					raw = append(append([]byte(nil), frame...), mutated...)
				}
				path := filepath.Join(t.TempDir(), "length.db")
				if err := os.WriteFile(path, raw, 0o600); err != nil {
					t.Fatal(err)
				}
				if durable, err := Open(path); !errors.Is(err, store.ErrCorrupt) {
					if durable != nil {
						_ = durable.Close()
					}
					t.Fatalf("declared length error = %v", err)
				}
			})
		}
	}
}

func TestCommittedCorruptionNeverRollsBackToOlderFrame(t *testing.T) {
	frame, err := encodeFrame(emptyState())
	if err != nil {
		t.Fatal(err)
	}
	for _, corruptIndex := range []int{0, 1} {
		t.Run(fmt.Sprintf("frame-%d", corruptIndex), func(t *testing.T) {
			raw := bytes.Repeat(frame, 2)
			raw[corruptIndex*len(frame)+frameHeader] ^= 0xff
			path := filepath.Join(t.TempDir(), "committed-corrupt.db")
			if err := os.WriteFile(path, raw, 0o600); err != nil {
				t.Fatal(err)
			}
			if durable, err := Open(path); !errors.Is(err, store.ErrCorrupt) {
				if durable != nil {
					_ = durable.Close()
				}
				t.Fatalf("committed corruption error = %v", err)
			}
		})
	}
}

func TestRecoveryHardBounds(t *testing.T) {
	frame, err := encodeFrame(emptyState())
	if err != nil {
		t.Fatal(err)
	}
	t.Run("frame-count", func(t *testing.T) {
		path := filepath.Join(t.TempDir(), "too-many.db")
		if err := os.WriteFile(path, bytes.Repeat(frame, retainedFrameBudget+1), 0o600); err != nil {
			t.Fatal(err)
		}
		if durable, err := Open(path); !errors.Is(err, store.ErrCorrupt) {
			if durable != nil {
				_ = durable.Close()
			}
			t.Fatalf("frame-count bound error = %v", err)
		}
	})
	t.Run("file-size", func(t *testing.T) {
		path := filepath.Join(t.TempDir(), "too-large.db")
		file, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0o600)
		if err != nil {
			t.Fatal(err)
		}
		if err := file.Truncate(maxRecoveryBytes + 1); err != nil {
			t.Fatal(err)
		}
		if err := file.Close(); err != nil {
			t.Fatal(err)
		}
		if durable, err := Open(path); !errors.Is(err, store.ErrCorrupt) {
			if durable != nil {
				_ = durable.Close()
			}
			t.Fatalf("file-size bound error = %v", err)
		}
	})
}

func TestOpenDurabilityBarriersAndFailClosedFaults(t *testing.T) {
	t.Run("barrier-order", func(t *testing.T) {
		path := filepath.Join(t.TempDir(), "barriers.db")
		var observed []string
		hooks := &ioHooks{sync: func(operation string, file *os.File) error {
			observed = append(observed, operation)
			info, err := file.Stat()
			if err != nil {
				return err
			}
			if operation == "dir-sync" && !info.IsDir() {
				return errors.New("dir-sync did not target the pinned parent")
			}
			if operation != "dir-sync" && !info.Mode().IsRegular() {
				return errors.New("file sync did not target a regular file")
			}
			return file.Sync()
		}}
		durable, err := openWithHooks(path, hooks)
		if err != nil {
			t.Fatal(err)
		}
		if err := durable.Close(); err != nil {
			t.Fatal(err)
		}
		want := []string{"lock-sync", "data-sync", "dir-sync"}
		if fmt.Sprint(observed) != fmt.Sprint(want) {
			t.Fatalf("Open barriers = %v, want %v", observed, want)
		}
	})

	for _, operation := range []string{"lock-sync", "data-sync", "dir-sync"} {
		t.Run(operation+"-fault", func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "barrier-fault.db")
			injected := errors.New("injected " + operation)
			failed := false
			hooks := &ioHooks{sync: func(current string, file *os.File) error {
				if current == operation && !failed {
					failed = true
					return injected
				}
				return file.Sync()
			}}
			if durable, err := openWithHooks(path, hooks); durable != nil || !errors.Is(err, injected) {
				if durable != nil {
					_ = durable.Close()
				}
				t.Fatalf("faulted Open = %#v, %v", durable, err)
			}
			// Open's failure cleanup must release the sidecar and leave no
			// apparently successful handle behind.
			reopened, err := Open(path)
			if err != nil {
				t.Fatalf("reopen after barrier fault: %v", err)
			}
			if err := reopened.Close(); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestOpenRecoveryFaultsFailClosed(t *testing.T) {
	frame, err := encodeFrame(emptyState())
	if err != nil {
		t.Fatal(err)
	}
	for _, operation := range []string{"truncate", "data-sync"} {
		t.Run(operation, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "recovery-fault.db")
			if err := os.WriteFile(path, frame[:len(frame)-1], 0o600); err != nil {
				t.Fatal(err)
			}
			injected := errors.New("injected " + operation)
			hooks := &ioHooks{}
			if operation == "truncate" {
				hooks.truncate = func(*os.File, int64) error { return injected }
			} else {
				failed := false
				hooks.sync = func(current string, file *os.File) error {
					if current == "data-sync" && !failed {
						failed = true
						return injected
					}
					return file.Sync()
				}
			}
			if durable, err := openWithHooks(path, hooks); durable != nil || !errors.Is(err, injected) {
				if durable != nil {
					_ = durable.Close()
				}
				t.Fatalf("faulted recovery = %#v, %v", durable, err)
			}
			reopened, err := Open(path)
			if err != nil {
				t.Fatalf("reopen after recovery fault: %v", err)
			}
			_ = reopened.Close()
		})
	}
}

func TestWriteAndReplacementFaultsPoisonUntilReopen(t *testing.T) {
	injected := errors.New("injected durability fault")
	tests := []struct {
		name        string
		prepare     func(*testing.T, *Store)
		installHook func(*Store)
	}{
		{
			name: "write",
			installHook: func(durable *Store) {
				failed := false
				durable.hooks = &ioHooks{write: func(operation string, file *os.File, raw []byte) (int, error) {
					if operation == "data-write" && !failed {
						failed = true
						written, writeErr := file.Write(raw[:len(raw)/2])
						return written, errors.Join(injected, writeErr)
					}
					return file.Write(raw)
				}}
			},
		},
		{
			name:    "stage-write",
			prepare: appendFrames(retainedFrameBudget),
			installHook: func(durable *Store) {
				durable.hooks = &ioHooks{write: func(operation string, file *os.File, raw []byte) (int, error) {
					if operation == "stage-write" {
						return 0, injected
					}
					return file.Write(raw)
				}}
			},
		},
		{
			name: "data-sync",
			installHook: func(durable *Store) {
				durable.hooks = &ioHooks{sync: func(operation string, file *os.File) error {
					if operation == "data-sync" {
						return injected
					}
					return file.Sync()
				}}
			},
		},
		{
			name:    "rename",
			prepare: appendFrames(retainedFrameBudget),
			installHook: func(durable *Store) {
				durable.hooks = &ioHooks{rename: func(*os.File, string, string) error { return injected }}
			},
		},
		{
			name:    "dir-sync",
			prepare: appendFrames(retainedFrameBudget),
			installHook: func(durable *Store) {
				durable.hooks = &ioHooks{sync: func(operation string, file *os.File) error {
					if operation == "dir-sync" {
						if syncErr := file.Sync(); syncErr != nil {
							return syncErr
						}
						return injected
					}
					return file.Sync()
				}}
			},
		},
		{
			name:    "replacement-close",
			prepare: appendFrames(retainedFrameBudget),
			installHook: func(durable *Store) {
				durable.hooks = &ioHooks{close: func(operation string, file *os.File) error {
					closeErr := file.Close()
					if operation == "replaced-data-close" {
						return errors.Join(injected, closeErr)
					}
					return closeErr
				}}
			},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "fault.db")
			durable, err := Open(path)
			if err != nil {
				t.Fatal(err)
			}
			if test.prepare != nil {
				test.prepare(t, durable)
			}
			test.installHook(durable)
			if err := durable.append(emptyState()); !errors.Is(err, injected) {
				t.Fatalf("first fault = %v", err)
			}
			if err := durable.append(emptyState()); err == nil || !strings.Contains(err.Error(), "uncertain") {
				t.Fatalf("poisoned write = %v", err)
			}
			durable.hooks = nil
			if err := durable.Close(); err != nil {
				t.Fatal(err)
			}
			reopened, err := Open(path)
			if err != nil {
				t.Fatalf("reopen after %s fault: %v", test.name, err)
			}
			if err := reopened.Close(); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestUnlockAndCloseFaultsStillReleaseForReopen(t *testing.T) {
	injected := errors.New("injected close fault")
	for _, operation := range []string{"unlock", "data-close", "lock-close", "parent-close"} {
		t.Run(operation, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "close.db")
			durable, err := Open(path)
			if err != nil {
				t.Fatal(err)
			}
			durable.hooks = &ioHooks{
				flock: func(current string, file *os.File, how int) error {
					flockErr := syscall.Flock(int(file.Fd()), how)
					if current == operation {
						return errors.Join(injected, flockErr)
					}
					return flockErr
				},
				close: func(current string, file *os.File) error {
					closeErr := file.Close()
					if current == operation {
						return errors.Join(injected, closeErr)
					}
					return closeErr
				},
			}
			if err := durable.Close(); !errors.Is(err, injected) {
				t.Fatalf("Close fault = %v", err)
			}
			reopened, err := Open(path)
			if err != nil {
				t.Fatalf("reopen after %s fault: %v", operation, err)
			}
			_ = reopened.Close()
		})
	}
}

func appendFrames(count int) func(*testing.T, *Store) {
	return func(t *testing.T, durable *Store) {
		t.Helper()
		for index := 0; index < count; index++ {
			if err := durable.append(emptyState()); err != nil {
				t.Fatal(err)
			}
		}
	}
}

func TestV2PartialCrashTailBoundaries(t *testing.T) {
	frame, err := encodeFrame(emptyState())
	if err != nil {
		t.Fatal(err)
	}
	for _, cut := range []int{
		1, len(frameMagic), frameHeader - 1, frameHeader, frameHeader + 1,
		len(frame) - frameFooter, len(frame) - frameFooter + 1,
		len(frame) - frameFooter/2, len(frame) - 1,
	} {
		t.Run(fmt.Sprintf("cut-%d", cut), func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "partial.db")
			if err := os.WriteFile(path, frame[:cut], 0o600); err != nil {
				t.Fatal(err)
			}
			durable, err := Open(path)
			if err != nil {
				t.Fatalf("partial frame failed recovery: %v", err)
			}
			if err := durable.Close(); err != nil {
				t.Fatal(err)
			}
			info, err := os.Stat(path)
			if err != nil || info.Size() != 0 {
				t.Fatalf("partial recovery bytes=%d err=%v", info.Size(), err)
			}
		})
	}
}

func flipAt(offset int) func([]byte) []byte {
	return func(raw []byte) []byte {
		raw[offset] ^= 0xff
		return raw
	}
}

func setUint64(offset int, value uint64) func([]byte) []byte {
	return func(raw []byte) []byte {
		binary.BigEndian.PutUint64(raw[offset:offset+8], value)
		return raw
	}
}

func TestOpenValidatesAndCompactsRetainedSnapshots(t *testing.T) {
	path := filepath.Join(t.TempDir(), "seed.wal")
	durable, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	t0 := time.Date(2026, 8, 13, 11, 0, 0, 0, time.UTC)
	decision, err := execution.AdmitRun(execution.AdmitRunCommand{
		RunID: "run-compaction", NamespaceID: "lab",
		ActionRef:        contracts.ActionRef{ActionID: "workflow", Version: "v1", Digest: fixtureDigest},
		ExecutionPlanRef: "plan-1", PlanDigest: fixtureDigest, TriggerRef: "trigger-1", TriggerDigest: fixtureDigest,
		InputDigest: fixtureDigest, ActorRef: "operator", SourceRef: "api", CommandID: "admit-compaction", At: t0,
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := durable.Commit(t.Context(), runTransaction(t, decision, 0, "admit-compaction", t0)); err != nil {
		t.Fatal(err)
	}
	if err := durable.Close(); err != nil {
		t.Fatal(err)
	}
	frame, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	const retainedFrames = retainedFrameBudget
	journal := bytes.Repeat(frame, retainedFrames)
	if err := os.WriteFile(path, journal, 0o600); err != nil {
		t.Fatal(err)
	}
	durable, err = Open(path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = durable.Close() })
	info, err := os.Stat(path)
	if err != nil || info.Size() != int64(len(frame)) {
		t.Fatalf("compacted journal bytes=%d want=%d err=%v", info.Size(), len(frame), err)
	}
	if _, err := Open(path); !errors.Is(err, store.ErrLocked) {
		t.Fatalf("replacement inode lost the process lock: %v", err)
	}
	recovered, err := durable.GetAggregate(t.Context(), store.AggregateKey{Type: "run", ID: decision.Run.RunID})
	if err != nil || recovered.Revision != decision.Run.Revision {
		t.Fatalf("recovered latest aggregate=%#v err=%v", recovered, err)
	}
}

func TestAppendKeepsOnlyBoundedFullSnapshotHistory(t *testing.T) {
	path := filepath.Join(t.TempDir(), "bounded.wal")
	durable, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer durable.Close()
	frame, err := encodeFrame(emptyState())
	if err != nil {
		t.Fatal(err)
	}
	for index := 0; index < retainedFrameBudget; index++ {
		if err := durable.append(emptyState()); err != nil {
			t.Fatal(err)
		}
	}
	if durable.frameCount != retainedFrameBudget {
		t.Fatalf("frame count before boundary = %d", durable.frameCount)
	}
	if err := durable.append(emptyState()); err != nil {
		t.Fatal(err)
	}
	if durable.frameCount != 1 {
		t.Fatalf("fifth frame was not compacted first: count=%d", durable.frameCount)
	}
	for index := 0; index < 95; index++ {
		if err := durable.append(emptyState()); err != nil {
			t.Fatal(err)
		}
	}
	info, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	if info.Size() > int64(len(frame)*retainedFrameBudget) {
		t.Fatalf("bounded journal grew to %d bytes for a %d-byte frame", info.Size(), len(frame))
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

func TestForkStateUsesCopyOnWriteIndexes(t *testing.T) {
	original := emptyState()
	original.Aggregates["fixture\x00frozen"] = store.AggregateRecord{
		Key: store.AggregateKey{Type: "fixture", ID: "frozen"}, Revision: 1,
		PayloadDigest: fixtureDigest, Payload: json.RawMessage(`{"value":"frozen"}`),
	}
	original.Events["event-frozen"] = contracts.DomainEvent{EventID: "event-frozen"}
	original.Commands["command-frozen"] = commandRecord{IdentityDigest: fixtureDigest}
	original.Intents["intent-frozen"] = store.IntentRecord{Revision: 1}
	original.Inbox["source\x00message"] = store.InboxRecord{SourceRef: "source", MessageID: "message"}

	next := forkState(original)
	delete(next.Aggregates, "fixture\x00frozen")
	delete(next.Events, "event-frozen")
	delete(next.Commands, "command-frozen")
	delete(next.Intents, "intent-frozen")
	delete(next.Inbox, "source\x00message")
	next.Aggregates["fixture\x00mutable"] = store.AggregateRecord{
		Key: store.AggregateKey{Type: "fixture", ID: "mutable"}, Revision: 1,
	}

	if len(original.Aggregates) != 1 || original.Aggregates["fixture\x00frozen"].Key.ID != "frozen" ||
		len(original.Events) != 1 || len(original.Commands) != 1 || len(original.Intents) != 1 || len(original.Inbox) != 1 {
		t.Fatalf("fork mutated published state: %#v", original)
	}
	if _, exists := original.Aggregates["fixture\x00mutable"]; exists {
		t.Fatal("forked aggregate index aliases the published index")
	}
}

func TestCheckpointEncodingPreservesCanonicalRecordIdentityAcrossRestart(t *testing.T) {
	path := filepath.Join(t.TempDir(), "record-identity.wal")
	durable, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	payload, err := canonicaljson.Marshal(map[string]any{"value": "<>&\u2028"})
	if err != nil {
		t.Fatal(err)
	}
	payloadDigest, err := canonicaljson.Digest(payload)
	if err != nil {
		t.Fatal(err)
	}
	eventPayload := map[string]any{"fixtureId": "record-identity"}
	eventPayloadDigest, err := canonicaljson.DigestValue(eventPayload)
	if err != nil {
		t.Fatal(err)
	}
	key := store.AggregateKey{Type: "fixture", ID: "record-identity"}
	eventID, err := execution.StableEventID(key.Type, key.ID, 1)
	if err != nil {
		t.Fatal(err)
	}
	transaction := store.Transaction{
		CommandID: "persist-record-identity", IdentityDigest: fixtureDigest,
		Expected: []store.ExpectedRevision{{Key: key}},
		Mutations: []store.AggregateRecord{{
			Key: key, Revision: 1, PayloadDigest: payloadDigest, Payload: payload,
		}},
		Events: []contracts.DomainEvent{{
			EventID: eventID, AggregateType: key.Type, AggregateID: key.ID, AggregateRevision: 1,
			Type: "fixture.created", CommandID: "persist-record-identity",
			PayloadSchemaDigest: fixtureDigest, PayloadDigest: eventPayloadDigest, Payload: eventPayload,
			OccurredAt: time.Date(2026, 8, 13, 23, 0, 0, 0, time.UTC),
		}},
		Outcome: json.RawMessage(`{"stored":true}`), At: time.Date(2026, 8, 13, 23, 0, 0, 0, time.UTC),
	}
	if _, err := durable.Commit(t.Context(), transaction); err != nil {
		t.Fatal(err)
	}
	if err := durable.Close(); err != nil {
		t.Fatal(err)
	}
	durable, err = Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer durable.Close()
	recovered, err := durable.GetAggregate(t.Context(), key)
	if err != nil {
		t.Fatal(err)
	}
	recoveredDigest, err := canonicaljson.Digest(recovered.Payload)
	if err != nil || recovered.PayloadDigest != payloadDigest || recoveredDigest != payloadDigest {
		t.Fatalf("recovered identity payload=%s envelope=%s calculated=%s err=%v", recovered.Payload, recovered.PayloadDigest, recoveredDigest, err)
	}
}

var benchmarkDiskState diskState

func BenchmarkForkStateWithLargeFrozenAggregate(b *testing.B) {
	state := largeBenchmarkState(b)
	b.ReportAllocs()
	b.SetBytes(int64(len(state.Aggregates["fixture\x00frozen"].Payload)))
	for b.Loop() {
		next := forkState(state)
		next.Inbox["source\x00message"] = store.InboxRecord{SourceRef: "source", MessageID: "message"}
		benchmarkDiskState = next
	}
}

func BenchmarkEncodeFrameWithLargeFrozenAggregate(b *testing.B) {
	state := largeBenchmarkState(b)
	frame, err := encodeFrame(state)
	if err != nil {
		b.Fatal(err)
	}
	frameBytes := len(frame)
	b.ReportAllocs()
	b.SetBytes(int64(len(state.Aggregates["fixture\x00frozen"].Payload)))
	b.ResetTimer()
	for b.Loop() {
		if _, err := encodeFrame(state); err != nil {
			b.Fatal(err)
		}
	}
	b.ReportMetric(float64(frameBytes), "frame-bytes")
}

func largeBenchmarkState(tb testing.TB) diskState {
	tb.Helper()
	payload, err := canonicaljson.MarshalWithLimits(
		map[string]any{"definition": []any{
			strings.Repeat("frozen-workflow-spec-", 30_000),
			strings.Repeat("frozen-workflow-spec-", 30_000),
			strings.Repeat("frozen-workflow-spec-", 30_000),
			strings.Repeat("frozen-workflow-spec-", 30_000),
		}},
		frameJSONLimits(),
	)
	if err != nil {
		tb.Fatal(err)
	}
	payloadSum := sha256.Sum256(payload)
	payloadDigest := "sha256:" + fmt.Sprintf("%x", payloadSum[:])
	state := emptyState()
	state.Aggregates["fixture\x00frozen"] = store.AggregateRecord{
		Key: store.AggregateKey{Type: "fixture", ID: "frozen"}, Revision: 1,
		PayloadDigest: payloadDigest, Payload: payload,
	}
	return state
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
