//go:build linux

package processlocal

import (
	"bufio"
	"context"
	"errors"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/lxk36/xgc2-orchestration-core/kernel/effect"
	"github.com/lxk36/xgc2-orchestration-core/kernel/execution"
	processport "github.com/lxk36/xgc2-orchestration-core/provider/process"
	"github.com/lxk36/xgc2-orchestration-core/sdk/go/contracts"
)

const testDigest = "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"

func TestStartInspectStopWholeProcessGroupAndReplay(t *testing.T) {
	provider, err := New("local-process", testDigest)
	if err != nil {
		t.Fatal(err)
	}
	t0 := time.Now().UTC()
	directory := t.TempDir()
	stdout := filepath.Join(directory, "stdout.log")
	stderr := filepath.Join(directory, "stderr.log")
	start := testDispatch(t, "command-start", processport.ActionStart, t0, stdout, stderr)
	start.Executable = "/bin/sh"
	start.Arguments = []string{"-c", "sleep 1000 & child=$!; echo \"$$ $child\"; wait $child"}
	start.Environment = []string{"PATH=/usr/bin:/bin"}
	result, err := provider.Start(context.Background(), start)
	if err != nil {
		t.Fatal(err)
	}
	if result.Observation == nil || result.Observation.Identity == nil || result.Observation.State != contracts.RuntimeObservedRunning || len(result.Ledger.Receipts) != 2 {
		t.Fatalf("start result = %#v", result)
	}
	if err := effect.ValidateLedger(result.Ledger); err != nil {
		t.Fatalf("start ledger: %v", err)
	}
	identity := *result.Observation.Identity
	if identity.PID <= 0 || identity.PGID != identity.PID || identity.StartTicks == 0 {
		t.Fatalf("process identity = %#v", identity)
	}
	parentPID, childPID := waitPIDs(t, stdout)
	if parentPID != identity.PID || childPID <= 0 {
		t.Fatalf("logged pids = %d %d, identity = %#v", parentPID, childPID, identity)
	}
	childIdentity, err := readIdentity(childPID)
	if err != nil || childIdentity.PGID != identity.PGID {
		t.Fatalf("child identity = %#v, err %v", childIdentity, err)
	}

	replayed, err := provider.Start(context.Background(), start)
	if err != nil || replayed.Observation != nil || len(replayed.Ledger.Receipts) != 2 || replayed.Ledger.Receipts[1].ExternalIdentity != result.Ledger.Receipts[1].ExternalIdentity {
		t.Fatalf("start replay = %#v, err %v", replayed, err)
	}
	conflict := start
	conflict.Envelope.PayloadDigest = "sha256:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
	conflict.Envelope.IdentityDigest, _ = effect.CommandIdentityDigest(conflict.Envelope)
	if _, err := provider.Start(context.Background(), conflict); !errors.Is(err, effect.ErrCommandConflict) {
		t.Fatalf("start identity conflict = %v", err)
	}

	observation, err := provider.Inspect(context.Background(), processport.InspectRequest{
		BindingID: start.Envelope.TargetRef, Generation: 1, FencingToken: 7, Identity: identity, At: t0.Add(time.Second),
	})
	if err != nil || observation.State != contracts.RuntimeObservedRunning {
		t.Fatalf("inspect = %#v, err %v", observation, err)
	}
	_, err = provider.Inspect(context.Background(), processport.InspectRequest{
		BindingID: start.Envelope.TargetRef, Generation: 2, FencingToken: 8, Identity: identity, At: t0.Add(2 * time.Second),
	})
	if err != nil {
		t.Fatal(err)
	}
	staleStop := testDispatch(t, "command-stale-stop", processport.ActionStop, t0.Add(3*time.Second), stdout, stderr)
	staleStop.KnownIdentity = &identity
	staleResult, err := provider.Stop(context.Background(), staleStop)
	if err != nil || len(staleResult.Ledger.Receipts) != 1 || staleResult.Ledger.Receipts[0].Status != contracts.ReceiptRejected {
		t.Fatalf("stale stop = %#v, err %v", staleResult, err)
	}
	if live, err := alive(identity); err != nil || !live {
		t.Fatalf("stale stop affected process: live=%v err=%v", live, err)
	}
	stop := testDispatch(t, "command-stop", processport.ActionStop, t0.Add(4*time.Second), stdout, stderr)
	stop.Envelope.Fence.Generation.Generation = 2
	stop.Envelope.Fence.Generation.FencingToken = 8
	stop.Envelope.IdentityDigest, err = effect.CommandIdentityDigest(stop.Envelope)
	if err != nil {
		t.Fatal(err)
	}
	stop.KnownIdentity = &identity
	stopped, err := provider.Stop(context.Background(), stop)
	if err != nil {
		t.Fatal(err)
	}
	if stopped.Observation == nil || stopped.Observation.State != contracts.RuntimeObservedStopped || len(stopped.Ledger.Receipts) != 2 {
		t.Fatalf("stop result = %#v", stopped)
	}
	if err := effect.ValidateLedger(stopped.Ledger); err != nil {
		t.Fatalf("stop ledger: %v", err)
	}
	if live, err := alive(identity); err != nil || live {
		t.Fatalf("parent live = %v, err %v", live, err)
	}
	if err := syscall.Kill(childPID, 0); err == nil {
		childAfter, identityErr := readIdentity(childPID)
		if identityErr == nil && childAfter.State != 'Z' {
			t.Fatalf("child %d remains alive after group stop: %#v", childPID, childAfter)
		}
	}
}

func TestStopRejectsReusedOrIncompleteIdentityWithoutSignaling(t *testing.T) {
	provider, err := New("local-process", testDigest)
	if err != nil {
		t.Fatal(err)
	}
	t0 := time.Now().UTC()
	directory := t.TempDir()
	dispatch := testDispatch(t, "command-invalid-stop", processport.ActionStop, t0, filepath.Join(directory, "out"), filepath.Join(directory, "err"))
	dispatch.KnownIdentity = &contracts.ProcessIdentity{PID: os.Getpid(), PGID: syscall.Getpgrp(), StartTicks: 1}
	result, err := provider.Stop(context.Background(), dispatch)
	if err != nil {
		t.Fatal(err)
	}
	if len(result.Ledger.Receipts) != 2 || result.Ledger.Receipts[1].Status != contracts.ReceiptSucceeded {
		t.Fatalf("stale identity stop result = %#v", result)
	}
	if result.Observation == nil || result.Observation.State != contracts.RuntimeObservedStopped {
		t.Fatalf("stale identity observation = %#v", result.Observation)
	}
}

func testDispatch(t *testing.T, commandID, action string, at time.Time, stdout, stderr string) processport.Dispatch {
	t.Helper()
	idempotencyKey := "key-" + commandID
	keyHash, err := execution.PrivateTokenDigest(idempotencyKey)
	if err != nil {
		t.Fatal(err)
	}
	capability := "capability-" + commandID
	capabilityHash, err := execution.PrivateTokenDigest(capability)
	if err != nil {
		t.Fatal(err)
	}
	envelope := contracts.CommandEnvelope{
		CommandID: commandID, EffectID: "effect-1", IdempotencyKey: idempotencyKey, IdempotencyKeyHash: keyHash,
		NamespaceID: "lab", TargetRef: "runtime-1", Action: action, ActorRef: "controller", SourceRef: "orchestrator",
		ReasonCode: "runtime.reconcile", Risk: contracts.RiskHigh,
		Fence:         contracts.TargetFence{Kind: contracts.FenceGeneration, Generation: &contracts.GenerationFence{BindingID: "runtime-1", Generation: 1, FencingToken: 7}},
		PayloadDigest: testDigest, PolicyDigest: testDigest, DescriptorDigest: testDigest,
		Deadline: at.Add(time.Minute), CancellationID: "cancel-1",
		RequiredCapabilityRefs: []string{"process.control"}, CapabilityToken: capability, CapabilityTokenHash: capabilityHash,
	}
	envelope.IdentityDigest, err = effect.CommandIdentityDigest(envelope)
	if err != nil {
		t.Fatal(err)
	}
	return processport.Dispatch{
		Envelope: envelope,
		Spec: contracts.ProcessSpec{
			ProcessID: "test-process", Version: "v1", DescriptorDigest: testDigest, ExecutableRef: "executable-1",
			ArgumentTemplateDigest: testDigest, StdoutArtifactRef: "stdout-1", StderrArtifactRef: "stderr-1",
			GracePeriodMillis: 100, KillWaitMillis: 1000,
		},
		StdoutPath: stdout, StderrPath: stderr, AuthorizationDigest: testDigest, At: at,
	}
}

func waitPIDs(t *testing.T, path string) (int, int) {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		file, err := os.Open(path)
		if err == nil {
			scanner := bufio.NewScanner(file)
			if scanner.Scan() {
				fields := strings.Fields(scanner.Text())
				_ = file.Close()
				if len(fields) == 2 {
					parent, parentErr := strconv.Atoi(fields[0])
					child, childErr := strconv.Atoi(fields[1])
					if parentErr == nil && childErr == nil {
						return parent, child
					}
				}
			} else {
				_ = file.Close()
			}
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("did not read process identities from %s", path)
	return 0, 0
}
