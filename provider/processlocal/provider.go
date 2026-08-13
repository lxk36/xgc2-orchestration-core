//go:build linux

// Package processlocal is the Linux local-process provider. It creates a new
// process group, records PID/PGID/startTicks identity, validates identity before
// every signal, applies TERM then KILL, and never invokes a shell.
package processlocal

import (
	"context"
	"errors"
	"fmt"
	"os/exec"
	"sync"
	"syscall"
	"time"

	"github.com/lxk36/xgc2-orchestration-core/kernel/canonicaljson"
	"github.com/lxk36/xgc2-orchestration-core/kernel/effect"
	processport "github.com/lxk36/xgc2-orchestration-core/provider/process"
	"github.com/lxk36/xgc2-orchestration-core/sdk/go/contracts"
)

type group struct {
	identity contracts.ProcessIdentity
	command  *exec.Cmd
	done     chan exit
}

type exit struct {
	code int
	err  error
	at   time.Time
}

type Provider struct {
	ProviderRef    string
	ProviderDigest string
	mu             sync.Mutex
	commands       map[string]contracts.CommandLedger
	groups         map[string]*group
	fences         map[string]contracts.GenerationFence
}

func New(providerRef, providerDigest string) (*Provider, error) {
	if !contracts.ValidIdentifier(providerRef) || !contracts.ValidDigest(providerDigest) {
		return nil, errors.New("local process provider identity or digest is invalid")
	}
	return &Provider{
		ProviderRef: providerRef, ProviderDigest: providerDigest,
		commands: map[string]contracts.CommandLedger{}, groups: map[string]*group{}, fences: map[string]contracts.GenerationFence{},
	}, nil
}

func (provider *Provider) Start(ctx context.Context, dispatch processport.Dispatch) (processport.Result, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if err := validateDispatch(dispatch, processport.ActionStart); err != nil {
		return processport.Result{}, err
	}
	provider.mu.Lock()
	defer provider.mu.Unlock()
	if replay, conflict := provider.replay(dispatch.Envelope); replay != nil || conflict != nil {
		return processport.Result{Ledger: cloneLedger(*replay)}, conflict
	}
	if err := provider.acceptFence(dispatch.Envelope.Fence.Generation); err != nil {
		return provider.rejected(dispatch, "runtime.stale-fence", err)
	}
	if existing := provider.groups[dispatch.Envelope.TargetRef]; existing != nil {
		live, err := alive(existing.identity)
		if err != nil {
			return provider.uncertain(dispatch, "process.existing-inspect", err, &existing.identity)
		}
		if live {
			return provider.rejected(dispatch, "process.already-running", errors.New("runtime binding already has a live process group"))
		}
		delete(provider.groups, dispatch.Envelope.TargetRef)
	}
	stdout, err := openLog(dispatch.StdoutPath)
	if err != nil {
		return provider.rejected(dispatch, "process.stdout-open", err)
	}
	defer stdout.Close()
	stderr, err := openLog(dispatch.StderrPath)
	if err != nil {
		return provider.rejected(dispatch, "process.stderr-open", err)
	}
	defer stderr.Close()
	command := exec.CommandContext(context.WithoutCancel(ctx), dispatch.Executable, dispatch.Arguments...)
	command.Dir = dispatch.WorkingDirectory
	command.Env = append([]string(nil), dispatch.Environment...)
	command.Stdout = stdout
	command.Stderr = stderr
	command.SysProcAttr = &syscall.SysProcAttr{Setpgid: true, Pdeathsig: syscall.SIGTERM}
	command.WaitDelay = time.Duration(dispatch.Spec.KillWaitMillis) * time.Millisecond
	if err := command.Start(); err != nil {
		return provider.rejected(dispatch, "process.start", err)
	}
	identity, err := readIdentity(command.Process.Pid)
	if err != nil {
		_ = syscall.Kill(-command.Process.Pid, syscall.SIGKILL)
		_ = command.Wait()
		return provider.uncertain(dispatch, "process.identity", err, nil)
	}
	managed := &group{identity: identity.ProcessIdentity, command: command, done: make(chan exit, 1)}
	go func() {
		err := command.Wait()
		code := 0
		if err != nil {
			code = -1
			var exitErr *exec.ExitError
			if errors.As(err, &exitErr) {
				code = exitErr.ExitCode()
			}
		}
		managed.done <- exit{code: code, err: err, at: time.Now().UTC()}
		close(managed.done)
	}()
	provider.groups[dispatch.Envelope.TargetRef] = managed
	observation, err := processObservation(dispatch.Envelope.Fence.Generation, managed.identity, contracts.RuntimeObservedRunning, contracts.RuntimeHealthHealthy, nil, dispatch.At)
	if err != nil {
		return processport.Result{}, err
	}
	ledger, err := provider.successLedger(dispatch, managed.identity, dispatch.At)
	if err != nil {
		return processport.Result{}, err
	}
	provider.commands[dispatch.Envelope.CommandID] = ledger
	return processport.Result{Ledger: cloneLedger(ledger), Observation: observation}, nil
}

func (provider *Provider) Stop(ctx context.Context, dispatch processport.Dispatch) (processport.Result, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if err := validateDispatch(dispatch, processport.ActionStop); err != nil {
		return processport.Result{}, err
	}
	provider.mu.Lock()
	defer provider.mu.Unlock()
	if replay, conflict := provider.replay(dispatch.Envelope); replay != nil || conflict != nil {
		return processport.Result{Ledger: cloneLedger(*replay)}, conflict
	}
	if err := provider.acceptFence(dispatch.Envelope.Fence.Generation); err != nil {
		return provider.rejected(dispatch, "runtime.stale-fence", err)
	}
	identity := *dispatch.KnownIdentity
	managed := provider.groups[dispatch.Envelope.TargetRef]
	if managed != nil && managed.identity != identity {
		return provider.rejected(dispatch, "process.identity-conflict", errors.New("known identity does not match provider group"))
	}
	live, err := alive(identity)
	if err != nil {
		return provider.uncertain(dispatch, "process.inspect", err, &identity)
	}
	if live {
		if err := signalGroup(identity, syscall.SIGTERM); err != nil {
			return provider.uncertain(dispatch, "process.term", err, &identity)
		}
		grace := time.NewTimer(time.Duration(dispatch.Spec.GracePeriodMillis) * time.Millisecond)
		defer grace.Stop()
		if managed != nil {
			select {
			case <-managed.done:
				live = false
			case <-grace.C:
			case <-ctx.Done():
			}
		} else {
			select {
			case <-grace.C:
			case <-ctx.Done():
			}
			live, err = alive(identity)
			if err != nil {
				return provider.uncertain(dispatch, "process.inspect-after-term", err, &identity)
			}
		}
	}
	if live {
		if err := signalGroup(identity, syscall.SIGKILL); err != nil {
			return provider.uncertain(dispatch, "process.kill", err, &identity)
		}
		deadline := time.NewTimer(time.Duration(dispatch.Spec.KillWaitMillis) * time.Millisecond)
		defer deadline.Stop()
		if managed != nil {
			select {
			case <-managed.done:
			case <-deadline.C:
				return provider.uncertain(dispatch, "process.reap-timeout", errors.New("process group did not reap after kill"), &identity)
			}
		} else {
			for {
				live, err = alive(identity)
				if err != nil {
					return provider.uncertain(dispatch, "process.inspect-after-kill", err, &identity)
				}
				if !live {
					break
				}
				select {
				case <-deadline.C:
					return provider.uncertain(dispatch, "process.kill-timeout", errors.New("process group remained alive after kill"), &identity)
				case <-time.After(10 * time.Millisecond):
				}
			}
		}
	}
	delete(provider.groups, dispatch.Envelope.TargetRef)
	observation, err := processObservation(dispatch.Envelope.Fence.Generation, identity, contracts.RuntimeObservedStopped, contracts.RuntimeHealthUnknown, intPointer(0), dispatch.At)
	if err != nil {
		return processport.Result{}, err
	}
	ledger, err := provider.successLedger(dispatch, identity, dispatch.At)
	if err != nil {
		return processport.Result{}, err
	}
	provider.commands[dispatch.Envelope.CommandID] = ledger
	return processport.Result{Ledger: cloneLedger(ledger), Observation: observation}, nil
}

func (provider *Provider) Inspect(ctx context.Context, request processport.InspectRequest) (contracts.ProcessObservation, error) {
	if err := ctx.Err(); err != nil {
		return contracts.ProcessObservation{}, err
	}
	if !contracts.ValidIdentifier(request.BindingID) || request.Generation == 0 || request.FencingToken == 0 || request.At.IsZero() {
		return contracts.ProcessObservation{}, errors.New("process inspect request is invalid")
	}
	provider.mu.Lock()
	defer provider.mu.Unlock()
	if err := provider.acceptFence(&contracts.GenerationFence{BindingID: request.BindingID, Generation: request.Generation, FencingToken: request.FencingToken}); err != nil {
		return contracts.ProcessObservation{}, err
	}
	live, err := alive(request.Identity)
	if err != nil {
		return contracts.ProcessObservation{}, err
	}
	state := contracts.RuntimeObservedStopped
	health := contracts.RuntimeHealthUnknown
	if live {
		state = contracts.RuntimeObservedRunning
		health = contracts.RuntimeHealthHealthy
	}
	fence := &contracts.GenerationFence{BindingID: request.BindingID, Generation: request.Generation, FencingToken: request.FencingToken}
	observation, err := processObservation(fence, request.Identity, state, health, nil, request.At)
	if err != nil {
		return contracts.ProcessObservation{}, err
	}
	return *observation, nil
}

func (provider *Provider) acceptFence(incoming *contracts.GenerationFence) error {
	if incoming == nil || !contracts.ValidIdentifier(incoming.BindingID) || incoming.Generation == 0 || incoming.FencingToken == 0 {
		return errors.New("process provider generation fence is invalid")
	}
	current, exists := provider.fences[incoming.BindingID]
	if exists {
		if incoming.Generation < current.Generation ||
			(incoming.Generation == current.Generation && incoming.FencingToken != current.FencingToken) ||
			(incoming.Generation > current.Generation && incoming.FencingToken <= current.FencingToken) {
			return fmt.Errorf("stale generation fence %d/%d; current is %d/%d", incoming.Generation, incoming.FencingToken, current.Generation, current.FencingToken)
		}
	}
	if !exists || incoming.Generation > current.Generation {
		provider.fences[incoming.BindingID] = *incoming
	}
	return nil
}

func (provider *Provider) replay(envelope contracts.CommandEnvelope) (*contracts.CommandLedger, error) {
	prior, exists := provider.commands[envelope.CommandID]
	if !exists {
		return nil, nil
	}
	if prior.Envelope.IdentityDigest != envelope.IdentityDigest || prior.Envelope.IdempotencyKeyHash != envelope.IdempotencyKeyHash {
		return &prior, effect.ErrCommandConflict
	}
	return &prior, nil
}

func (provider *Provider) successLedger(dispatch processport.Dispatch, identity contracts.ProcessIdentity, at time.Time) (contracts.CommandLedger, error) {
	resultDigest, err := canonicaljson.DigestValue(identity)
	if err != nil {
		return contracts.CommandLedger{}, err
	}
	accepted, err := provider.receipt(dispatch, 1, contracts.ReceiptAccepted, nil, "", at)
	if err != nil {
		return contracts.CommandLedger{}, err
	}
	succeeded, err := provider.receipt(dispatch, 2, contracts.ReceiptSucceeded, nil, resultDigest, at)
	if err != nil {
		return contracts.CommandLedger{}, err
	}
	succeeded.ExternalIdentity = processIdentityRef(identity)
	return contracts.CommandLedger{Envelope: publicEnvelope(dispatch.Envelope), Receipts: []contracts.CommandReceipt{accepted, succeeded}}, nil
}

func (provider *Provider) rejected(dispatch processport.Dispatch, code string, cause error) (processport.Result, error) {
	failure := &contracts.StructuredFailure{Class: contracts.FailurePermanent, Code: code, Message: cause.Error()}
	receipt, err := provider.receipt(dispatch, 1, contracts.ReceiptRejected, failure, "", dispatch.At)
	if err != nil {
		return processport.Result{}, err
	}
	ledger := contracts.CommandLedger{Envelope: publicEnvelope(dispatch.Envelope), Receipts: []contracts.CommandReceipt{receipt}}
	provider.commands[dispatch.Envelope.CommandID] = ledger
	return processport.Result{Ledger: cloneLedger(ledger)}, nil
}

func (provider *Provider) uncertain(dispatch processport.Dispatch, code string, cause error, identity *contracts.ProcessIdentity) (processport.Result, error) {
	failure := &contracts.StructuredFailure{Class: contracts.FailureUncertain, Code: code, Message: cause.Error()}
	accepted, err := provider.receipt(dispatch, 1, contracts.ReceiptAccepted, nil, "", dispatch.At)
	if err != nil {
		return processport.Result{}, err
	}
	receipt, err := provider.receipt(dispatch, 2, contracts.ReceiptUncertain, failure, "", dispatch.At)
	if err != nil {
		return processport.Result{}, err
	}
	if identity != nil {
		receipt.ExternalIdentity = processIdentityRef(*identity)
	}
	ledger := contracts.CommandLedger{Envelope: publicEnvelope(dispatch.Envelope), Receipts: []contracts.CommandReceipt{accepted, receipt}}
	provider.commands[dispatch.Envelope.CommandID] = ledger
	return processport.Result{Ledger: cloneLedger(ledger)}, nil
}

func (provider *Provider) receipt(dispatch processport.Dispatch, sequence uint32, status contracts.ReceiptStatus, failure *contracts.StructuredFailure, resultDigest string, at time.Time) (contracts.CommandReceipt, error) {
	id, err := effect.StableReceiptID(dispatch.Envelope.CommandID, sequence)
	if err != nil {
		return contracts.CommandReceipt{}, err
	}
	fenceDigest, err := effect.FenceDigest(dispatch.Envelope.Fence)
	if err != nil {
		return contracts.CommandReceipt{}, err
	}
	return contracts.CommandReceipt{
		ReceiptID: id, CommandID: dispatch.Envelope.CommandID, Sequence: sequence, Status: status,
		IdentityDigest: dispatch.Envelope.IdentityDigest, FenceDigest: fenceDigest, ProviderRef: provider.ProviderRef,
		ProviderDigest: provider.ProviderDigest, PolicyDigest: dispatch.Envelope.PolicyDigest,
		AuthorizationDigest: dispatch.AuthorizationDigest, ResultDigest: resultDigest,
		Failure: cloneFailure(failure), ObservedAt: at.UTC(),
	}, nil
}

func processObservation(fence *contracts.GenerationFence, identity contracts.ProcessIdentity, state contracts.RuntimeObservedState, health contracts.RuntimeHealth, exitCode *int, at time.Time) (*contracts.ProcessObservation, error) {
	identityPointer := &identity
	payload := map[string]any{
		"bindingId": fence.BindingID, "generation": fence.Generation, "fencingToken": fence.FencingToken,
		"identity": identity, "state": state, "health": health, "exitCode": exitCode,
	}
	digest, err := canonicaljson.DigestValue(payload)
	if err != nil {
		return nil, err
	}
	return &contracts.ProcessObservation{
		BindingID: fence.BindingID, Generation: fence.Generation, FencingToken: fence.FencingToken,
		Identity: identityPointer, State: state, Health: health, ExitCode: exitCode, ObservationDigest: digest, ObservedAt: at.UTC(),
	}, nil
}

func publicEnvelope(envelope contracts.CommandEnvelope) contracts.CommandEnvelope {
	envelope.IdempotencyKey = ""
	envelope.CapabilityToken = ""
	return envelope
}

func cloneLedger(ledger contracts.CommandLedger) contracts.CommandLedger {
	ledger.Envelope = publicEnvelope(ledger.Envelope)
	ledger.Receipts = append([]contracts.CommandReceipt(nil), ledger.Receipts...)
	for index := range ledger.Receipts {
		ledger.Receipts[index].Failure = cloneFailure(ledger.Receipts[index].Failure)
	}
	return ledger
}

func cloneFailure(failure *contracts.StructuredFailure) *contracts.StructuredFailure {
	if failure == nil {
		return nil
	}
	copy := *failure
	return &copy
}

func processIdentityRef(identity contracts.ProcessIdentity) string {
	return fmt.Sprintf("pid-%d-%d", identity.PID, identity.StartTicks)
}

func intPointer(value int) *int { return &value }

var _ processport.Provider = (*Provider)(nil)
