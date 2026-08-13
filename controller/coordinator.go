package controller

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"time"

	"github.com/lxk36/xgc2-orchestration-core/durable/store"
	"github.com/lxk36/xgc2-orchestration-core/durable/worker"
	effectport "github.com/lxk36/xgc2-orchestration-core/provider/effect"
	"github.com/lxk36/xgc2-orchestration-core/sdk/go/contracts"
)

var (
	// ErrEffectAdapterUnavailable means a Run is durably waiting for an Effect
	// whose provider is not installed in this host. The Run is left unchanged
	// so another conforming host can continue it later.
	ErrEffectAdapterUnavailable = errors.New("effect adapter is unavailable")
	// ErrCoordinatorStepLimit prevents a malformed or endlessly expanding
	// workflow from monopolising one host call.
	ErrCoordinatorStepLimit = errors.New("workflow coordinator step limit exceeded")
)

// EffectDispatchPlanner is the product policy boundary. It supplies command
// identity, authorization material, action and target fencing for one already
// prepared public Effect. Raw credentials are not persisted by the core.
type EffectDispatchPlanner interface {
	PlanEffectDispatch(context.Context, contracts.EffectRecord) (BeginEffectRequest, error)
}

// EffectCompensationPlanner supplies a fresh, deterministic command envelope
// for reversing one applied Effect. It is a host policy boundary because
// action names, credentials, and target fences are provider concerns.
type EffectCompensationPlanner interface {
	PlanEffectCompensation(context.Context, contracts.EffectRecord) (BeginEffectRequest, error)
}

type CoordinatorConfig struct {
	Controller    *Controller
	Store         store.Store
	Planner       EffectDispatchPlanner
	Credentials   EffectCredentialBroker
	Adapters      []EffectAdapter
	OwnerRef      string
	LeaseDuration time.Duration
	BatchLimit    int
	MaxSteps      int
	Clock         Clock
}

// Coordinator owns the reusable host loop around the pure/durable Controller:
// drive nodes, begin a prepared Effect, dispatch its outbox, consume its
// receipt, resume the node without replaying Execute, and continue the Run.
// Providers, policies and credentials remain host ports.
type Coordinator struct {
	controller    *Controller
	store         store.Store
	planner       EffectDispatchPlanner
	worker        worker.Worker
	adapterKinds  map[string]struct{}
	leaseDuration time.Duration
	batchLimit    int
	maxSteps      int
	clock         Clock
}

func NewCoordinator(config CoordinatorConfig) (*Coordinator, error) {
	if config.Controller == nil || config.Store == nil || !contracts.ValidIdentifier(config.OwnerRef) {
		return nil, errors.New("coordinator controller, store, and owner are required")
	}
	children, err := NewChildResolutionHandler(config.Controller)
	if err != nil {
		return nil, err
	}
	if config.Clock == nil {
		config.Clock = systemClock{}
	}
	if config.LeaseDuration <= 0 {
		config.LeaseDuration = 30 * time.Second
	}
	if config.BatchLimit <= 0 || config.BatchLimit > 1000 {
		config.BatchLimit = 100
	}
	if config.MaxSteps <= 0 || config.MaxSteps > 10000 {
		config.MaxSteps = 1000
	}
	kinds := make(map[string]struct{}, len(config.Adapters))
	handlers := map[contracts.DurableIntentKind]worker.Handler{
		contracts.IntentChildResolution: children,
		contracts.IntentCleanup:         InternalCleanupHandler{},
	}
	if len(config.Adapters) != 0 {
		if config.Planner == nil || config.Credentials == nil {
			return nil, errors.New("coordinator effect planner and credentials are required when adapters are installed")
		}
		outbox, outboxErr := NewEffectOutboxHandler(config.Controller, config.Credentials, config.Adapters...)
		if outboxErr != nil {
			return nil, outboxErr
		}
		waits, waitsErr := NewWaitResolutionHandler(config.Controller)
		if waitsErr != nil {
			return nil, waitsErr
		}
		handlers[contracts.IntentOutbox] = outbox
		handlers[contracts.IntentWaitResolution] = waits
		hasCompensator := false
		for _, adapter := range config.Adapters {
			if _, ok := adapter.(effectport.Compensator); ok {
				hasCompensator = true
				break
			}
		}
		if hasCompensator {
			compensationPlanner, ok := config.Planner.(EffectCompensationPlanner)
			if !ok {
				return nil, errors.New("coordinator effect planner does not implement compensation planning")
			}
			cleanup, cleanupErr := NewEffectCleanupHandler(config.Controller, compensationPlanner, config.Credentials, config.Adapters...)
			if cleanupErr != nil {
				return nil, cleanupErr
			}
			handlers[contracts.IntentCleanup] = cleanup
		}
	}
	for _, adapter := range config.Adapters {
		kinds[adapter.Descriptor().Kind] = struct{}{}
	}
	return &Coordinator{
		controller: config.Controller, store: config.Store, planner: config.Planner,
		worker: worker.Worker{
			Store: config.Store, OwnerRef: config.OwnerRef,
			Handlers: handlers,
		},
		adapterKinds: kinds, leaseDuration: config.LeaseDuration,
		batchLimit: config.BatchLimit, maxSteps: config.MaxSteps, clock: config.Clock,
	}, nil
}

// AdvanceRun makes bounded durable progress. A terminal Run is returned
// immediately. Adapter unavailability and live attempt ownership are reported
// without mutating or failing the Run; callers may retry or route it elsewhere.
func (coordinator *Coordinator) AdvanceRun(ctx context.Context, runID string) (contracts.Run, error) {
	if coordinator == nil || ctx == nil || !contracts.ValidIdentifier(runID) {
		return contracts.Run{}, errors.New("coordinator context or run identity is invalid")
	}
	for step := 0; step < coordinator.maxSteps; step++ {
		run, driveErr := coordinator.controller.Drive(ctx, runID)
		if driveErr != nil && !errors.Is(driveErr, ErrRunWaiting) && !errors.Is(driveErr, ErrRunClosureOpen) {
			return run, driveErr
		}
		if run.Status.Terminal() {
			return run, nil
		}
		if errors.Is(driveErr, ErrRunClosureOpen) {
			progress := 0
			for _, item := range []struct {
				kind  contracts.DurableIntentKind
				phase string
			}{
				{contracts.IntentOutbox, "stopping-outbox"},
				{contracts.IntentWaitResolution, "stopping-wait"},
				{contracts.IntentChildResolution, "stopping-child"},
				{contracts.IntentCleanup, "cleanup"},
			} {
				batch, batchErr := coordinator.runBatch(ctx, item.kind, item.phase)
				if batchErr != nil {
					return run, batchErr
				}
				progress += batch.Claimed
			}
			if progress == 0 {
				return run, driveErr
			}
			continue
		}
		if run.Status != contracts.RunWaiting {
			return run, driveErr
		}
		snapshot, _, err := coordinator.controller.GetSnapshot(ctx, runID)
		if err != nil {
			return run, err
		}
		if snapshot.ActionCall != nil {
			child, childErr := coordinator.AdvanceRun(ctx, snapshot.ActionCall.ChildRunID)
			if childErr != nil && !errors.Is(childErr, ErrRunWaiting) &&
				!errors.Is(childErr, ErrAttemptLeaseActive) && !errors.Is(childErr, ErrRunClosureOpen) {
				return run, childErr
			}
			if !child.Status.Terminal() {
				return run, ErrRunWaiting
			}
			if _, err := coordinator.runBatch(ctx, contracts.IntentChildResolution, "child"); err != nil {
				return run, err
			}
			continue
		}
		if snapshot.Waiting == nil || snapshot.Waiting.Wait == nil {
			return run, errors.New("waiting run has no durable wait in its snapshot")
		}
		if snapshot.Waiting.Wait.Kind != contracts.NodeWaitEffect {
			// Event, approval, and timer waits are resolved by their explicit
			// ingress/provider authority. Advancing a Run must never invent the
			// observation merely because a coordinator is active.
			return run, ErrRunWaiting
		}

		effectRecord, err := coordinator.waitedEffect(ctx, runID)
		if err != nil {
			return run, err
		}
		if effectRecord.State == contracts.EffectPrepared {
			if _, installed := coordinator.adapterKinds[effectRecord.Intent.Kind]; !installed {
				return run, fmt.Errorf("%w: %s", ErrEffectAdapterUnavailable, effectRecord.Intent.Kind)
			}
			begin, planErr := coordinator.planner.PlanEffectDispatch(ctx, effectRecord)
			if planErr != nil {
				return run, planErr
			}
			if begin.EffectID == "" {
				begin.EffectID = effectRecord.EffectID
			}
			if begin.EffectID != effectRecord.EffectID {
				return run, errors.New("effect dispatch planner changed the prepared effect identity")
			}
			if _, beginErr := coordinator.controller.BeginEffect(ctx, begin); beginErr != nil {
				return run, beginErr
			}
		}

		if _, err := coordinator.runBatch(ctx, contracts.IntentOutbox, "outbox"); err != nil {
			return run, err
		}
		if _, err := coordinator.runBatch(ctx, contracts.IntentWaitResolution, "wait"); err != nil {
			return run, err
		}
		current, err := coordinator.controller.GetRun(ctx, runID)
		if err != nil {
			return contracts.Run{}, err
		}
		if current.Status.Terminal() {
			return current, nil
		}
	}
	current, err := coordinator.controller.GetRun(ctx, runID)
	if err != nil {
		return contracts.Run{}, err
	}
	return current, ErrCoordinatorStepLimit
}

func (coordinator *Coordinator) waitedEffect(ctx context.Context, runID string) (contracts.EffectRecord, error) {
	snapshot, _, err := coordinator.controller.GetSnapshot(ctx, runID)
	if err != nil {
		return contracts.EffectRecord{}, err
	}
	if snapshot.Waiting == nil || snapshot.Waiting.Wait == nil || snapshot.Waiting.Wait.Kind != contracts.NodeWaitEffect {
		return contracts.EffectRecord{}, errors.New("waiting run has no effect wait in its snapshot")
	}
	waitRef := snapshot.Waiting.Wait.SubjectRef
	after := ""
	for {
		effects, listErr := coordinator.controller.ListEffects(ctx, after, 500)
		if listErr != nil {
			return contracts.EffectRecord{}, listErr
		}
		for _, current := range effects {
			if current.Intent.RunID == runID && current.Intent.EffectKey == waitRef {
				return current, nil
			}
		}
		if len(effects) < 500 {
			break
		}
		after = effects[len(effects)-1].EffectID
	}
	return contracts.EffectRecord{}, errors.New("waiting run has no matching durable effect")
}

func (coordinator *Coordinator) runBatch(ctx context.Context, kind contracts.DurableIntentKind, phase string) (worker.BatchResult, error) {
	now := coordinator.clock.Now().UTC()
	token, err := coordinatorToken(phase)
	if err != nil {
		return worker.BatchResult{}, err
	}
	return coordinator.worker.RunOnce(ctx, worker.Batch{
		Kinds: []contracts.DurableIntentKind{kind}, LeaseToken: token,
		Now: now, LeaseExpiresAt: now.Add(coordinator.leaseDuration), Limit: coordinator.batchLimit,
	})
}

func coordinatorToken(phase string) (string, error) {
	raw := make([]byte, 16)
	if _, err := rand.Read(raw); err != nil {
		return "", err
	}
	return "coordinator-" + phase + "-" + hex.EncodeToString(raw), nil
}
