package controller

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/lxk36/xgc2-orchestration-core/durable/store"
	"github.com/lxk36/xgc2-orchestration-core/durable/worker"
	"github.com/lxk36/xgc2-orchestration-core/kernel/effect"
	effectport "github.com/lxk36/xgc2-orchestration-core/provider/effect"
	"github.com/lxk36/xgc2-orchestration-core/sdk/go/contracts"
)

type DispatchCredentials = effectport.DispatchCredentials
type EffectCredentialBroker = effectport.CredentialBroker
type EffectAdapterDescriptor = effectport.AdapterDescriptor
type EffectAdapter = effectport.Adapter

type EffectOutboxHandler struct {
	controller *Controller
	broker     EffectCredentialBroker
	adapters   map[string]EffectAdapter
}

func NewEffectOutboxHandler(controller *Controller, broker EffectCredentialBroker, adapters ...EffectAdapter) (*EffectOutboxHandler, error) {
	if controller == nil || broker == nil {
		return nil, errors.New("effect outbox controller and credential broker are required")
	}
	registered := make(map[string]EffectAdapter, len(adapters))
	for _, adapter := range adapters {
		if adapter == nil {
			return nil, errors.New("effect adapter is required")
		}
		descriptor := adapter.Descriptor()
		if !contracts.ValidTypeRef(descriptor.Kind) || !contracts.ValidIdentifier(descriptor.ProviderRef) || !contracts.ValidDigest(descriptor.ProviderDigest) {
			return nil, errors.New("effect adapter descriptor is invalid")
		}
		if _, duplicate := registered[descriptor.Kind]; duplicate {
			return nil, fmt.Errorf("effect adapter kind %q is duplicated", descriptor.Kind)
		}
		registered[descriptor.Kind] = adapter
	}
	return &EffectOutboxHandler{controller: controller, broker: broker, adapters: registered}, nil
}

func (handler *EffectOutboxHandler) Handle(ctx context.Context, claimed store.ClaimedIntent) worker.Result {
	if claimed.Record.Intent.Kind != contracts.IntentOutbox {
		return deadIntent("outbox.kind", "claimed intent is not an outbox item")
	}
	current, err := handler.controller.GetEffect(ctx, claimed.Record.Intent.AggregateID)
	if err != nil {
		return retryIntent(handler.controller.clock.Now(), "outbox.effect-load", err)
	}
	if current.State.Terminal() {
		return worker.Result{Disposition: worker.Complete}
	}
	if current.State != contracts.EffectApplying {
		return deadIntent("outbox.effect-state", "outbox does not target an applying effect")
	}
	ledger, err := handler.controller.GetCommandLedger(ctx, current.CommandID)
	if err != nil {
		return retryIntent(handler.controller.clock.Now(), "outbox.ledger-load", err)
	}
	adapter := handler.adapters[current.Intent.Kind]
	if adapter == nil {
		if err := handler.reject(ctx, current, ledger, EffectAdapterDescriptor{
			Kind: current.Intent.Kind, ProviderRef: handler.controller.ownerRef,
			ProviderDigest: current.Intent.DescriptorDigest,
		}, current.Intent.PolicyDigest, "adapter.not-installed", "no adapter is installed for the prepared effect kind"); err != nil {
			return retryIntent(handler.controller.clock.Now(), "outbox.reject", err)
		}
		return worker.Result{Disposition: worker.Complete}
	}
	descriptor := adapter.Descriptor()
	credentials, err := handler.broker.ResolveEffectCredentials(ctx, current, ledger)
	if err != nil {
		if observeErr := handler.reject(ctx, current, ledger, descriptor, current.Intent.PolicyDigest, "credential.resolve", err.Error()); observeErr != nil {
			return retryIntent(handler.controller.clock.Now(), "outbox.credential-reject", observeErr)
		}
		return worker.Result{Disposition: worker.Complete}
	}
	envelope := ledger.Envelope
	envelope.IdempotencyKey = credentials.IdempotencyKey
	envelope.CapabilityToken = credentials.CapabilityToken
	now := handler.controller.clock.Now().UTC()
	if err := effect.ValidateEnvelopeForIntent(envelope, current.Intent, now, true); err != nil {
		if observeErr := handler.reject(ctx, current, ledger, descriptor, credentials.AuthorizationDigest, "command.expired-or-invalid", err.Error()); observeErr != nil {
			return retryIntent(now, "outbox.invalid-reject", observeErr)
		}
		return worker.Result{Disposition: worker.Complete}
	}
	providerLedger, dispatchErr := adapter.Dispatch(ctx, current.Intent, envelope, credentials.AuthorizationDigest)
	if dispatchErr != nil {
		if observeErr := handler.observeUncertain(ctx, current, ledger, descriptor, credentials.AuthorizationDigest, dispatchErr); observeErr != nil {
			return retryIntent(now, "outbox.uncertain-observe", observeErr)
		}
		return worker.Result{Disposition: worker.Complete}
	}
	if err := effect.ValidateLedger(providerLedger); err != nil || len(providerLedger.Receipts) == 0 || !samePublicEnvelope(providerLedger.Envelope, ledger.Envelope) {
		if err == nil {
			err = errors.New("provider returned no receipts or a conflicting command envelope")
		}
		if observeErr := handler.observeUncertain(ctx, current, ledger, descriptor, credentials.AuthorizationDigest, err); observeErr != nil {
			return retryIntent(now, "outbox.invalid-ledger", observeErr)
		}
		return worker.Result{Disposition: worker.Complete}
	}
	for _, receipt := range providerLedger.Receipts {
		if _, err := handler.controller.ObserveEffect(ctx, ObserveEffectRequest{
			EffectID: current.EffectID, Receipt: receipt,
			CommandID: phaseCommand(current.EffectID, "provider-receipt", uint64(receipt.Sequence)),
		}); err != nil {
			return retryIntent(now, "outbox.receipt-persist", err)
		}
	}
	return worker.Result{Disposition: worker.Complete}
}

func (handler *EffectOutboxHandler) reject(
	ctx context.Context,
	current contracts.EffectRecord,
	ledger contracts.CommandLedger,
	descriptor EffectAdapterDescriptor,
	authorizationDigest, code, message string,
) error {
	at := handler.controller.clock.Now().UTC()
	if at.Before(current.UpdatedAt) {
		at = current.UpdatedAt
	}
	receipt, err := syntheticReceipt(ledger.Envelope, descriptor, authorizationDigest, 1, contracts.ReceiptRejected,
		&contracts.StructuredFailure{Class: contracts.FailurePermanent, Code: code, Message: message}, at)
	if err != nil {
		return err
	}
	_, err = handler.controller.ObserveEffect(ctx, ObserveEffectRequest{
		EffectID: current.EffectID, Receipt: receipt,
		CommandID: phaseCommand(current.EffectID, "rejected", current.Revision),
	})
	return err
}

func (handler *EffectOutboxHandler) observeUncertain(
	ctx context.Context,
	current contracts.EffectRecord,
	ledger contracts.CommandLedger,
	descriptor EffectAdapterDescriptor,
	authorizationDigest string,
	cause error,
) error {
	at := handler.controller.clock.Now().UTC()
	if at.Before(current.UpdatedAt) {
		at = current.UpdatedAt
	}
	accepted, err := syntheticReceipt(ledger.Envelope, descriptor, authorizationDigest, 1, contracts.ReceiptAccepted, nil, at)
	if err != nil {
		return err
	}
	if _, err := handler.controller.ObserveEffect(ctx, ObserveEffectRequest{
		EffectID: current.EffectID, Receipt: accepted,
		CommandID: phaseCommand(current.EffectID, "uncertain-accepted", current.Revision),
	}); err != nil {
		return err
	}
	uncertain, err := syntheticReceipt(ledger.Envelope, descriptor, authorizationDigest, 2, contracts.ReceiptUncertain,
		&contracts.StructuredFailure{Class: contracts.FailureUncertain, Code: "adapter.dispatch-uncertain", Message: cause.Error()}, at)
	if err != nil {
		return err
	}
	_, err = handler.controller.ObserveEffect(ctx, ObserveEffectRequest{
		EffectID: current.EffectID, Receipt: uncertain,
		CommandID: phaseCommand(current.EffectID, "uncertain-terminal", current.Revision+1),
	})
	return err
}

func syntheticReceipt(
	envelope contracts.CommandEnvelope,
	descriptor EffectAdapterDescriptor,
	authorizationDigest string,
	sequence uint32,
	status contracts.ReceiptStatus,
	failure *contracts.StructuredFailure,
	at time.Time,
) (contracts.CommandReceipt, error) {
	if !contracts.ValidDigest(authorizationDigest) {
		authorizationDigest = envelope.PolicyDigest
	}
	receiptID, err := effect.StableReceiptID(envelope.CommandID, sequence)
	if err != nil {
		return contracts.CommandReceipt{}, err
	}
	fenceDigest, err := effect.FenceDigest(envelope.Fence)
	if err != nil {
		return contracts.CommandReceipt{}, err
	}
	return contracts.CommandReceipt{
		ReceiptID: receiptID, CommandID: envelope.CommandID, Sequence: sequence, Status: status,
		IdentityDigest: envelope.IdentityDigest, FenceDigest: fenceDigest,
		ProviderRef: descriptor.ProviderRef, ProviderDigest: descriptor.ProviderDigest,
		PolicyDigest: envelope.PolicyDigest, AuthorizationDigest: authorizationDigest,
		Failure: failure, ObservedAt: at,
	}, nil
}

type WaitResolutionHandler struct{ controller *Controller }

func NewWaitResolutionHandler(controller *Controller) (*WaitResolutionHandler, error) {
	if controller == nil {
		return nil, errors.New("wait resolution controller is required")
	}
	return &WaitResolutionHandler{controller: controller}, nil
}

func (handler *WaitResolutionHandler) Handle(ctx context.Context, claimed store.ClaimedIntent) worker.Result {
	if claimed.Record.Intent.Kind != contracts.IntentWaitResolution {
		return deadIntent("wait.kind", "claimed intent is not a wait resolution")
	}
	if _, err := handler.controller.ResolveEffectWait(ctx, claimed.Record.Intent.AggregateID); err != nil &&
		!errors.Is(err, ErrRunWaiting) && !errors.Is(err, ErrRunClosureOpen) {
		return retryIntent(handler.controller.clock.Now(), "wait.resolve", err)
	}
	return worker.Result{Disposition: worker.Complete}
}

func retryIntent(now time.Time, code string, cause error) worker.Result {
	available := now.UTC().Add(time.Second)
	return worker.Result{Disposition: worker.Retry, AvailableAt: &available, Failure: &contracts.StructuredFailure{
		Class: contracts.FailureTransient, Code: code, Message: cause.Error(),
	}}
}

func deadIntent(code, message string) worker.Result {
	return worker.Result{Disposition: worker.Dead, Failure: &contracts.StructuredFailure{
		Class: contracts.FailurePermanent, Code: code, Message: message,
	}}
}

var _ worker.Handler = (*EffectOutboxHandler)(nil)
var _ worker.Handler = (*WaitResolutionHandler)(nil)

// InternalCleanupHandler acknowledges Run/Invocation cleanup scheduling
// signals that contain no provider Effect. It never consumes Effect cleanup;
// a coordinator without a conforming compensator must leave that work for a
// capable host.
type InternalCleanupHandler struct{}

func (InternalCleanupHandler) Handle(_ context.Context, claimed store.ClaimedIntent) worker.Result {
	if claimed.Record.Intent.Kind != contracts.IntentCleanup {
		return deadIntent("cleanup.kind", "claimed intent is not cleanup work")
	}
	if effectID, _ := claimed.Record.Intent.Payload["effectId"].(string); effectID != "" {
		return worker.Result{Disposition: worker.Leave}
	}
	return worker.Result{Disposition: worker.Complete}
}

var _ worker.Handler = InternalCleanupHandler{}

type EffectCleanupHandler struct {
	controller *Controller
	planner    EffectCompensationPlanner
	broker     EffectCredentialBroker
	adapters   map[string]effectport.Compensator
}

func NewEffectCleanupHandler(
	controller *Controller,
	planner EffectCompensationPlanner,
	broker EffectCredentialBroker,
	adapters ...EffectAdapter,
) (*EffectCleanupHandler, error) {
	if controller == nil || planner == nil || broker == nil {
		return nil, errors.New("effect cleanup controller, planner, and credential broker are required")
	}
	registered := make(map[string]effectport.Compensator)
	for _, adapter := range adapters {
		if compensator, ok := adapter.(effectport.Compensator); ok {
			registered[adapter.Descriptor().Kind] = compensator
		}
	}
	return &EffectCleanupHandler{controller: controller, planner: planner, broker: broker, adapters: registered}, nil
}

func (handler *EffectCleanupHandler) Handle(ctx context.Context, claimed store.ClaimedIntent) worker.Result {
	if claimed.Record.Intent.Kind != contracts.IntentCleanup {
		return deadIntent("cleanup.kind", "claimed intent is not cleanup work")
	}
	// Run and Invocation cleanup intents are scheduling signals consumed by the
	// controller drive. Only Effect cleanup carries an effectId payload.
	effectID, _ := claimed.Record.Intent.Payload["effectId"].(string)
	if effectID == "" {
		return worker.Result{Disposition: worker.Complete}
	}
	current, err := handler.controller.GetEffect(ctx, effectID)
	if err != nil {
		return retryIntent(handler.controller.clock.Now(), "cleanup.effect-load", err)
	}
	if current.CompensationState.Terminal() {
		return worker.Result{Disposition: worker.Complete}
	}
	if current.CompensationState == contracts.EffectCompensationPending {
		request, planErr := handler.planner.PlanEffectCompensation(ctx, current)
		if planErr != nil {
			return retryIntent(handler.controller.clock.Now(), "cleanup.plan", planErr)
		}
		if request.EffectID == "" {
			request.EffectID = current.EffectID
		}
		if request.EffectID != current.EffectID {
			return deadIntent("cleanup.plan-conflict", "compensation planner changed the Effect identity")
		}
		begun, beginErr := handler.controller.BeginEffectCompensation(ctx, request)
		if beginErr != nil {
			return retryIntent(handler.controller.clock.Now(), "cleanup.begin", beginErr)
		}
		current = begun.Effect
	}
	if current.CompensationState != contracts.EffectCompensationRunning {
		return deadIntent("cleanup.effect-state", "cleanup does not target a pending or running compensation")
	}
	ledger, err := handler.controller.GetCommandLedger(ctx, current.CompensationCommandID)
	if err != nil {
		return retryIntent(handler.controller.clock.Now(), "cleanup.ledger-load", err)
	}
	compensator := handler.adapters[current.Intent.Kind]
	if compensator == nil {
		return handler.fail(ctx, current, ledger, EffectAdapterDescriptor{
			Kind: current.Intent.Kind, ProviderRef: handler.controller.ownerRef, ProviderDigest: current.Intent.DescriptorDigest,
		}, current.Intent.PolicyDigest, "compensator.not-installed", "no compensator is installed for the applied Effect kind")
	}
	credentials, err := handler.broker.ResolveEffectCredentials(ctx, current, ledger)
	if err != nil {
		return handler.fail(ctx, current, ledger, compensator.Descriptor(), current.Intent.PolicyDigest, "compensation.credential", err.Error())
	}
	envelope := ledger.Envelope
	envelope.IdempotencyKey = credentials.IdempotencyKey
	envelope.CapabilityToken = credentials.CapabilityToken
	now := handler.controller.clock.Now().UTC()
	if err := effect.ValidatePrivateEnvelopeTokens(envelope); err != nil || !envelope.Deadline.After(now) {
		if err == nil {
			err = errors.New("compensation command deadline has elapsed")
		}
		return handler.fail(ctx, current, ledger, compensator.Descriptor(), credentials.AuthorizationDigest, "compensation.command-invalid", err.Error())
	}
	providerLedger, dispatchErr := compensator.Compensate(ctx, current, envelope, credentials.AuthorizationDigest)
	if dispatchErr != nil {
		return handler.fail(ctx, current, ledger, compensator.Descriptor(), credentials.AuthorizationDigest, "compensation.dispatch-uncertain", dispatchErr.Error())
	}
	if err := effect.ValidateLedger(providerLedger); err != nil || len(providerLedger.Receipts) == 0 ||
		!providerLedger.Receipts[len(providerLedger.Receipts)-1].Status.Terminal() || !samePublicEnvelope(providerLedger.Envelope, ledger.Envelope) {
		if err == nil {
			err = errors.New("compensator returned no terminal ledger or a conflicting command envelope")
		}
		return handler.fail(ctx, current, ledger, compensator.Descriptor(), credentials.AuthorizationDigest, "compensation.ledger-invalid", err.Error())
	}
	for _, receipt := range providerLedger.Receipts {
		observed, observeErr := handler.controller.ObserveEffectCompensation(ctx, ObserveEffectCompensationRequest{
			EffectID: current.EffectID, Receipt: receipt,
			CommandID: phaseCommand(current.EffectID, "compensation-receipt", uint64(receipt.Sequence)),
		})
		if observeErr != nil {
			return retryIntent(now, "cleanup.receipt-persist", observeErr)
		}
		current = observed.Effect
	}
	return worker.Result{Disposition: worker.Complete}
}

func (handler *EffectCleanupHandler) fail(
	ctx context.Context,
	current contracts.EffectRecord,
	ledger contracts.CommandLedger,
	descriptor EffectAdapterDescriptor,
	authorizationDigest, code, message string,
) worker.Result {
	at := handler.controller.clock.Now().UTC()
	if at.Before(current.UpdatedAt) {
		at = current.UpdatedAt
	}
	sequence := uint32(len(ledger.Receipts) + 1)
	status := contracts.ReceiptUncertain
	class := contracts.FailureUncertain
	if code == "compensator.not-installed" || code == "compensation.command-invalid" || code == "compensation.credential" {
		status = contracts.ReceiptRejected
		class = contracts.FailurePermanent
	}
	receipt, err := syntheticReceipt(ledger.Envelope, descriptor, authorizationDigest, sequence, status,
		&contracts.StructuredFailure{Class: class, Code: code, Message: message}, at)
	if err != nil {
		return retryIntent(at, "cleanup.failure-receipt", err)
	}
	if _, err := handler.controller.ObserveEffectCompensation(ctx, ObserveEffectCompensationRequest{
		EffectID: current.EffectID, Receipt: receipt,
		CommandID: phaseCommand(current.EffectID, "compensation-failed", uint64(sequence)),
	}); err != nil {
		return retryIntent(at, "cleanup.failure-persist", err)
	}
	return worker.Result{Disposition: worker.Complete}
}

var _ worker.Handler = (*EffectCleanupHandler)(nil)

type ChildResolutionHandler struct{ controller *Controller }

func NewChildResolutionHandler(controller *Controller) (*ChildResolutionHandler, error) {
	if controller == nil {
		return nil, errors.New("child resolution controller is required")
	}
	return &ChildResolutionHandler{controller: controller}, nil
}

func (handler *ChildResolutionHandler) Handle(ctx context.Context, claimed store.ClaimedIntent) worker.Result {
	if claimed.Record.Intent.Kind != contracts.IntentChildResolution {
		return deadIntent("child.kind", "claimed intent is not a child Action resolution")
	}
	if _, err := handler.controller.ResolveActionCall(ctx, claimed.Record.Intent.AggregateID); err != nil {
		if errors.Is(err, ErrRunWaiting) || errors.Is(err, ErrAttemptLeaseActive) || errors.Is(err, ErrRunClosureOpen) {
			return retryIntent(handler.controller.clock.Now(), "child.waiting", err)
		}
		return retryIntent(handler.controller.clock.Now(), "child.resolve", err)
	}
	return worker.Result{Disposition: worker.Complete}
}

var _ worker.Handler = (*ChildResolutionHandler)(nil)
