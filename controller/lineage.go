package controller

import (
	"context"
	"errors"
	"fmt"

	"github.com/lxk36/xgc2-orchestration-core/durable/store"
	"github.com/lxk36/xgc2-orchestration-core/kernel/execution"
	"github.com/lxk36/xgc2-orchestration-core/sdk/go/contracts"
)

func (controller *Controller) validateInvokeLineage(ctx context.Context, request InvokeRequest) error {
	if request.Parent == nil {
		if request.RootRunID != "" {
			return errors.New("root Action invocation cannot supply another root Run")
		}
		if request.Trigger.Kind == contracts.TriggerActionCall {
			return errors.New("Action-call trigger requires an exact parent Run link")
		}
		return nil
	}
	parentLink := *request.Parent
	if request.Trigger.Kind != contracts.TriggerActionCall || request.CandidateOrigin != contracts.OriginParentMap {
		return errors.New("child Action invocation requires an Action-call trigger and parent-map inputs")
	}
	if !contracts.ValidIdentifier(request.RootRunID) || !contracts.ValidDigest(request.MappingDigest) ||
		parentLink.MappingDigest != request.MappingDigest {
		return errors.New("child Action invocation requires its exact root and mapping digest")
	}
	parentRecord, err := controller.store.GetAggregate(ctx, runKey(parentLink.ParentRunID))
	if err != nil {
		return fmt.Errorf("load parent Run: %w", err)
	}
	parent, err := decodeRun(parentRecord)
	if err != nil {
		return err
	}
	if parent.NamespaceID != request.NamespaceID || parent.RootRunID != request.RootRunID || parent.Status.Terminal() {
		return errors.New("child Action parent namespace, root, or lifecycle is invalid")
	}
	expectedInvocationID, err := execution.StableInvocationID(parent.RunID, parentLink.CallNodeID)
	if err != nil || expectedInvocationID != parentLink.ParentInvocationID {
		return errors.New("child Action parent invocation identity is invalid")
	}
	invocationRecord, err := controller.store.GetAggregate(ctx, store.AggregateKey{Type: invocationType, ID: parentLink.ParentInvocationID})
	if err != nil {
		return fmt.Errorf("load parent invocation: %w", err)
	}
	ledger, err := decodeLedger(invocationRecord)
	if err != nil {
		return err
	}
	if ledger.Invocation.RunID != parent.RunID || ledger.Invocation.NodeID != parentLink.CallNodeID ||
		(ledger.Invocation.Status != contracts.InvocationRunning && ledger.Invocation.Status != contracts.InvocationWaiting) {
		return errors.New("child Action parent invocation is not an active matching call node")
	}
	return nil
}
