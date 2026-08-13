// Package controller composes the pure kernels, durable Store, and sealed Node
// registry into a recoverable single-controller workflow runtime. Product
// authoring models and product databases remain outside this package.
package controller

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"time"

	"github.com/lxk36/xgc2-orchestration-core/durable/store"
	"github.com/lxk36/xgc2-orchestration-core/kernel/action"
	"github.com/lxk36/xgc2-orchestration-core/kernel/canonicaljson"
	"github.com/lxk36/xgc2-orchestration-core/kernel/effect"
	"github.com/lxk36/xgc2-orchestration-core/kernel/execution"
	"github.com/lxk36/xgc2-orchestration-core/kernel/node"
	workflowkernel "github.com/lxk36/xgc2-orchestration-core/kernel/workflow"
	"github.com/lxk36/xgc2-orchestration-core/sdk/go/contracts"
)

var (
	ErrAttemptLeaseActive = errors.New("node invocation is owned by a live attempt lease")
	ErrRunWaiting         = errors.New("workflow run is durably waiting")
	ErrRunClosureOpen     = errors.New("workflow run ownership closure is still open")
	errContinueDrive      = errors.New("continue driving workflow")
)

const (
	runAggregateType      = "run"
	snapshotAggregateType = "run-snapshot"
	invocationType        = "invocation"
	effectAggregateType   = "effect"
	commandLedgerType     = "command-ledger"
	ownershipGraphType    = "ownership-graph"
	eventSchemaDigest     = "sha256:90b2f52b665b9a8e896a5708d6bf7b2083b47e45992498a57268edbbc2e8f49a"
)

type Clock interface{ Now() time.Time }

type systemClock struct{}

func (systemClock) Now() time.Time { return time.Now().UTC() }

type GrantResolver interface {
	ResolveGrants(context.Context, contracts.Run, contracts.NodeDescriptor, time.Time) ([]contracts.CapabilityGrant, error)
}

type TokenSource interface{ NewToken() (string, error) }

type cryptoTokenSource struct{}

func (cryptoTokenSource) NewToken() (string, error) {
	raw := make([]byte, 32)
	if _, err := rand.Read(raw); err != nil {
		return "", err
	}
	return "lease-" + hex.EncodeToString(raw), nil
}

type Config struct {
	Store             store.Store
	Nodes             *node.Registry
	OwnerRef          string
	LeaseDuration     time.Duration
	InvocationTimeout time.Duration
	Clock             Clock
	Grants            GrantResolver
	Tokens            TokenSource
}

type Controller struct {
	store             store.Store
	nodes             *node.Registry
	descriptors       map[string]contracts.NodeDescriptor
	ownerRef          string
	leaseDuration     time.Duration
	invocationTimeout time.Duration
	clock             Clock
	grants            GrantResolver
	tokens            TokenSource
	afterClaim        func(contracts.InvocationLedger) error
}

type InvokeRequest struct {
	RunID           string
	NamespaceID     string
	Action          contracts.ActionVersion
	Definition      contracts.WorkflowDefinition
	Trigger         contracts.TriggerEvent
	Preset          *contracts.ActionPresetVersion
	Candidate       map[string]any
	CandidateOrigin contracts.InputOriginKind
	CandidateRef    string
	MappingDigest   string
	Scope           map[string]any
	ScopeRef        string
	CorrelationRef  string
	CommandID       string
}

type InvokeResult struct {
	Run    contracts.Run
	Replay bool
}

type RunSnapshot struct {
	SchemaVersion string                           `json:"schemaVersion"`
	RunID         string                           `json:"runId"`
	Action        contracts.ActionVersion          `json:"action"`
	Definition    contracts.WorkflowDefinition     `json:"definition"`
	Plan          contracts.CompiledWorkflowPlan   `json:"plan"`
	Entrypoint    string                           `json:"entrypoint"`
	NodeOrder     []string                         `json:"nodeOrder"`
	Inputs        map[string]any                   `json:"inputs"`
	Trigger       map[string]any                   `json:"trigger"`
	Scope         map[string]any                   `json:"scope"`
	Provenance    []contracts.InputFieldProvenance `json:"provenance"`
	NodeOutputs   map[string]map[string]any        `json:"nodeOutputs"`
	NextNode      int                              `json:"nextNode"`
	Waiting       *contracts.NodeResult            `json:"waiting,omitempty"`
	Failure       *contracts.StructuredFailure     `json:"failure,omitempty"`
	Result        map[string]any                   `json:"result,omitempty"`
	ResultDigest  string                           `json:"resultDigest,omitempty"`
}

// Compile validates the complete product-neutral graph and every installed
// node descriptor/schema pin without creating a Run. Authoring tools and
// Agents use the returned canonical digests instead of reimplementing the
// compiler or guessing wire canonicalization.
func (controller *Controller) Compile(definition contracts.WorkflowDefinition) (contracts.CompiledWorkflowPlan, error) {
	if controller == nil {
		return contracts.CompiledWorkflowPlan{}, errors.New("controller is unavailable")
	}
	plan, err := workflowkernel.Compile(definition)
	if err != nil {
		return contracts.CompiledWorkflowPlan{}, err
	}
	if err := controller.validateNodePins(definition); err != nil {
		return contracts.CompiledWorkflowPlan{}, err
	}
	return plan, nil
}

func New(config Config) (*Controller, error) {
	if config.Store == nil || config.Nodes == nil || !contracts.ValidIdentifier(config.OwnerRef) {
		return nil, errors.New("controller store, sealed node registry, and owner ref are required")
	}
	if config.Clock == nil {
		config.Clock = systemClock{}
	}
	if config.Tokens == nil {
		config.Tokens = cryptoTokenSource{}
	}
	if config.LeaseDuration <= 0 {
		config.LeaseDuration = 30 * time.Second
	}
	if config.InvocationTimeout <= 0 {
		config.InvocationTimeout = 10 * time.Second
	}
	if config.LeaseDuration <= config.InvocationTimeout {
		return nil, errors.New("controller lease duration must exceed invocation timeout")
	}
	catalog, _, err := config.Nodes.Catalog()
	if err != nil {
		return nil, err
	}
	descriptors := make(map[string]contracts.NodeDescriptor, len(catalog))
	for _, descriptor := range catalog {
		descriptors[descriptor.TypeRef] = descriptor
	}
	return &Controller{
		store: config.Store, nodes: config.Nodes, descriptors: descriptors, ownerRef: config.OwnerRef,
		leaseDuration: config.LeaseDuration, invocationTimeout: config.InvocationTimeout,
		clock: config.Clock, grants: config.Grants, tokens: config.Tokens,
	}, nil
}

func (controller *Controller) Invoke(ctx context.Context, request InvokeRequest) (InvokeResult, error) {
	if ctx == nil {
		return InvokeResult{}, errors.New("invoke context is required")
	}
	if !contracts.ValidIdentifier(request.RunID) || !contracts.ValidIdentifier(request.NamespaceID) ||
		!contracts.ValidIdentifier(request.CommandID) {
		return InvokeResult{}, errors.New("invoke run, namespace, or command identity is invalid")
	}
	plan, err := workflowkernel.Compile(request.Definition)
	if err != nil {
		return InvokeResult{}, err
	}
	if err := controller.validatePinnedAction(request.Action, request.Definition, plan); err != nil {
		return InvokeResult{}, err
	}
	if err := controller.validateNodePins(request.Definition); err != nil {
		return InvokeResult{}, err
	}
	if request.Scope == nil {
		request.Scope = map[string]any{}
	}
	if err := request.Definition.ScopeSchema.ValidateValue(request.Scope); err != nil {
		return InvokeResult{}, fmt.Errorf("invoke scope: %w", err)
	}
	if err := request.Definition.TriggerSchema.ValidateValue(request.Trigger.Payload); err != nil {
		return InvokeResult{}, fmt.Errorf("invoke trigger payload: %w", err)
	}
	admission, err := action.Admit(action.Request{
		Action: request.Action, Trigger: request.Trigger, Preset: request.Preset, Candidate: request.Candidate,
		CandidateOrigin: request.CandidateOrigin, CandidateRef: request.CandidateRef, MappingDigest: request.MappingDigest,
	})
	if err != nil {
		return InvokeResult{}, err
	}
	at := controller.clock.Now().UTC()
	decision, err := execution.AdmitRun(execution.AdmitRunCommand{
		RunID: request.RunID, NamespaceID: request.NamespaceID, ActionRef: request.Action.Ref(),
		ExecutionPlanRef: digestRef("plan", plan.PlanDigest), PlanDigest: plan.PlanDigest,
		TriggerRef: request.Trigger.EventID, TriggerDigest: admission.TriggerDigest, InputDigest: admission.InputDigest,
		ProvenanceArtifactRef: digestRef("provenance", admission.InputDigest), ScopeRef: request.ScopeRef,
		ActorRef: request.Trigger.ActorRef, SourceRef: request.Trigger.SourceRef, CorrelationRef: request.CorrelationRef,
		CommandID: request.CommandID, At: at,
	})
	if err != nil {
		return InvokeResult{}, err
	}
	nodeOrder := append([]string(nil), plan.EntrypointNodeOrder[request.Action.Entrypoint]...)
	snapshot := RunSnapshot{
		SchemaVersion: "xgc.run-snapshot/v1", RunID: request.RunID, Action: request.Action,
		Definition: request.Definition, Plan: plan, Entrypoint: request.Action.Entrypoint, NodeOrder: nodeOrder,
		Inputs: admission.Inputs, Trigger: admission.Trigger.Payload, Scope: request.Scope,
		Provenance: admission.FieldProvenance, NodeOutputs: map[string]map[string]any{},
	}
	runMutation, err := aggregateMutation(runKey(request.RunID), 0, decision.Run)
	if err != nil {
		return InvokeResult{}, err
	}
	snapshotMutation, err := aggregateMutation(snapshotKey(request.RunID), 0, snapshot)
	if err != nil {
		return InvokeResult{}, err
	}
	snapshotEvent, err := aggregateEvent(snapshotMutation, "snapshot.created", request.CommandID, at, map[string]any{
		"planDigest": plan.PlanDigest, "entrypoint": request.Action.Entrypoint, "inputDigest": admission.InputDigest,
	})
	if err != nil {
		return InvokeResult{}, err
	}
	identityDigest, err := canonicaljson.DigestValue(map[string]any{
		"runId": request.RunID, "actionRef": request.Action.Ref(), "planDigest": plan.PlanDigest,
		"triggerDigest": admission.TriggerDigest, "inputDigest": admission.InputDigest,
	})
	if err != nil {
		return InvokeResult{}, err
	}
	outcome, err := canonicaljson.Marshal(decision.Run)
	if err != nil {
		return InvokeResult{}, err
	}
	committed, err := controller.store.Commit(ctx, store.Transaction{
		CommandID: request.CommandID, IdentityDigest: identityDigest,
		Expected:  []store.ExpectedRevision{{Key: runMutation.Key, Revision: 0}, {Key: snapshotMutation.Key, Revision: 0}},
		Mutations: []store.AggregateRecord{runMutation, snapshotMutation},
		Events:    append(decision.Events, snapshotEvent), Outcome: outcome, At: at,
	})
	if err != nil {
		return InvokeResult{}, err
	}
	var run contracts.Run
	if err := canonicaljson.UnmarshalStrict(committed.Outcome, &run); err != nil {
		return InvokeResult{}, err
	}
	return InvokeResult{Run: run, Replay: committed.Replay}, nil
}

func (controller *Controller) GetRun(ctx context.Context, runID string) (contracts.Run, error) {
	record, err := controller.store.GetAggregate(ctx, runKey(runID))
	if err != nil {
		return contracts.Run{}, err
	}
	return decodeRun(record)
}

func (controller *Controller) ListRuns(ctx context.Context, afterRunID string, limit int) ([]contracts.Run, error) {
	records, err := controller.store.ListAggregates(ctx, runAggregateType, afterRunID, limit)
	if err != nil {
		return nil, err
	}
	runs := make([]contracts.Run, 0, len(records))
	for _, record := range records {
		run, decodeErr := decodeRun(record)
		if decodeErr != nil {
			return nil, decodeErr
		}
		runs = append(runs, run)
	}
	return runs, nil
}

func (controller *Controller) GetSnapshot(ctx context.Context, runID string) (RunSnapshot, uint64, error) {
	record, err := controller.store.GetAggregate(ctx, snapshotKey(runID))
	if err != nil {
		return RunSnapshot{}, 0, err
	}
	var snapshot RunSnapshot
	if err := canonicaljson.UnmarshalStrict(record.Payload, &snapshot); err != nil {
		return RunSnapshot{}, 0, err
	}
	return snapshot, record.Revision, nil
}

func (controller *Controller) validatePinnedAction(actionVersion contracts.ActionVersion, definition contracts.WorkflowDefinition, plan contracts.CompiledWorkflowPlan) error {
	if actionVersion.DefinitionDigest != plan.DefinitionDigest || definition.WorkflowID != actionVersion.ActionID ||
		definition.Version != actionVersion.Version {
		return errors.New("action does not pin the compiled workflow identity and digest")
	}
	if _, exists := definition.Entrypoints[actionVersion.Entrypoint]; !exists {
		return errors.New("action entrypoint is absent from the workflow")
	}
	left, err := canonicaljson.DigestValue(actionVersion.InputSchema)
	if err != nil {
		return err
	}
	right, err := canonicaljson.DigestValue(definition.InputSchema)
	if err != nil || left != right {
		return errors.New("action and workflow input schemas differ")
	}
	left, err = canonicaljson.DigestValue(actionVersion.ResultSchema)
	if err != nil {
		return err
	}
	right, err = canonicaljson.DigestValue(definition.ResultSchema)
	if err != nil || left != right {
		return errors.New("action and workflow result schemas differ")
	}
	return nil
}

func (controller *Controller) validateNodePins(definition contracts.WorkflowDefinition) error {
	for _, workflowNode := range definition.Nodes {
		descriptor, exists := controller.descriptors[workflowNode.TypeRef]
		if !exists || descriptor.DescriptorDigest != workflowNode.DescriptorDigest {
			return fmt.Errorf("workflow node %q does not pin an installed descriptor", workflowNode.NodeID)
		}
		inputDigest, err := canonicaljson.DigestValue(workflowNode.InputSchema)
		if err != nil {
			return err
		}
		descriptorInputDigest, err := canonicaljson.DigestValue(descriptor.InputSchema)
		if err != nil || inputDigest != descriptorInputDigest {
			return fmt.Errorf("workflow node %q input schema differs from its descriptor", workflowNode.NodeID)
		}
		outputDigest, err := canonicaljson.DigestValue(workflowNode.OutputSchema)
		if err != nil {
			return err
		}
		descriptorOutputDigest, err := canonicaljson.DigestValue(descriptor.OutputSchema)
		if err != nil || outputDigest != descriptorOutputDigest {
			return fmt.Errorf("workflow node %q output schema differs from its descriptor", workflowNode.NodeID)
		}
	}
	return nil
}

func prepareEffects(result contracts.NodeResult, run contracts.Run, ledger contracts.InvocationLedger, at time.Time, commandID string) ([]store.AggregateRecord, []contracts.DomainEvent, error) {
	if len(result.Effects) == 0 {
		return nil, nil, nil
	}
	attempt := ledger.Attempts[len(ledger.Attempts)-1]
	mutations := make([]store.AggregateRecord, 0, len(result.Effects))
	events := make([]contracts.DomainEvent, 0, len(result.Effects))
	for _, proposal := range result.Effects {
		effectID, err := effect.StableEffectID(ledger.Invocation.InvocationID, proposal.EffectKey)
		if err != nil {
			return nil, nil, err
		}
		decision, err := effect.Prepare(effect.PrepareCommand{Intent: contracts.EffectIntent{
			EffectID: effectID, NamespaceID: run.NamespaceID, RunID: run.RunID,
			InvocationID: ledger.Invocation.InvocationID, PreparedAttemptID: attempt.AttemptID,
			EffectKey: proposal.EffectKey, Kind: proposal.Kind, TargetRef: proposal.TargetRef,
			IntentSchemaDigest: proposal.IntentSchemaDigest, Intent: proposal.Intent, IntentDigest: proposal.IntentDigest,
			IntentArtifactRef: proposal.IntentArtifactRef, Ownership: proposal.Ownership,
			CompensationPolicy: proposal.CompensationPolicy, RequiredCapabilityRefs: proposal.RequiredCapabilityRefs,
			PolicyDigest: proposal.PolicyDigest, DescriptorDigest: ledger.Invocation.DescriptorDigest,
			Deadline: proposal.Deadline,
		}, CommandID: commandID, At: at})
		if err != nil {
			return nil, nil, err
		}
		mutation, err := aggregateMutation(store.AggregateKey{Type: effectAggregateType, ID: effectID}, 0, decision.Effect)
		if err != nil {
			return nil, nil, err
		}
		mutations = append(mutations, mutation)
		events = append(events, decision.Events...)
	}
	return mutations, events, nil
}
