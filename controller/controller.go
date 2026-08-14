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
	runAggregateType         = "run"
	snapshotAggregateType    = "run-snapshot"
	invocationType           = "invocation"
	effectAggregateType      = "effect"
	commandLedgerType        = "command-ledger"
	ownershipGraphType       = "ownership-graph"
	activeOwnerAggregateType = "active-run-owner"
	eventSchemaDigest        = "sha256:90b2f52b665b9a8e896a5708d6bf7b2083b47e45992498a57268edbbc2e8f49a"
)

type Clock interface{ Now() time.Time }

type systemClock struct{}

func (systemClock) Now() time.Time { return time.Now().UTC() }

type GrantResolver interface {
	ResolveGrants(context.Context, contracts.Run, contracts.NodeDescriptor, time.Time) ([]contracts.CapabilityGrant, error)
}

// ActionResolver resolves an exact child Action pin without exposing mutable
// authoring heads to the controller. Implementations may read a product
// catalog, a signed package index, or an immutable local registry.
type ActionResolver interface {
	ResolveAction(context.Context, string, contracts.ActionRef) (contracts.ActionVersion, contracts.WorkflowDefinition, error)
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
	Store                   store.Store
	Nodes                   *node.Registry
	OwnerRef                string
	LeaseDuration           time.Duration
	InvocationTimeout       time.Duration
	Clock                   Clock
	Grants                  GrantResolver
	Actions                 ActionResolver
	Tokens                  TokenSource
	ReservedIngressPolicies []*ReservedIngressPolicy
}

type Controller struct {
	store              store.Store
	nodes              *node.Registry
	descriptors        map[string]contracts.NodeDescriptor
	ownerRef           string
	leaseDuration      time.Duration
	invocationTimeout  time.Duration
	clock              Clock
	grants             GrantResolver
	actions            ActionResolver
	tokens             TokenSource
	afterClaim         func(contracts.InvocationLedger) error
	reservedIngress    []registeredIngressPolicy
	reservedNamespaces map[string]struct{}
}

type registeredIngressPolicy struct {
	policy *ReservedIngressPolicy
	spec   ReservedIngressPolicySpec
	digest string
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
	Parent          *contracts.ParentRunLink
	RootRunID       string
	IngressPermit   *IngressPermit
	ActiveOwnerKey  *contracts.ActiveOwnerKey
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
	ActionCall    *ActionCallWait                  `json:"actionCall,omitempty"`
	Failure       *contracts.StructuredFailure     `json:"failure,omitempty"`
	Result        map[string]any                   `json:"result,omitempty"`
	ResultDigest  string                           `json:"resultDigest,omitempty"`
}

// ActionCallWait is the exact durable join between one parent invocation and
// one child Run. It contains no mutable catalog pointer or ambient context.
type ActionCallWait struct {
	InvocationID  string              `json:"invocationId"`
	NodeID        string              `json:"nodeId"`
	ChildRunID    string              `json:"childRunId"`
	ActionRef     contracts.ActionRef `json:"actionRef"`
	MappingDigest string              `json:"mappingDigest"`
	InputDigest   string              `json:"inputDigest"`
	TriggerDigest string              `json:"triggerDigest"`
	ScopeDigest   string              `json:"scopeDigest"`
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
	reservedIngress := make([]registeredIngressPolicy, 0, len(config.ReservedIngressPolicies))
	reservedNamespaces := make(map[string]struct{}, len(config.ReservedIngressPolicies))
	policyRefs := make(map[string]struct{}, len(config.ReservedIngressPolicies))
	seenPolicies := make(map[*ReservedIngressPolicy]struct{}, len(config.ReservedIngressPolicies))
	for _, policy := range config.ReservedIngressPolicies {
		spec, valid := policy.Spec()
		if !valid {
			return nil, errors.New("reserved ingress policy is not an issued capability")
		}
		if _, duplicate := seenPolicies[policy]; duplicate {
			return nil, errors.New("reserved ingress policy capability is duplicated")
		}
		if _, duplicate := policyRefs[spec.PolicyRef]; duplicate {
			return nil, errors.New("reserved ingress policy ref is duplicated")
		}
		digest := policy.Digest()
		if !contracts.ValidDigest(digest) {
			return nil, errors.New("reserved ingress policy digest is invalid")
		}
		reservedIngress = append(reservedIngress, registeredIngressPolicy{policy: policy, spec: spec, digest: digest})
		reservedNamespaces[spec.NamespaceID] = struct{}{}
		policyRefs[spec.PolicyRef] = struct{}{}
		seenPolicies[policy] = struct{}{}
	}
	return &Controller{
		store: config.Store, nodes: config.Nodes, descriptors: descriptors, ownerRef: config.OwnerRef,
		leaseDuration: config.LeaseDuration, invocationTimeout: config.InvocationTimeout,
		clock: config.Clock, grants: config.Grants, actions: config.Actions, tokens: config.Tokens,
		reservedIngress: reservedIngress, reservedNamespaces: reservedNamespaces,
	}, nil
}

func (controller *Controller) Invoke(ctx context.Context, request InvokeRequest) (InvokeResult, error) {
	if request.Parent != nil || request.RootRunID != "" || request.Trigger.Kind == contracts.TriggerActionCall {
		return InvokeResult{}, fmt.Errorf("%w: child Action ingress is kernel-private", ErrReservedIngressDenied)
	}
	return controller.invoke(ctx, request, nil)
}

type childIngressProof struct{ requestDigest string }

func (controller *Controller) invokeChild(ctx context.Context, request InvokeRequest) (InvokeResult, error) {
	digest, err := childIngressRequestDigest(request)
	if err != nil {
		return InvokeResult{}, err
	}
	return controller.invoke(ctx, request, &childIngressProof{requestDigest: digest})
}

func childIngressRequestDigest(request InvokeRequest) (string, error) {
	return canonicaljson.DigestValue(map[string]any{
		"schemaVersion": "xgc.child-ingress-proof/v1", "runId": request.RunID,
		"namespaceId": request.NamespaceID, "actionRef": request.Action.Ref(), "parent": request.Parent,
		"rootRunId": request.RootRunID, "mappingDigest": request.MappingDigest,
		"trigger": request.Trigger, "candidate": request.Candidate, "candidateOrigin": request.CandidateOrigin,
		"scope": request.Scope, "scopeRef": request.ScopeRef, "correlationRef": request.CorrelationRef,
		"commandId": request.CommandID,
	})
}

func (controller *Controller) invoke(ctx context.Context, request InvokeRequest, childProof *childIngressProof) (InvokeResult, error) {
	if controller == nil || ctx == nil {
		return InvokeResult{}, errors.New("controller and invoke context are required")
	}
	if !contracts.ValidIdentifier(request.RunID) || !contracts.ValidIdentifier(request.NamespaceID) ||
		!contracts.ValidIdentifier(request.CommandID) {
		return InvokeResult{}, errors.New("invoke run, namespace, or command identity is invalid")
	}
	childShape := request.Parent != nil || request.RootRunID != "" || request.Trigger.Kind == contracts.TriggerActionCall
	if childShape {
		if childProof == nil {
			return InvokeResult{}, fmt.Errorf("%w: child Action ingress lacks a kernel proof", ErrReservedIngressDenied)
		}
		digest, digestErr := childIngressRequestDigest(request)
		if digestErr != nil || digest != childProof.requestDigest {
			return InvokeResult{}, fmt.Errorf("%w: child Action ingress proof differs from request", ErrReservedIngressDenied)
		}
	} else if childProof != nil {
		return InvokeResult{}, fmt.Errorf("%w: root ingress cannot carry a child proof", ErrReservedIngressDenied)
	}
	if err := controller.validateInvokeLineage(ctx, request); err != nil {
		return InvokeResult{}, err
	}
	ingress, err := controller.admitIngress(request)
	if err != nil {
		return InvokeResult{}, err
	}
	plan, err := workflowkernel.Compile(request.Definition)
	if err != nil {
		return InvokeResult{}, err
	}
	if err := controller.validatePinnedAction(request.Action, request.Definition, plan); err != nil {
		return InvokeResult{}, err
	}
	triggerSchemaDigest, err := canonicaljson.DigestValue(request.Definition.TriggerSchema)
	if err != nil {
		return InvokeResult{}, fmt.Errorf("digest invoke trigger schema: %w", err)
	}
	if request.Trigger.PayloadSchemaDigest != triggerSchemaDigest {
		return InvokeResult{}, errors.New("trigger payload schema digest differs from the pinned Workflow trigger schema")
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
	scopeDigest, err := canonicaljson.DigestValue(request.Scope)
	if err != nil {
		return InvokeResult{}, err
	}
	provenanceDigest, err := canonicaljson.DigestValue(admission.FieldProvenance)
	if err != nil {
		return InvokeResult{}, err
	}
	identityDigest, err := canonicaljson.DigestValue(map[string]any{
		"schemaVersion": "xgc.run-admission-command/v1", "commandId": request.CommandID,
		"runId": request.RunID, "namespaceId": request.NamespaceID, "action": request.Action,
		"planDigest": plan.PlanDigest, "triggerDigest": admission.TriggerDigest,
		"inputDigest": admission.InputDigest, "provenanceDigest": provenanceDigest,
		"scope": request.Scope, "scopeDigest": scopeDigest, "scopeRef": request.ScopeRef,
		"candidateRef": request.CandidateRef, "mappingDigest": request.MappingDigest,
		"parent": request.Parent, "rootRunId": request.RootRunID, "correlationRef": request.CorrelationRef,
		"admissionPolicyRef": ingress.policyRef, "admissionPolicyDigest": ingress.policyDigest,
		"activeOwnerKeyDigest": ingress.keyDigest,
	})
	if err != nil {
		return InvokeResult{}, err
	}
	if ingress.ownerKey != nil {
		if receipt, found, receiptErr := controller.getRunAdmissionReceipt(ctx, request.CommandID); receiptErr != nil {
			return InvokeResult{}, receiptErr
		} else if found {
			if receipt.RequestIdentityDigest != identityDigest {
				return InvokeResult{}, store.ErrIdentityConflict
			}
			return InvokeResult{Run: receipt.AcceptedRun, Replay: true}, nil
		}
	}
	at := controller.clock.Now().UTC()
	nodeOrder := append([]string(nil), plan.EntrypointNodeOrder[request.Action.Entrypoint]...)
	for attempt := 0; attempt < 4; attempt++ {
		owner, ownerExpected := contracts.ActiveRunOwner{}, uint64(0)
		if ingress.ownerKey != nil {
			owner, ownerExpected, err = controller.prepareActiveOwnerAcquire(ctx, ingress, request.RunID, at)
			if err != nil {
				return InvokeResult{}, err
			}
		}
		decision, admitErr := execution.AdmitRun(execution.AdmitRunCommand{
			RunID: request.RunID, NamespaceID: request.NamespaceID, ActionRef: request.Action.Ref(),
			ExecutionPlanRef: digestRef("plan", plan.PlanDigest), PlanDigest: plan.PlanDigest,
			TriggerRef: request.Trigger.EventID, TriggerDigest: admission.TriggerDigest, InputDigest: admission.InputDigest,
			ProvenanceArtifactRef: digestRef("provenance", admission.InputDigest), ScopeRef: request.ScopeRef,
			Parent: request.Parent, RootRunID: request.RootRunID,
			ActorRef: request.Trigger.ActorRef, SourceRef: request.Trigger.SourceRef, CorrelationRef: request.CorrelationRef,
			AdmissionPolicyRef: ingress.policyRef, AdmissionPolicyDigest: ingress.policyDigest,
			ActiveOwnerRef: ingress.ownerRef, ActiveOwnerGeneration: owner.Generation,
			CommandID: request.CommandID, At: at,
		})
		if admitErr != nil {
			return InvokeResult{}, admitErr
		}
		snapshot := RunSnapshot{
			SchemaVersion: "xgc.run-snapshot/v1", RunID: request.RunID, Action: request.Action,
			Definition: request.Definition, Plan: plan, Entrypoint: request.Action.Entrypoint, NodeOrder: nodeOrder,
			Inputs: admission.Inputs, Trigger: admission.Trigger.Payload, Scope: request.Scope,
			Provenance: admission.FieldProvenance, NodeOutputs: map[string]map[string]any{},
		}
		runMutation, mutationErr := aggregateMutation(runKey(request.RunID), 0, decision.Run)
		if mutationErr != nil {
			return InvokeResult{}, mutationErr
		}
		snapshotMutation, mutationErr := aggregateMutation(snapshotKey(request.RunID), 0, snapshot)
		if mutationErr != nil {
			return InvokeResult{}, mutationErr
		}
		snapshotEvent, eventErr := aggregateEvent(snapshotMutation, "snapshot.created", request.CommandID, at, map[string]any{
			"planDigest": plan.PlanDigest, "entrypoint": request.Action.Entrypoint, "inputDigest": admission.InputDigest,
		})
		if eventErr != nil {
			return InvokeResult{}, eventErr
		}
		expected := []store.ExpectedRevision{{Key: runMutation.Key, Revision: 0}, {Key: snapshotMutation.Key, Revision: 0}}
		mutations := []store.AggregateRecord{runMutation, snapshotMutation}
		events := append(append([]contracts.DomainEvent(nil), decision.Events...), snapshotEvent)
		if ingress.ownerKey != nil {
			ownerMutation, ownerErr := aggregateMutation(activeOwnerKey(owner.OwnerRef), ownerExpected, owner)
			if ownerErr != nil {
				return InvokeResult{}, ownerErr
			}
			ownerEvent, ownerErr := aggregateEvent(ownerMutation, "active-run-owner.acquired", request.CommandID, at, map[string]any{
				"runId": request.RunID, "ownerRef": owner.OwnerRef, "generation": owner.Generation,
				"policyRef": owner.PolicyRef, "keyDigest": owner.KeyDigest,
			})
			if ownerErr != nil {
				return InvokeResult{}, ownerErr
			}
			receipt := runAdmissionReceipt{
				SchemaVersion: runAdmissionReceiptSchema, CommandID: request.CommandID,
				RequestIdentityDigest: identityDigest, AcceptedRun: decision.Run,
				OwnerRef: owner.OwnerRef, OwnerGeneration: owner.Generation,
			}
			receiptMutation, receiptErr := aggregateMutation(runAdmissionReceiptKey(request.CommandID), 0, receipt)
			if receiptErr != nil {
				return InvokeResult{}, receiptErr
			}
			receiptEvent, receiptErr := aggregateEvent(receiptMutation, "run-admission.received", request.CommandID, at, map[string]any{
				"runId": request.RunID, "ownerRef": owner.OwnerRef, "requestIdentityDigest": identityDigest,
			})
			if receiptErr != nil {
				return InvokeResult{}, receiptErr
			}
			expected = append(expected,
				store.ExpectedRevision{Key: ownerMutation.Key, Revision: ownerExpected},
				store.ExpectedRevision{Key: receiptMutation.Key, Revision: 0},
			)
			mutations = append(mutations, ownerMutation, receiptMutation)
			events = append(events, ownerEvent, receiptEvent)
		}
		outcome, marshalErr := canonicaljson.Marshal(decision.Run)
		if marshalErr != nil {
			return InvokeResult{}, marshalErr
		}
		committed, commitErr := controller.store.Commit(ctx, store.Transaction{
			CommandID: request.CommandID, IdentityDigest: identityDigest, Expected: expected,
			Mutations: mutations, Events: events, Outcome: outcome, At: at,
		})
		if commitErr == nil {
			var run contracts.Run
			if err := canonicaljson.UnmarshalStrict(committed.Outcome, &run); err != nil {
				return InvokeResult{}, err
			}
			return InvokeResult{Run: run, Replay: committed.Replay}, nil
		}
		if ingress.ownerKey == nil || (!errors.Is(commitErr, store.ErrRevisionConflict) && !errors.Is(commitErr, store.ErrIdentityConflict)) {
			return InvokeResult{}, commitErr
		}
		if receipt, found, receiptErr := controller.getRunAdmissionReceipt(ctx, request.CommandID); receiptErr != nil {
			return InvokeResult{}, receiptErr
		} else if found {
			if receipt.RequestIdentityDigest != identityDigest {
				return InvokeResult{}, store.ErrIdentityConflict
			}
			return InvokeResult{Run: receipt.AcceptedRun, Replay: true}, nil
		}
		if _, runErr := controller.store.GetAggregate(ctx, runKey(request.RunID)); runErr == nil {
			return InvokeResult{}, commitErr
		} else if !errors.Is(runErr, store.ErrNotFound) {
			return InvokeResult{}, runErr
		}
		current, ownerErr := controller.GetActiveRunOwner(ctx, *ingress.ownerKey)
		if ownerErr != nil {
			return InvokeResult{}, ownerErr
		}
		if current.State == contracts.ActiveRunOwnerActive {
			return InvokeResult{}, &ActiveOwnerConflictError{Owner: current}
		}
		if attempt == 3 {
			return InvokeResult{}, commitErr
		}
	}
	return InvokeResult{}, store.ErrRevisionConflict
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
		if descriptor.SchemaMode == contracts.NodeSchemaCallAction {
			if workflowNode.CallAction == nil || workflowNode.InputSchema.Type != contracts.TypeObject ||
				len(workflowNode.InputSchema.Properties) != 0 || len(workflowNode.InputSchema.Required) != 0 {
				return fmt.Errorf("workflow node %q call-action control descriptor requires an empty occurrence input and an explicit CallAction", workflowNode.NodeID)
			}
			continue
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
		if workflowNode.CallAction != nil &&
			(descriptor.Mode != contracts.NodeWaiting || descriptor.Determinism != contracts.NodeRecorded ||
				len(descriptor.AllowedEffectKinds) != 0 || descriptor.CompensationTypeRef != "") {
			return fmt.Errorf("workflow node %q child Action call requires a recorded waiting descriptor without effects or compensation", workflowNode.NodeID)
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
