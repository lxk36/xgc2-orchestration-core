package execution

import (
	"errors"
	"fmt"
	"time"

	"github.com/lxk36/xgc2-orchestration-core/sdk/go/contracts"
)

var (
	ErrInvalidTransition = errors.New("invalid execution transition")
	ErrRevisionConflict  = errors.New("execution revision conflict")
	ErrClosureOpen       = errors.New("execution ownership closure is open")
)

type AdmitRunCommand struct {
	RunID                 string
	NamespaceID           string
	ActionRef             contracts.ActionRef
	ExecutionPlanRef      string
	PlanDigest            string
	TriggerRef            string
	TriggerDigest         string
	InputDigest           string
	ProvenanceArtifactRef string
	ScopeRef              string
	Parent                *contracts.ParentRunLink
	RootRunID             string
	ActorRef              string
	SourceRef             string
	CorrelationRef        string
	CommandID             string
	At                    time.Time
}

type RunTransitionCommand struct {
	RunID            string
	ExpectedRevision uint64
	To               contracts.RunStatus
	Termination      *contracts.TerminationIntent
	ResultRef        string
	CleanupFailures  []contracts.StructuredFailure
	Closure          contracts.RunClosureFacts
	CommandID        string
	At               time.Time
}

type RunDecision struct {
	Run     contracts.Run
	Events  []contracts.DomainEvent
	Intents []contracts.DurableIntent
}

func AdmitRun(command AdmitRunCommand) (RunDecision, error) {
	if err := validateAdmitRun(command); err != nil {
		return RunDecision{}, err
	}
	rootRunID := command.RootRunID
	if rootRunID == "" {
		rootRunID = command.RunID
	}
	run := contracts.Run{
		RunID: command.RunID, NamespaceID: command.NamespaceID, ActionRef: command.ActionRef,
		ExecutionPlanRef: command.ExecutionPlanRef, PlanDigest: command.PlanDigest,
		TriggerRef: command.TriggerRef, TriggerDigest: command.TriggerDigest, InputDigest: command.InputDigest,
		ProvenanceArtifactRef: command.ProvenanceArtifactRef, ScopeRef: command.ScopeRef,
		Parent: cloneParent(command.Parent), RootRunID: rootRunID, ActorRef: command.ActorRef,
		SourceRef: command.SourceRef, CorrelationRef: command.CorrelationRef, Status: contracts.RunAccepted,
		AcceptedAt: command.At.UTC(), UpdatedAt: command.At.UTC(), Revision: 1,
	}
	if err := ValidateRun(run); err != nil {
		return RunDecision{}, err
	}
	event, err := newEvent("run", run.RunID, run.Revision, "run.accepted", command.CommandID, command.At, map[string]any{
		"actionRef": run.ActionRef, "planDigest": run.PlanDigest, "triggerDigest": run.TriggerDigest,
		"inputDigest": run.InputDigest,
	})
	if err != nil {
		return RunDecision{}, err
	}
	return RunDecision{Run: run, Events: []contracts.DomainEvent{event}}, nil
}

func TransitionRun(current contracts.Run, command RunTransitionCommand) (RunDecision, error) {
	if err := ValidateRun(current); err != nil {
		return RunDecision{}, fmt.Errorf("current run: %w", err)
	}
	if command.RunID != current.RunID {
		return RunDecision{}, errors.New("run transition targets another aggregate")
	}
	if command.ExpectedRevision != current.Revision {
		return RunDecision{}, ErrRevisionConflict
	}
	if command.At.IsZero() || command.At.Before(current.UpdatedAt) {
		return RunDecision{}, errors.New("run transition time is missing or moves backwards")
	}
	if err := validateIdentity(command.CommandID, "run command id"); err != nil {
		return RunDecision{}, err
	}
	if err := ValidateRunTransition(current.Status, command.To); err != nil {
		return RunDecision{}, err
	}
	if current.Status.Terminal() {
		return RunDecision{}, fmt.Errorf("%w: terminal run cannot transition", ErrInvalidTransition)
	}
	if command.To.Terminal() && command.Closure.RunRevision != current.Revision {
		return RunDecision{}, errors.New("run closure facts do not match the current run revision")
	}
	if err := validateRunTransitionGuards(current, command); err != nil {
		return RunDecision{}, err
	}

	next := cloneRun(current)
	next.Status = command.To
	next.Revision++
	next.UpdatedAt = command.At.UTC()
	if next.StartedAt == nil && command.To == contracts.RunRunning {
		started := command.At.UTC()
		next.StartedAt = &started
	}
	if command.To == contracts.RunStopping {
		next.Termination = cloneTermination(command.Termination)
	}
	if command.To == contracts.RunSucceeded {
		next.TerminationKind = contracts.TerminationCompleted
		next.ResultRef = command.ResultRef
	}
	if command.To == contracts.RunRejected {
		next.TerminationKind = contracts.TerminationRejected
	}
	if command.To.Terminal() {
		if next.Termination != nil && next.Termination.Kind.RequiresStopping() {
			next.TerminationKind = next.Termination.Kind
			next.PrimaryFailure = cloneFailure(next.Termination.PrimaryFailure)
		}
		next.CleanupFailures = cloneFailures(command.CleanupFailures)
		finished := command.At.UTC()
		next.FinishedAt = &finished
	}
	if err := ValidateRun(next); err != nil {
		return RunDecision{}, fmt.Errorf("next run: %w", err)
	}
	event, err := newEvent("run", next.RunID, next.Revision, "run."+string(command.To), command.CommandID, command.At, map[string]any{
		"from": current.Status, "to": command.To, "terminationKind": next.TerminationKind,
	})
	if err != nil {
		return RunDecision{}, err
	}
	decision := RunDecision{Run: next, Events: []contracts.DomainEvent{event}}
	if command.To == contracts.RunStopping {
		payload := map[string]any{"runId": next.RunID, "terminationKind": next.Termination.Kind, "runRevision": next.Revision}
		intent, err := newDurableIntent(contracts.IntentCleanup, next.RunID, next.Revision, payload)
		if err != nil {
			return RunDecision{}, err
		}
		decision.Intents = []contracts.DurableIntent{intent}
	}
	return decision, nil
}

func ValidateRunTransition(from, to contracts.RunStatus) error {
	if !from.Valid() || !to.Valid() || from == to {
		return fmt.Errorf("%w: run %q -> %q", ErrInvalidTransition, from, to)
	}
	allowed := map[contracts.RunStatus]map[contracts.RunStatus]bool{
		contracts.RunAccepted: {contracts.RunQueued: true, contracts.RunRunning: true, contracts.RunStopping: true, contracts.RunRejected: true},
		contracts.RunQueued:   {contracts.RunRunning: true, contracts.RunStopping: true},
		contracts.RunRunning:  {contracts.RunWaiting: true, contracts.RunStopping: true, contracts.RunSucceeded: true},
		contracts.RunWaiting:  {contracts.RunRunning: true, contracts.RunStopping: true, contracts.RunSucceeded: true},
		contracts.RunStopping: {contracts.RunFailed: true, contracts.RunCanceled: true, contracts.RunStopped: true},
	}
	if !allowed[from][to] {
		return fmt.Errorf("%w: run %q -> %q", ErrInvalidTransition, from, to)
	}
	return nil
}

func ValidateRun(run contracts.Run) error {
	for label, value := range map[string]string{
		"run id": run.RunID, "namespace id": run.NamespaceID, "execution plan ref": run.ExecutionPlanRef,
		"trigger ref": run.TriggerRef, "root run id": run.RootRunID, "actor ref": run.ActorRef, "source ref": run.SourceRef,
	} {
		if err := validateIdentity(value, label); err != nil {
			return err
		}
	}
	if !contracts.ValidIdentifier(run.ActionRef.ActionID) || !contracts.ValidIdentifier(run.ActionRef.Version) ||
		!contracts.ValidDigest(run.ActionRef.Digest) {
		return errors.New("run action ref is invalid")
	}
	for label, digest := range map[string]string{
		"plan digest": run.PlanDigest, "trigger digest": run.TriggerDigest, "input digest": run.InputDigest,
	} {
		if !contracts.ValidDigest(digest) {
			return fmt.Errorf("%s is invalid", label)
		}
	}
	for label, value := range map[string]string{
		"provenance artifact ref": run.ProvenanceArtifactRef, "scope ref": run.ScopeRef,
		"correlation ref": run.CorrelationRef, "result ref": run.ResultRef,
	} {
		if err := validateOptionalIdentity(value, label); err != nil {
			return err
		}
	}
	if !run.Status.Valid() || run.Revision == 0 || run.AcceptedAt.IsZero() || run.UpdatedAt.Before(run.AcceptedAt) {
		return errors.New("run state, revision, or timestamps are invalid")
	}
	if run.StartedAt != nil && (run.StartedAt.Before(run.AcceptedAt) || run.StartedAt.After(run.UpdatedAt)) {
		return errors.New("run startedAt is outside its lifecycle")
	}
	if run.Status.Terminal() != (run.FinishedAt != nil) {
		return errors.New("run terminal state and finishedAt disagree")
	}
	if run.FinishedAt != nil && (run.FinishedAt.Before(run.AcceptedAt) || run.FinishedAt.After(run.UpdatedAt)) {
		return errors.New("run finishedAt is outside its lifecycle")
	}
	if run.Status == contracts.RunStopping && run.Termination == nil {
		return errors.New("stopping run requires a termination intent")
	}
	if run.Termination != nil {
		if err := validateTermination(*run.Termination); err != nil {
			return err
		}
	}
	if err := validateFailure(run.PrimaryFailure, run.Status == contracts.RunFailed, "run primary failure"); err != nil {
		return err
	}
	for index := range run.CleanupFailures {
		if err := validateFailure(&run.CleanupFailures[index], true, fmt.Sprintf("cleanup failure %d", index)); err != nil {
			return err
		}
	}
	if run.Status == contracts.RunSucceeded && (run.TerminationKind != contracts.TerminationCompleted || run.ResultRef == "") {
		return errors.New("succeeded run requires completed termination kind and result ref")
	}
	if run.Status == contracts.RunRejected && run.TerminationKind != contracts.TerminationRejected {
		return errors.New("rejected run requires rejected termination kind")
	}
	if run.Status.Terminal() && !run.TerminationKind.Valid() {
		return errors.New("terminal run requires a termination kind")
	}
	if run.Parent != nil {
		if err := validateParent(*run.Parent, run.RunID, run.RootRunID); err != nil {
			return err
		}
	}
	return nil
}

func validateAdmitRun(command AdmitRunCommand) error {
	for label, value := range map[string]string{
		"run id": command.RunID, "namespace id": command.NamespaceID, "execution plan ref": command.ExecutionPlanRef,
		"trigger ref": command.TriggerRef, "actor ref": command.ActorRef, "source ref": command.SourceRef,
		"command id": command.CommandID,
	} {
		if err := validateIdentity(value, label); err != nil {
			return err
		}
	}
	if command.At.IsZero() {
		return errors.New("run admission time is required")
	}
	for _, digest := range []string{command.PlanDigest, command.TriggerDigest, command.InputDigest} {
		if !contracts.ValidDigest(digest) {
			return errors.New("run admission contains an invalid digest")
		}
	}
	if !contracts.ValidIdentifier(command.ActionRef.ActionID) || !contracts.ValidIdentifier(command.ActionRef.Version) ||
		!contracts.ValidDigest(command.ActionRef.Digest) {
		return errors.New("run admission action ref is invalid")
	}
	return nil
}

func validateRunTransitionGuards(current contracts.Run, command RunTransitionCommand) error {
	terminal := command.To.Terminal()
	if terminal && !command.Closure.Satisfied() {
		return ErrClosureOpen
	}
	if !terminal && command.ResultRef != "" {
		return errors.New("only a succeeded terminal run can publish a result")
	}
	if command.To == contracts.RunStopping {
		if current.Termination != nil {
			return errors.New("run termination intent is already frozen")
		}
		if command.Termination == nil || !command.Termination.Kind.RequiresStopping() {
			return errors.New("stopping requires a failed, canceled, or stopped intent")
		}
		if err := validateTermination(*command.Termination); err != nil {
			return err
		}
		return nil
	}
	if command.Termination != nil {
		return errors.New("termination intent can only be frozen on entry to stopping")
	}
	if current.Status == contracts.RunStopping {
		if current.Termination == nil || terminalStatus(current.Termination.Kind) != command.To {
			return errors.New("terminal run status does not match the frozen termination intent")
		}
	}
	if command.To == contracts.RunSucceeded && command.ResultRef == "" {
		return errors.New("succeeded run requires a result ref")
	}
	if command.To == contracts.RunRejected && current.Status != contracts.RunAccepted {
		return errors.New("only an accepted run can be rejected")
	}
	return nil
}

func validateTermination(intent contracts.TerminationIntent) error {
	if !intent.Kind.RequiresStopping() {
		return errors.New("termination intent kind is invalid")
	}
	for label, value := range map[string]string{
		"termination requestedBy": intent.RequestedBy, "termination reasonCode": intent.ReasonCode,
		"termination command id": intent.CommandID,
	} {
		if err := validateIdentity(value, label); err != nil {
			return err
		}
	}
	if err := validateCanonicalText(intent.Reason, "termination reason", maxMessageBytes, false); err != nil {
		return err
	}
	if intent.RequestedAt.IsZero() {
		return errors.New("termination requestedAt is required")
	}
	return validateFailure(intent.PrimaryFailure, intent.Kind == contracts.TerminationFailed, "termination primary failure")
}

func terminalStatus(kind contracts.TerminationKind) contracts.RunStatus {
	switch kind {
	case contracts.TerminationFailed:
		return contracts.RunFailed
	case contracts.TerminationCanceled:
		return contracts.RunCanceled
	case contracts.TerminationStopped:
		return contracts.RunStopped
	default:
		return ""
	}
}

func validateParent(parent contracts.ParentRunLink, runID, rootRunID string) error {
	for label, value := range map[string]string{
		"parent run id": parent.ParentRunID, "parent invocation id": parent.ParentInvocationID, "call node id": parent.CallNodeID,
	} {
		if err := validateIdentity(value, label); err != nil {
			return err
		}
	}
	if !contracts.ValidDigest(parent.MappingDigest) || parent.ParentRunID == runID || rootRunID == runID {
		return errors.New("parent run link or mapping digest is invalid")
	}
	return nil
}

func cloneRun(run contracts.Run) contracts.Run {
	run.Parent = cloneParent(run.Parent)
	run.Termination = cloneTermination(run.Termination)
	run.PrimaryFailure = cloneFailure(run.PrimaryFailure)
	run.CleanupFailures = cloneFailures(run.CleanupFailures)
	run.StartedAt = cloneTime(run.StartedAt)
	run.FinishedAt = cloneTime(run.FinishedAt)
	return run
}

func cloneParent(parent *contracts.ParentRunLink) *contracts.ParentRunLink {
	if parent == nil {
		return nil
	}
	copy := *parent
	return &copy
}

func cloneTermination(intent *contracts.TerminationIntent) *contracts.TerminationIntent {
	if intent == nil {
		return nil
	}
	copy := *intent
	copy.PrimaryFailure = cloneFailure(intent.PrimaryFailure)
	return &copy
}

func cloneFailure(failure *contracts.StructuredFailure) *contracts.StructuredFailure {
	if failure == nil {
		return nil
	}
	copy := *failure
	return &copy
}

func cloneFailures(failures []contracts.StructuredFailure) []contracts.StructuredFailure {
	return append([]contracts.StructuredFailure(nil), failures...)
}

func cloneTime(value *time.Time) *time.Time {
	if value == nil {
		return nil
	}
	copy := *value
	return &copy
}
