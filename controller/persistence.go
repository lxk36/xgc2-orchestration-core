package controller

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/lxk36/xgc2-orchestration-core/durable/store"
	"github.com/lxk36/xgc2-orchestration-core/kernel/canonicaljson"
	"github.com/lxk36/xgc2-orchestration-core/kernel/effect"
	"github.com/lxk36/xgc2-orchestration-core/kernel/execution"
	"github.com/lxk36/xgc2-orchestration-core/sdk/go/contracts"
)

func runKey(runID string) store.AggregateKey {
	return store.AggregateKey{Type: runAggregateType, ID: runID}
}

func snapshotKey(runID string) store.AggregateKey {
	return store.AggregateKey{Type: snapshotAggregateType, ID: runID}
}

func effectKey(effectID string) store.AggregateKey {
	return store.AggregateKey{Type: effectAggregateType, ID: effectID}
}

func commandLedgerKey(commandID string) store.AggregateKey {
	return store.AggregateKey{Type: commandLedgerType, ID: commandID}
}

func aggregateMutation(key store.AggregateKey, currentRevision uint64, payload any) (store.AggregateRecord, error) {
	raw, err := canonicaljson.Marshal(payload)
	if err != nil {
		return store.AggregateRecord{}, err
	}
	digest, err := canonicaljson.Digest(raw)
	if err != nil {
		return store.AggregateRecord{}, err
	}
	return store.AggregateRecord{Key: key, Revision: currentRevision + 1, PayloadDigest: digest, Payload: raw}, nil
}

func aggregateEvent(record store.AggregateRecord, eventType, commandID string, at time.Time, payload map[string]any) (contracts.DomainEvent, error) {
	eventID, err := execution.StableEventID(record.Key.Type, record.Key.ID, record.Revision)
	if err != nil {
		return contracts.DomainEvent{}, err
	}
	payloadDigest, err := canonicaljson.DigestValue(payload)
	if err != nil {
		return contracts.DomainEvent{}, err
	}
	return contracts.DomainEvent{
		EventID: eventID, AggregateType: record.Key.Type, AggregateID: record.Key.ID,
		AggregateRevision: record.Revision, Type: eventType, CommandID: commandID,
		PayloadSchemaDigest: eventSchemaDigest, PayloadDigest: payloadDigest, Payload: payload, OccurredAt: at,
	}, nil
}

func decodeRun(record store.AggregateRecord) (contracts.Run, error) {
	var run contracts.Run
	if err := canonicaljson.UnmarshalStrict(record.Payload, &run); err != nil {
		return contracts.Run{}, err
	}
	if err := execution.ValidateRun(run); err != nil {
		return contracts.Run{}, err
	}
	return run, nil
}

func decodeLedger(record store.AggregateRecord) (contracts.InvocationLedger, error) {
	var ledger contracts.InvocationLedger
	if err := canonicaljson.UnmarshalStrict(record.Payload, &ledger); err != nil {
		return contracts.InvocationLedger{}, err
	}
	if err := execution.ValidateInvocationLedger(ledger); err != nil {
		return contracts.InvocationLedger{}, err
	}
	return ledger, nil
}

func decodeEffect(record store.AggregateRecord) (contracts.EffectRecord, error) {
	var current contracts.EffectRecord
	if err := canonicaljson.UnmarshalStrict(record.Payload, &current); err != nil {
		return contracts.EffectRecord{}, err
	}
	if err := effect.ValidateRecord(current); err != nil {
		return contracts.EffectRecord{}, err
	}
	return current, nil
}

func decodeCommandLedger(record store.AggregateRecord) (contracts.CommandLedger, error) {
	var ledger contracts.CommandLedger
	if err := canonicaljson.UnmarshalStrict(record.Payload, &ledger); err != nil {
		return contracts.CommandLedger{}, err
	}
	if err := effect.ValidateLedger(ledger); err != nil {
		return contracts.CommandLedger{}, err
	}
	return ledger, nil
}

func (controller *Controller) commitRunDecision(ctx context.Context, expected uint64, decision execution.RunDecision, commandID string, at time.Time) error {
	mutation, err := aggregateMutation(runKey(decision.Run.RunID), expected, decision.Run)
	if err != nil {
		return err
	}
	return controller.commit(ctx, commandID, at, []store.ExpectedRevision{{Key: mutation.Key, Revision: expected}}, []store.AggregateRecord{mutation}, decision.Events, decision.Intents, decision.Run)
}

func (controller *Controller) commitInvocationDecision(
	ctx context.Context,
	expected uint64,
	decision execution.InvocationDecision,
	commandID string,
	at time.Time,
	snapshotMutation *store.AggregateRecord,
	snapshotEvent *contracts.DomainEvent,
) error {
	invocationMutation, err := aggregateMutation(store.AggregateKey{Type: invocationType, ID: decision.Ledger.Invocation.InvocationID}, expected, decision.Ledger)
	if err != nil {
		return err
	}
	expectedRecords := []store.ExpectedRevision{{Key: invocationMutation.Key, Revision: expected}}
	mutations := []store.AggregateRecord{invocationMutation}
	events := append([]contracts.DomainEvent(nil), decision.Events...)
	if snapshotMutation != nil && snapshotEvent != nil {
		expectedRecords = append(expectedRecords, store.ExpectedRevision{Key: snapshotMutation.Key, Revision: snapshotMutation.Revision - 1})
		mutations = append(mutations, *snapshotMutation)
		events = append(events, *snapshotEvent)
	}
	return controller.commit(ctx, commandID, at, expectedRecords, mutations, events, decision.Intents, decision.Ledger.Invocation)
}

func (controller *Controller) commitWaiting(
	ctx context.Context,
	runExpected uint64,
	invocationExpected uint64,
	snapshotExpected uint64,
	invocation execution.InvocationDecision,
	run execution.RunDecision,
	snapshot store.AggregateRecord,
	effects []store.AggregateRecord,
	effectEvents []contracts.DomainEvent,
	commandID string,
	at time.Time,
) error {
	var projected RunSnapshot
	if err := canonicaljson.UnmarshalStrict(snapshot.Payload, &projected); err != nil {
		return err
	}
	if projected.RunID != run.Run.RunID || snapshot.Key != snapshotKey(projected.RunID) {
		return errors.New("waiting Snapshot identity differs from its Run or aggregate key")
	}
	if err := validateRunSnapshot(projected); err != nil {
		return err
	}
	if err := validateSnapshotRunState(projected, run.Run); err != nil {
		return err
	}
	if projected.Waiting != nil {
		if err := validateSnapshotWaitingOccurrence(projected, invocation.Ledger); err != nil {
			return err
		}
	}
	runMutation, err := aggregateMutation(runKey(run.Run.RunID), runExpected, run.Run)
	if err != nil {
		return err
	}
	invocationMutation, err := aggregateMutation(store.AggregateKey{Type: invocationType, ID: invocation.Ledger.Invocation.InvocationID}, invocationExpected, invocation.Ledger)
	if err != nil {
		return err
	}
	expected := []store.ExpectedRevision{
		{Key: runMutation.Key, Revision: runExpected},
		{Key: invocationMutation.Key, Revision: invocationExpected},
		{Key: snapshot.Key, Revision: snapshotExpected},
	}
	mutations := []store.AggregateRecord{runMutation, invocationMutation, snapshot}
	snapshotEvent, err := aggregateEvent(snapshot, "snapshot.node-waiting", commandID, at, map[string]any{
		"nodeId":         invocation.Ledger.Invocation.NodeID,
		"invocationId":   invocation.Ledger.Invocation.InvocationID,
		"waitGeneration": invocation.Ledger.Invocation.WaitGeneration,
	})
	if err != nil {
		return err
	}
	events := append(append(append([]contracts.DomainEvent{}, run.Events...), invocation.Events...), snapshotEvent)
	events = append(events, effectEvents...)
	for _, mutation := range effects {
		expected = append(expected, store.ExpectedRevision{Key: mutation.Key, Revision: 0})
		mutations = append(mutations, mutation)
	}
	return controller.commit(ctx, commandID, at, expected, mutations, events, append(run.Intents, invocation.Intents...), run.Run)
}

func (controller *Controller) commit(
	ctx context.Context,
	commandID string,
	at time.Time,
	expected []store.ExpectedRevision,
	mutations []store.AggregateRecord,
	events []contracts.DomainEvent,
	intents []contracts.DurableIntent,
	outcome any,
) error {
	identityDigest, err := canonicaljson.DigestValue(map[string]any{"commandId": commandID, "mutations": mutations})
	if err != nil {
		return err
	}
	outcomeRaw, err := canonicaljson.Marshal(outcome)
	if err != nil {
		return err
	}
	seeds := make([]store.IntentSeed, len(intents))
	for index := range intents {
		seeds[index] = store.IntentSeed{Intent: intents[index], AvailableAt: at}
	}
	_, err = controller.store.Commit(ctx, store.Transaction{
		CommandID: commandID, IdentityDigest: identityDigest, Expected: expected, Mutations: mutations,
		Events: events, Intents: seeds, Outcome: outcomeRaw, At: at,
	})
	return err
}

func phaseCommand(runID, phase string, revision uint64) string {
	clean := strings.Trim(strings.Map(func(value rune) rune {
		if (value >= 'a' && value <= 'z') || (value >= 'A' && value <= 'Z') || (value >= '0' && value <= '9') || value == '.' || value == '_' || value == '-' {
			return value
		}
		return '-'
	}, phase), "-")
	if len(clean) > 24 {
		clean = clean[:24]
	}
	payload := fmt.Sprintf("%s\x00%s\x00%d", runID, phase, revision)
	sum := sha256.Sum256([]byte(payload))
	return "cmd-" + clean + "-" + hex.EncodeToString(sum[:])
}

func digestRef(prefix, digest string) string {
	return prefix + "-" + strings.TrimPrefix(digest, "sha256:")
}

func structuredFailure(code string, cause error) contracts.StructuredFailure {
	if !contracts.ValidIdentifier(code) {
		code = "orchestration.failure"
	}
	message := cause.Error()
	if len(message) > 4096 {
		message = message[:4096]
	}
	return contracts.StructuredFailure{Class: contracts.FailurePermanent, Code: code, Message: message}
}

func requireFailure(failure *contracts.StructuredFailure) (contracts.StructuredFailure, error) {
	if failure == nil {
		return contracts.StructuredFailure{}, errors.New("node failure result is missing its failure")
	}
	return *failure, nil
}
