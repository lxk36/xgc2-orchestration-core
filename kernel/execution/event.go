package execution

import (
	"fmt"
	"time"

	"github.com/lxk36/xgc2-orchestration-core/kernel/canonicaljson"
	"github.com/lxk36/xgc2-orchestration-core/sdk/go/contracts"
)

// eventPayloadSchemaDigest identifies the deliberately open v1 event payload
// schema: {"additionalProperties":true,"type":"object"}. Event types own the
// meaning of their payload fields; the envelope remains stable across them.
const eventPayloadSchemaDigest = "sha256:82ef96cebaf5fbe16269fd18b0240d78f5b9b90a4155a17eb797115b09148ecf"

func newEvent(aggregateType, aggregateID string, revision uint64, eventType, commandID string, at time.Time, payload map[string]any) (contracts.DomainEvent, error) {
	if err := validateIdentity(aggregateType, "event aggregate type"); err != nil {
		return contracts.DomainEvent{}, err
	}
	if err := validateIdentity(aggregateID, "event aggregate id"); err != nil {
		return contracts.DomainEvent{}, err
	}
	if revision == 0 || at.IsZero() {
		return contracts.DomainEvent{}, fmt.Errorf("event revision and time are required")
	}
	if err := validateIdentity(eventType, "event type"); err != nil {
		return contracts.DomainEvent{}, err
	}
	if err := validateIdentity(commandID, "event command id"); err != nil {
		return contracts.DomainEvent{}, err
	}
	if payload == nil {
		payload = map[string]any{}
	}
	payloadDigest, err := canonicaljson.DigestValue(payload)
	if err != nil {
		return contracts.DomainEvent{}, fmt.Errorf("event payload: %w", err)
	}
	eventID, err := StableEventID(aggregateType, aggregateID, revision)
	if err != nil {
		return contracts.DomainEvent{}, err
	}
	return contracts.DomainEvent{
		EventID: eventID, AggregateType: aggregateType, AggregateID: aggregateID, AggregateRevision: revision,
		Type: eventType, CommandID: commandID, PayloadSchemaDigest: eventPayloadSchemaDigest,
		PayloadDigest: payloadDigest, Payload: payload, OccurredAt: at.UTC(),
	}, nil
}

func newDurableIntent(kind contracts.DurableIntentKind, aggregateID string, revision uint64, payload map[string]any) (contracts.DurableIntent, error) {
	if payload == nil {
		payload = map[string]any{}
	}
	digest, err := canonicaljson.DigestValue(payload)
	if err != nil {
		return contracts.DurableIntent{}, fmt.Errorf("durable intent payload: %w", err)
	}
	id, err := StableIntentID(kind, aggregateID, revision)
	if err != nil {
		return contracts.DurableIntent{}, err
	}
	return contracts.DurableIntent{Kind: kind, Identity: id, AggregateID: aggregateID, PayloadDigest: digest, Payload: payload}, nil
}
