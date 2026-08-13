// Package execution implements deterministic, product-neutral reducers for
// Run, Invocation, and Attempt aggregates. Reducers perform no I/O and do not
// own a clock, scheduler, worker, or persistence implementation.
package execution

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"strings"
	"unicode/utf8"

	"github.com/lxk36/xgc2-orchestration-core/kernel/canonicaljson"
	"github.com/lxk36/xgc2-orchestration-core/sdk/go/contracts"
)

const (
	maxIdentityBytes = 256
	maxMessageBytes  = 4096
)

func StableInvocationID(runID, nodeID string) (string, error) {
	return stableID("inv", "xgc.invocation/v1", runID, nodeID)
}

func StableChildRunID(parentInvocationID string, action contracts.ActionRef) (string, error) {
	if !contracts.ValidIdentifier(parentInvocationID) || !contracts.ValidIdentifier(action.ActionID) ||
		!contracts.ValidIdentifier(action.Version) || !contracts.ValidDigest(action.Digest) {
		return "", fmt.Errorf("child Run parent invocation or Action ref is invalid")
	}
	return stableID("run", "xgc.child-run/v1", parentInvocationID, action)
}

func StableChildTriggerEventID(childRunID string) (string, error) {
	if !contracts.ValidIdentifier(childRunID) {
		return "", fmt.Errorf("child trigger Run identity is invalid")
	}
	return stableID("trg", "xgc.child-trigger/v1", childRunID)
}

func StableAttemptID(invocationID string, phase contracts.AttemptPhase, ordinal uint32) (string, error) {
	return stableID("att", "xgc.attempt/v1", invocationID, string(phase), ordinal)
}

func StableEventID(aggregateType, aggregateID string, revision uint64) (string, error) {
	return stableID("evt", "xgc.domain-event/v1", aggregateType, aggregateID, revision)
}

func StableIntentID(kind contracts.DurableIntentKind, aggregateID string, revision uint64) (string, error) {
	return stableID("dti", "xgc.durable-intent/v1", string(kind), aggregateID, revision)
}

func stableID(prefix, domain string, parts ...any) (string, error) {
	if !contracts.ValidIdentifier(prefix) || !contracts.ValidTypeRef(domain) {
		return "", fmt.Errorf("stable identity domain %q or prefix %q is invalid", domain, prefix)
	}
	payload := append([]any{domain}, parts...)
	canonical, err := canonicaljson.Marshal(payload)
	if err != nil {
		return "", fmt.Errorf("stable identity payload: %w", err)
	}
	sum := sha256.Sum256(canonical)
	return prefix + "-" + hex.EncodeToString(sum[:]), nil
}

// PrivateTokenDigest exposes the one hash profile used for lease,
// idempotency, and capability tokens. Raw tokens are never public state.
func PrivateTokenDigest(value string) (string, error) {
	if err := validateCanonicalText(value, "private identity", 512, true); err != nil {
		return "", err
	}
	sum := sha256.Sum256([]byte(value))
	return "sha256:" + hex.EncodeToString(sum[:]), nil
}

func validateIdentity(value, label string) error {
	if !contracts.ValidIdentifier(value) {
		return fmt.Errorf("%s must be a portable identifier", label)
	}
	return nil
}

func validateOptionalIdentity(value, label string) error {
	if value == "" {
		return nil
	}
	return validateIdentity(value, label)
}

func validateCanonicalText(value, label string, maximum int, required bool) error {
	if value == "" {
		if required {
			return fmt.Errorf("%s is required", label)
		}
		return nil
	}
	if !utf8.ValidString(value) || len(value) > maximum || strings.TrimSpace(value) != value || strings.ContainsRune(value, '\x00') {
		return fmt.Errorf("%s must be canonical UTF-8 of at most %d bytes", label, maximum)
	}
	return nil
}

func validateFailure(failure *contracts.StructuredFailure, required bool, label string) error {
	if failure == nil {
		if required {
			return fmt.Errorf("%s is required", label)
		}
		return nil
	}
	if !failure.Class.Valid() {
		return fmt.Errorf("%s class %q is invalid", label, failure.Class)
	}
	if err := validateIdentity(failure.Code, label+" code"); err != nil {
		return err
	}
	if err := validateCanonicalText(failure.Message, label+" message", maxMessageBytes, true); err != nil {
		return err
	}
	return validateOptionalIdentity(failure.EvidenceRef, label+" evidence ref")
}
