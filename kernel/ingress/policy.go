// Package ingress defines composition-time capabilities for privileged root
// ingress. Capability seals exist only in memory and are never protocol data.
package ingress

import (
	"crypto/rand"
	"errors"
	"fmt"

	"github.com/lxk36/xgc2-orchestration-core/kernel/canonicaljson"
	"github.com/lxk36/xgc2-orchestration-core/sdk/go/contracts"
)

type ReservedIngressPolicySpec struct {
	PolicyRef                 string
	NamespaceID               string
	TriggerKind               contracts.TriggerKind
	TriggerVersion            string
	SourceRef                 string
	CandidateOrigin           contracts.InputOriginKind
	RootOnly                  bool
	RequireActiveOwner        bool
	ActiveOwnerKind           string
	ActiveOwnerIdentityFields []string
}

type capabilitySeal struct{ nonce [32]byte }

// ReservedIngressPolicy and IngressPermit share a random, non-zero,
// unexported seal. Equivalent public policy strings cannot mint a permit for a
// policy registered by another composition root.
type ReservedIngressPolicy struct {
	spec ReservedIngressPolicySpec
	seal *capabilitySeal
}

type IngressPermit struct{ seal *capabilitySeal }

func NewReservedIngressPolicy(spec ReservedIngressPolicySpec) (*ReservedIngressPolicy, *IngressPermit, error) {
	spec.ActiveOwnerIdentityFields = append([]string(nil), spec.ActiveOwnerIdentityFields...)
	if err := validatePolicy(spec); err != nil {
		return nil, nil, err
	}
	seal := &capabilitySeal{}
	if _, err := rand.Read(seal.nonce[:]); err != nil {
		return nil, nil, fmt.Errorf("issue reserved ingress capability: %w", err)
	}
	return &ReservedIngressPolicy{spec: spec, seal: seal}, &IngressPermit{seal: seal}, nil
}

func (policy *ReservedIngressPolicy) Spec() (ReservedIngressPolicySpec, bool) {
	if policy == nil || policy.seal == nil || validatePolicy(policy.spec) != nil {
		return ReservedIngressPolicySpec{}, false
	}
	spec := policy.spec
	spec.ActiveOwnerIdentityFields = append([]string(nil), spec.ActiveOwnerIdentityFields...)
	return spec, true
}

func (policy *ReservedIngressPolicy) Authorizes(permit *IngressPermit) bool {
	return policy != nil && policy.seal != nil && permit != nil && permit.seal == policy.seal
}

func (policy *ReservedIngressPolicy) Digest() string {
	spec, valid := policy.Spec()
	if !valid {
		return ""
	}
	digest, _ := PolicyDigest(spec)
	return digest
}

func PolicyDigest(spec ReservedIngressPolicySpec) (string, error) {
	if err := validatePolicy(spec); err != nil {
		return "", err
	}
	return canonicaljson.DigestValue(map[string]any{
		"schemaVersion": "xgc.reserved-ingress-policy/v1", "policyRef": spec.PolicyRef,
		"namespaceId": spec.NamespaceID, "triggerKind": spec.TriggerKind,
		"triggerVersion": spec.TriggerVersion, "sourceRef": spec.SourceRef,
		"candidateOrigin": spec.CandidateOrigin, "rootOnly": spec.RootOnly,
		"requireActiveOwner": spec.RequireActiveOwner, "activeOwnerKind": spec.ActiveOwnerKind,
		"activeOwnerIdentityFields": spec.ActiveOwnerIdentityFields,
	})
}

func validatePolicy(spec ReservedIngressPolicySpec) error {
	if !contracts.ValidIdentifier(spec.PolicyRef) || !contracts.ValidIdentifier(spec.NamespaceID) ||
		!spec.TriggerKind.Valid() || !contracts.ValidIdentifier(spec.TriggerVersion) ||
		!contracts.ValidIdentifier(spec.SourceRef) {
		return errors.New("reserved ingress policy identity is invalid")
	}
	expected, err := expectedCandidateOrigin(spec.TriggerKind)
	if err != nil || spec.CandidateOrigin != expected {
		return errors.New("reserved ingress policy candidate origin does not match its trigger")
	}
	if spec.RequireActiveOwner && !spec.RootOnly {
		return errors.New("active owner policy must be root-only")
	}
	if spec.TriggerKind == contracts.TriggerActionCall ||
		(spec.TriggerKind == contracts.TriggerProductBuilder && !spec.RootOnly) {
		return errors.New("reserved child ingress is forbidden and product-builder policy must be root-only")
	}
	if spec.RequireActiveOwner {
		if !contracts.ValidIdentifier(spec.ActiveOwnerKind) || len(spec.ActiveOwnerIdentityFields) == 0 ||
			len(spec.ActiveOwnerIdentityFields) > 32 {
			return errors.New("active owner policy key contract is invalid")
		}
		prior := ""
		for _, field := range spec.ActiveOwnerIdentityFields {
			if !contracts.ValidIdentifier(field) || field <= prior {
				return errors.New("active owner identity fields must be unique and sorted")
			}
			prior = field
		}
	} else if spec.ActiveOwnerKind != "" || len(spec.ActiveOwnerIdentityFields) != 0 {
		return errors.New("policy without active ownership cannot declare an owner key")
	}
	return nil
}

func expectedCandidateOrigin(kind contracts.TriggerKind) (contracts.InputOriginKind, error) {
	switch kind {
	case contracts.TriggerManual, contracts.TriggerAPI, contracts.TriggerPanel:
		return contracts.OriginCaller, nil
	case contracts.TriggerSchedule, contracts.TriggerWebhook:
		return contracts.OriginTriggerMap, nil
	case contracts.TriggerProductBuilder:
		return contracts.OriginProductBuilder, nil
	case contracts.TriggerActionCall:
		return contracts.OriginParentMap, nil
	default:
		return "", errors.New("unsupported trigger kind")
	}
}
