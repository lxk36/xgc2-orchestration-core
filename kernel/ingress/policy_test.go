package ingress

import (
	"testing"

	"github.com/lxk36/xgc2-orchestration-core/sdk/go/contracts"
)

func TestPolicyCapabilityCannotBeForgedFromEquivalentFacts(t *testing.T) {
	spec := ReservedIngressPolicySpec{
		PolicyRef: "experiment-builder-v1", NamespaceID: "xgc2-experiments",
		TriggerKind: contracts.TriggerProductBuilder, TriggerVersion: "v1", SourceRef: "xgc2-experiment-builder",
		CandidateOrigin: contracts.OriginProductBuilder, RootOnly: true, RequireActiveOwner: true,
		ActiveOwnerKind:           "configuration.resource-branch",
		ActiveOwnerIdentityFields: []string{"branch", "domain", "resourceId"},
	}
	policy, permit, err := NewReservedIngressPolicy(spec)
	if err != nil || !policy.Authorizes(permit) || policy.Authorizes(&IngressPermit{}) {
		t.Fatalf("issued capability is invalid: policy=%#v err=%v", policy, err)
	}
	otherPolicy, otherPermit, err := NewReservedIngressPolicy(spec)
	if err != nil || policy.Authorizes(otherPermit) || otherPolicy.Authorizes(permit) {
		t.Fatalf("equivalent facts forged a capability: err=%v", err)
	}
	digest := policy.Digest()
	spec.ActiveOwnerIdentityFields[0] = "changed"
	frozen, valid := policy.Spec()
	if !valid || frozen.ActiveOwnerIdentityFields[0] != "branch" || policy.Digest() != digest {
		t.Fatalf("caller mutated frozen policy: %#v valid=%v", frozen, valid)
	}
	frozen.ActiveOwnerIdentityFields[0] = "changed-again"
	again, valid := policy.Spec()
	if !valid || again.ActiveOwnerIdentityFields[0] != "branch" {
		t.Fatalf("returned policy spec aliased internal state: %#v valid=%v", again, valid)
	}
}

func TestPolicyDigestPinsEveryAdmissionFact(t *testing.T) {
	base := ReservedIngressPolicySpec{
		PolicyRef: "builder-v1", NamespaceID: "reserved", TriggerKind: contracts.TriggerProductBuilder,
		TriggerVersion: "v1", SourceRef: "builder", CandidateOrigin: contracts.OriginProductBuilder, RootOnly: true,
	}
	left, err := PolicyDigest(base)
	if err != nil {
		t.Fatal(err)
	}
	base.SourceRef = "other-builder"
	right, err := PolicyDigest(base)
	if err != nil || left == right {
		t.Fatalf("policy fact was absent from digest: %s %s err=%v", left, right, err)
	}
}
