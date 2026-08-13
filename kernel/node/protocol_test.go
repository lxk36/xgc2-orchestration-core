package node

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/lxk36/xgc2-orchestration-core/kernel/canonicaljson"
	"github.com/lxk36/xgc2-orchestration-core/sdk/go/contracts"
	nodesdk "github.com/lxk36/xgc2-orchestration-core/sdk/go/node"
)

const digest = "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"

func TestRegistryFreezesDescriptorsAndValidatesStructuredResult(t *testing.T) {
	descriptor := pureDescriptor(t)
	executor := nodesdk.ExecutorFunc{
		NodeDescriptor: descriptor,
		Function: func(_ context.Context, request contracts.NodeInvocationRequest) (contracts.NodeResult, error) {
			output := map[string]any{"message": request.Input["message"]}
			outputDigest, _ := canonicaljson.DigestValue(output)
			return contracts.NodeResult{Status: contracts.NodeResultSucceeded, Output: output, OutputDigest: outputDigest, EvidenceDigest: outputDigest}, nil
		},
	}
	registry := NewRegistry()
	if err := registry.Register(executor); err != nil {
		t.Fatal(err)
	}
	executor.NodeDescriptor.DisplayName = "mutated after registration"
	catalogDigest, err := registry.Seal()
	if err != nil || !contracts.ValidDigest(catalogDigest) {
		t.Fatalf("catalog digest = %q, err %v", catalogDigest, err)
	}
	request := requestFor(t, descriptor)
	result, err := registry.Execute(context.Background(), request)
	if err != nil {
		t.Fatal(err)
	}
	if result.Output["message"] != "hello" {
		t.Fatalf("node result = %#v", result)
	}
	catalog, returnedDigest, err := registry.Catalog()
	if err != nil || returnedDigest != catalogDigest || len(catalog) != 1 || catalog[0].DisplayName != descriptor.DisplayName {
		t.Fatalf("catalog = %#v, digest %q, err %v", catalog, returnedDigest, err)
	}
	catalog[0].DisplayName = "caller mutation"
	again, _, _ := registry.Catalog()
	if again[0].DisplayName != descriptor.DisplayName {
		t.Fatal("published catalog was mutable")
	}
	if err := registry.Register(executor); !errors.Is(err, ErrRegistrySealed) {
		t.Fatalf("late registration error = %v", err)
	}
}

func TestCapabilitiesAndEffectsFailClosed(t *testing.T) {
	descriptor := pureDescriptor(t)
	descriptor.TypeRef = "xgc.test-effect-node/v1"
	descriptor.Mode = contracts.NodeEffectful
	descriptor.RequiredCapabilities = []contracts.CapabilityRequirement{{CapabilityRef: "mcp.invoke", Scope: "project"}}
	descriptor.AllowedEffectKinds = []string{"xgc.mcp-call/v1"}
	descriptor.DescriptorDigest = ""
	descriptor.DescriptorDigest, _ = DescriptorDigest(descriptor)
	if err := ValidateDescriptor(descriptor); err != nil {
		t.Fatal(err)
	}
	request := requestFor(t, descriptor)
	if err := ValidateRequest(descriptor, request); err == nil {
		t.Fatal("missing required capability was accepted")
	}
	request.CapabilityGrants = []contracts.CapabilityGrant{{
		CapabilityRef: "mcp.invoke", Scope: "project", HandleRef: "grant-1", AuthorizationDigest: digest,
		ExpiresAt: request.Deadline,
	}}
	if err := ValidateRequest(descriptor, request); err != nil {
		t.Fatal(err)
	}
	intent := map[string]any{"tool": "issue.create"}
	intentDigest, _ := canonicaljson.DigestValue(intent)
	result := contracts.NodeResult{
		Status: contracts.NodeResultWaiting, EvidenceDigest: digest,
		Effects: []contracts.EffectProposal{{
			EffectKey: "invoke-tool", Kind: "xgc.mcp-call/v1", TargetRef: "mcp-server-1",
			IntentSchemaDigest: digest, Intent: intent, IntentDigest: intentDigest, Ownership: contracts.EffectAttached,
			CompensationPolicy: contracts.CompensationNone, RequiredCapabilityRefs: []string{"mcp.invoke"},
			PolicyDigest: digest, Deadline: request.Deadline,
		}},
		Wait: &contracts.NodeWait{Kind: contracts.NodeWaitEffect, SubjectRef: "invoke-tool", ConditionDigest: intentDigest},
	}
	if err := ValidateResult(descriptor, request, result); err != nil {
		t.Fatal(err)
	}
	result.Effects[0].RequiredCapabilityRefs = []string{"admin.root"}
	if err := ValidateResult(descriptor, request, result); err == nil {
		t.Fatal("ungranted effect capability was accepted")
	}
	result.Effects[0].RequiredCapabilityRefs = []string{"mcp.invoke"}
	result.Effects[0].Kind = "xgc.shell-root/v1"
	if err := ValidateResult(descriptor, request, result); err == nil {
		t.Fatal("undeclared effect kind was accepted")
	}
}

func TestBadOutputDigestAndAmbientGrantAreRejected(t *testing.T) {
	descriptor := pureDescriptor(t)
	request := requestFor(t, descriptor)
	request.CapabilityGrants = []contracts.CapabilityGrant{{
		CapabilityRef: "filesystem.write", Scope: "root", HandleRef: "grant-root", AuthorizationDigest: digest, ExpiresAt: request.Deadline,
	}}
	if err := ValidateRequest(descriptor, request); err == nil {
		t.Fatal("undeclared ambient grant was accepted")
	}
	request.CapabilityGrants = nil
	result := contracts.NodeResult{
		Status: contracts.NodeResultSucceeded, Output: map[string]any{"message": "hello"},
		OutputDigest: digest, EvidenceDigest: digest,
	}
	if err := ValidateResult(descriptor, request, result); err == nil {
		t.Fatal("bad structured output digest was accepted")
	}
}

func pureDescriptor(t *testing.T) contracts.NodeDescriptor {
	t.Helper()
	descriptor := contracts.NodeDescriptor{
		SchemaVersion: DescriptorSchemaVersion, TypeRef: "xgc.test-echo/v1", DisplayName: "Test echo",
		PackageRef: "xgc2-nodes-test", PackageDigest: digest,
		InputSchema:  contracts.Schema{Type: contracts.TypeObject, Properties: map[string]contracts.Schema{"message": {Type: contracts.TypeString}}, Required: []string{"message"}},
		OutputSchema: contracts.Schema{Type: contracts.TypeObject, Properties: map[string]contracts.Schema{"message": {Type: contracts.TypeString}}, Required: []string{"message"}},
		Mode:         contracts.NodePure, Determinism: contracts.NodeDeterministic, MaxInputBytes: 1024, MaxOutputBytes: 1024,
	}
	descriptor.DescriptorDigest, _ = DescriptorDigest(descriptor)
	if err := ValidateDescriptor(descriptor); err != nil {
		t.Fatal(err)
	}
	return descriptor
}

func requestFor(t *testing.T, descriptor contracts.NodeDescriptor) contracts.NodeInvocationRequest {
	t.Helper()
	input := map[string]any{"message": "hello"}
	inputDigest, err := canonicaljson.DigestValue(input)
	if err != nil {
		t.Fatal(err)
	}
	t0 := time.Date(2026, 8, 13, 1, 0, 0, 0, time.UTC)
	return contracts.NodeInvocationRequest{
		InvocationID: "invocation-1", RunID: "run-1", NodeID: "echo", TypeRef: descriptor.TypeRef,
		DescriptorDigest: descriptor.DescriptorDigest, AttemptID: "attempt-1", AttemptOrdinal: 1,
		Input: input, InputDigest: inputDigest, RequestedAt: t0, Deadline: t0.Add(time.Minute),
	}
}
