// Package node validates the product-neutral extension protocol. It turns a
// node pack into an untrusted declarative module: inputs/outputs are exact and
// side effects are only proposals for the Effect kernel.
package node

import (
	"errors"
	"fmt"
	"sort"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/lxk36/xgc2-orchestration-core/kernel/canonicaljson"
	"github.com/lxk36/xgc2-orchestration-core/sdk/go/contracts"
)

const DescriptorSchemaVersion = "xgc.node-descriptor/v1"

func DescriptorDigest(descriptor contracts.NodeDescriptor) (string, error) {
	descriptor.DescriptorDigest = ""
	return canonicaljson.DigestValue(descriptor)
}

func ValidateDescriptor(descriptor contracts.NodeDescriptor) error {
	if descriptor.SchemaVersion != DescriptorSchemaVersion || !contracts.ValidTypeRef(descriptor.TypeRef) ||
		!contracts.ValidIdentifier(descriptor.PackageRef) || !contracts.ValidDigest(descriptor.PackageDigest) ||
		!contracts.ValidDigest(descriptor.DescriptorDigest) {
		return errors.New("node descriptor schema, type, package, or digest is invalid")
	}
	if !utf8.ValidString(descriptor.DisplayName) || strings.TrimSpace(descriptor.DisplayName) != descriptor.DisplayName ||
		descriptor.DisplayName == "" || len(descriptor.DisplayName) > 160 {
		return errors.New("node display name is invalid")
	}
	if err := descriptor.InputSchema.ValidateDefinition(); err != nil {
		return fmt.Errorf("node input schema: %w", err)
	}
	if err := descriptor.OutputSchema.ValidateDefinition(); err != nil {
		return fmt.Errorf("node output schema: %w", err)
	}
	if descriptor.InputSchema.Type != contracts.TypeObject || descriptor.OutputSchema.Type != contracts.TypeObject {
		return errors.New("node input and output schemas must be objects")
	}
	if !descriptor.Mode.Valid() || !descriptor.Determinism.Valid() || descriptor.MaxInputBytes <= 0 ||
		descriptor.MaxInputBytes > canonicaljson.DefaultMaxInputBytes || descriptor.MaxOutputBytes <= 0 ||
		descriptor.MaxOutputBytes > canonicaljson.DefaultMaxCanonicalBytes {
		return errors.New("node mode, determinism, or size bounds are invalid")
	}
	if err := validateRequirements(descriptor.RequiredCapabilities); err != nil {
		return err
	}
	if !sort.StringsAreSorted(descriptor.AllowedEffectKinds) {
		return errors.New("node allowed effect kinds must be sorted")
	}
	for index, kind := range descriptor.AllowedEffectKinds {
		if !contracts.ValidTypeRef(kind) || (index > 0 && descriptor.AllowedEffectKinds[index-1] == kind) {
			return errors.New("node allowed effect kinds are invalid or duplicated")
		}
	}
	if descriptor.Mode == contracts.NodePure && (len(descriptor.AllowedEffectKinds) != 0 || descriptor.CompensationTypeRef != "") {
		return errors.New("pure node cannot declare effects or compensation")
	}
	if descriptor.Mode == contracts.NodeEffectful && len(descriptor.AllowedEffectKinds) == 0 {
		return errors.New("effectful node requires at least one allowed effect kind")
	}
	if descriptor.Mode == contracts.NodeWaiting && len(descriptor.AllowedEffectKinds) != 0 {
		return errors.New("waiting node cannot declare effects")
	}
	if descriptor.CompensationTypeRef != "" && !contracts.ValidTypeRef(descriptor.CompensationTypeRef) {
		return errors.New("node compensation type ref is invalid")
	}
	expected, err := DescriptorDigest(descriptor)
	if err != nil || expected != descriptor.DescriptorDigest {
		return errors.New("node descriptor content digest is invalid")
	}
	return nil
}

func ValidateRequest(descriptor contracts.NodeDescriptor, request contracts.NodeInvocationRequest) error {
	if err := ValidateDescriptor(descriptor); err != nil {
		return err
	}
	for label, value := range map[string]string{
		"invocation id": request.InvocationID, "run id": request.RunID, "node id": request.NodeID, "attempt id": request.AttemptID,
	} {
		if !contracts.ValidIdentifier(value) {
			return fmt.Errorf("node request %s is invalid", label)
		}
	}
	if request.TypeRef != descriptor.TypeRef || request.DescriptorDigest != descriptor.DescriptorDigest || request.AttemptOrdinal == 0 ||
		request.RequestedAt.IsZero() || !request.Deadline.After(request.RequestedAt) {
		return errors.New("node request descriptor, ordinal, or times are invalid")
	}
	canonical, err := canonicaljson.Marshal(request.Input)
	if err != nil || len(canonical) > descriptor.MaxInputBytes {
		return errors.New("node request input is invalid or exceeds descriptor bound")
	}
	digest, err := canonicaljson.Digest(canonical)
	if err != nil || digest != request.InputDigest {
		return errors.New("node request input digest is invalid")
	}
	if err := descriptor.InputSchema.ValidateValue(request.Input); err != nil {
		return fmt.Errorf("node request input schema: %w", err)
	}
	return validateGrants(descriptor.RequiredCapabilities, request.CapabilityGrants, request.RequestedAt, request.Deadline)
}

func ValidateResult(descriptor contracts.NodeDescriptor, request contracts.NodeInvocationRequest, result contracts.NodeResult) error {
	if err := ValidateRequest(descriptor, request); err != nil {
		return err
	}
	if !result.Status.Valid() || !contracts.ValidDigest(result.EvidenceDigest) {
		return errors.New("node result status or evidence digest is invalid")
	}
	if descriptor.Mode != contracts.NodeEffectful && len(result.Effects) != 0 {
		return errors.New("non-effectful node proposed an effect")
	}
	if err := validateEffects(descriptor, request, result.Effects); err != nil {
		return err
	}
	switch result.Status {
	case contracts.NodeResultSucceeded:
		if result.Output == nil || result.Wait != nil || result.Failure != nil || len(result.Effects) != 0 {
			return errors.New("successful node result requires output and no wait, failure, or effect")
		}
		canonical, err := canonicaljson.Marshal(result.Output)
		if err != nil || len(canonical) > descriptor.MaxOutputBytes {
			return errors.New("node result output is invalid or exceeds descriptor bound")
		}
		digest, err := canonicaljson.Digest(canonical)
		if err != nil || digest != result.OutputDigest {
			return errors.New("node result output digest is invalid")
		}
		if err := descriptor.OutputSchema.ValidateValue(result.Output); err != nil {
			return fmt.Errorf("node result output schema: %w", err)
		}
		if result.OutputArtifactRef != "" && !contracts.ValidIdentifier(result.OutputArtifactRef) {
			return errors.New("node output artifact ref is invalid")
		}
	case contracts.NodeResultWaiting:
		if result.Wait == nil || result.Output != nil || result.OutputDigest != "" || result.OutputArtifactRef != "" || result.Failure != nil {
			return errors.New("waiting node result has invalid output, wait, or failure")
		}
		if err := validateWait(*result.Wait, request); err != nil {
			return err
		}
		if result.Wait.Kind == contracts.NodeWaitEffect {
			if len(result.Effects) != 1 || result.Effects[0].EffectKey != result.Wait.SubjectRef ||
				result.Effects[0].IntentDigest != result.Wait.ConditionDigest {
				return errors.New("effect wait requires exactly one matching effect proposal")
			}
		} else if len(result.Effects) != 0 {
			return errors.New("non-effect wait cannot propose an effect")
		}
	case contracts.NodeResultFailed:
		if result.Failure == nil || result.Output != nil || result.OutputDigest != "" || result.OutputArtifactRef != "" || result.Wait != nil || len(result.Effects) != 0 {
			return errors.New("failed node result requires only a structured failure")
		}
		if err := validateFailure(*result.Failure); err != nil {
			return err
		}
	}
	return nil
}

func ValidateResumeRequest(descriptor contracts.NodeDescriptor, request contracts.NodeResumeRequest) error {
	for label, value := range map[string]string{
		"invocation id": request.InvocationID, "run id": request.RunID, "node id": request.NodeID,
		"attempt id": request.AttemptID,
	} {
		if !contracts.ValidIdentifier(value) {
			return fmt.Errorf("resume %s is invalid", label)
		}
	}
	if request.TypeRef != descriptor.TypeRef || request.DescriptorDigest != descriptor.DescriptorDigest || request.AttemptOrdinal == 0 || request.RequestedAt.IsZero() {
		return errors.New("node resume descriptor, ordinal, or time is invalid")
	}
	canonical, err := canonicaljson.Marshal(request.Input)
	if err != nil || len(canonical) > descriptor.MaxInputBytes {
		return errors.New("node resume input is invalid or exceeds descriptor bound")
	}
	digest, err := canonicaljson.Digest(canonical)
	if err != nil || digest != request.InputDigest {
		return errors.New("node resume input digest is invalid")
	}
	if err := descriptor.InputSchema.ValidateValue(request.Input); err != nil {
		return fmt.Errorf("node resume input schema: %w", err)
	}
	resolution := request.Resolution
	if !request.Wait.Kind.Valid() || !resolution.Kind.Valid() || request.Wait.Kind != resolution.Kind ||
		request.Wait.SubjectRef != resolution.SubjectRef || request.Wait.ConditionDigest != resolution.ConditionDigest ||
		!contracts.ValidIdentifier(resolution.SubjectRef) || !contracts.ValidDigest(resolution.ConditionDigest) ||
		!resolution.Status.Valid() || resolution.ObservedAt.IsZero() {
		return errors.New("node resume wait resolution does not match its durable wait")
	}
	if resolution.PayloadArtifactRef != "" && !contracts.ValidIdentifier(resolution.PayloadArtifactRef) {
		return errors.New("node resume payload artifact ref is invalid")
	}
	switch resolution.Status {
	case contracts.NodeWaitResolvedSucceeded:
		if resolution.Payload == nil || resolution.Failure != nil {
			return errors.New("successful wait resolution requires payload and no failure")
		}
		payloadDigest, err := canonicaljson.DigestValue(resolution.Payload)
		if err != nil || payloadDigest != resolution.PayloadDigest {
			return errors.New("wait resolution payload digest is invalid")
		}
	case contracts.NodeWaitResolvedFailed:
		if resolution.Payload != nil || resolution.PayloadDigest != "" || resolution.PayloadArtifactRef != "" || resolution.Failure == nil {
			return errors.New("failed wait resolution requires only a failure")
		}
		if !resolution.Failure.Class.Valid() || !contracts.ValidIdentifier(resolution.Failure.Code) || resolution.Failure.Message == "" {
			return errors.New("wait resolution failure is invalid")
		}
	case contracts.NodeWaitResolvedCanceled:
		if resolution.Payload != nil || resolution.PayloadDigest != "" || resolution.PayloadArtifactRef != "" || resolution.Failure != nil {
			return errors.New("canceled wait resolution carries data")
		}
	}
	return nil
}

func ValidateResumeResult(descriptor contracts.NodeDescriptor, result contracts.NodeResult) error {
	if !result.Status.Valid() || !contracts.ValidDigest(result.EvidenceDigest) || len(result.Effects) != 0 || result.Wait != nil {
		return errors.New("resumed node result status, evidence, effects, or wait is invalid")
	}
	switch result.Status {
	case contracts.NodeResultSucceeded:
		if result.Output == nil || result.Failure != nil {
			return errors.New("successful resumed node requires output and no failure")
		}
		canonical, err := canonicaljson.Marshal(result.Output)
		if err != nil || len(canonical) > descriptor.MaxOutputBytes {
			return errors.New("resumed node output is invalid or exceeds descriptor bound")
		}
		digest, err := canonicaljson.Digest(canonical)
		if err != nil || digest != result.OutputDigest {
			return errors.New("resumed node output digest is invalid")
		}
		if err := descriptor.OutputSchema.ValidateValue(result.Output); err != nil {
			return fmt.Errorf("resumed node output schema: %w", err)
		}
		if result.OutputArtifactRef != "" && !contracts.ValidIdentifier(result.OutputArtifactRef) {
			return errors.New("resumed node output artifact ref is invalid")
		}
	case contracts.NodeResultFailed:
		if result.Failure == nil || result.Output != nil || result.OutputDigest != "" || result.OutputArtifactRef != "" {
			return errors.New("failed resumed node requires only a failure")
		}
		if !result.Failure.Class.Valid() || !contracts.ValidIdentifier(result.Failure.Code) || result.Failure.Message == "" {
			return errors.New("resumed node failure is invalid")
		}
	default:
		return errors.New("node resumer must terminate instead of waiting again")
	}
	return nil
}

func validateRequirements(requirements []contracts.CapabilityRequirement) error {
	previous := ""
	capabilities := make(map[string]bool, len(requirements))
	for _, requirement := range requirements {
		key := requirement.CapabilityRef + "\x00" + requirement.Scope
		if !contracts.ValidIdentifier(requirement.CapabilityRef) || !contracts.ValidIdentifier(requirement.Scope) || key <= previous || capabilities[requirement.CapabilityRef] {
			return errors.New("node capability requirements must be valid, unique, and sorted")
		}
		capabilities[requirement.CapabilityRef] = true
		previous = key
	}
	return nil
}

func validateGrants(requirements []contracts.CapabilityRequirement, grants []contracts.CapabilityGrant, requestedAt, deadline time.Time) error {
	required := make(map[string]contracts.CapabilityRequirement, len(requirements))
	for _, requirement := range requirements {
		required[requirement.CapabilityRef+"\x00"+requirement.Scope] = requirement
	}
	seen := make(map[string]bool, len(grants))
	previous := ""
	for _, grant := range grants {
		key := grant.CapabilityRef + "\x00" + grant.Scope
		if key <= previous || !contracts.ValidIdentifier(grant.CapabilityRef) || !contracts.ValidIdentifier(grant.Scope) ||
			!contracts.ValidIdentifier(grant.HandleRef) || !contracts.ValidDigest(grant.AuthorizationDigest) ||
			grant.ExpiresAt.Before(deadline) || grant.ExpiresAt.Before(requestedAt) {
			return errors.New("node capability grants are invalid, expired, duplicated, or unsorted")
		}
		if _, allowed := required[key]; !allowed {
			return errors.New("node request grants an undeclared capability")
		}
		seen[key] = true
		previous = key
	}
	for key, requirement := range required {
		if !requirement.Optional && !seen[key] {
			return fmt.Errorf("node request is missing capability %s", requirement.CapabilityRef)
		}
	}
	return nil
}

func validateEffects(descriptor contracts.NodeDescriptor, request contracts.NodeInvocationRequest, proposals []contracts.EffectProposal) error {
	allowedKinds := make(map[string]bool, len(descriptor.AllowedEffectKinds))
	for _, kind := range descriptor.AllowedEffectKinds {
		allowedKinds[kind] = true
	}
	grants := make(map[string]bool, len(request.CapabilityGrants))
	for _, grant := range request.CapabilityGrants {
		grants[grant.CapabilityRef] = true
	}
	keys := make(map[string]bool, len(proposals))
	for index, proposal := range proposals {
		if !contracts.ValidIdentifier(proposal.EffectKey) || keys[proposal.EffectKey] || !allowedKinds[proposal.Kind] || !contracts.ValidIdentifier(proposal.TargetRef) {
			return fmt.Errorf("node effect proposal %d identity, key, kind, or target is invalid", index)
		}
		keys[proposal.EffectKey] = true
		for _, digest := range []string{proposal.IntentSchemaDigest, proposal.IntentDigest, proposal.PolicyDigest} {
			if !contracts.ValidDigest(digest) {
				return fmt.Errorf("node effect proposal %d digest is invalid", index)
			}
		}
		if proposal.Intent == nil {
			return fmt.Errorf("node effect proposal %d intent is missing", index)
		}
		intentBytes, err := canonicaljson.Marshal(proposal.Intent)
		if err != nil || len(intentBytes) > descriptor.MaxOutputBytes {
			return fmt.Errorf("node effect proposal %d intent is invalid or exceeds descriptor bound", index)
		}
		intentDigest, err := canonicaljson.Digest(intentBytes)
		if err != nil || intentDigest != proposal.IntentDigest {
			return fmt.Errorf("node effect proposal %d intent digest is invalid", index)
		}
		if proposal.IntentArtifactRef != "" && !contracts.ValidIdentifier(proposal.IntentArtifactRef) {
			return fmt.Errorf("node effect proposal %d artifact ref is invalid", index)
		}
		if !proposal.Ownership.Valid() || !proposal.CompensationPolicy.Valid() || proposal.Deadline.After(request.Deadline) || !proposal.Deadline.After(request.RequestedAt) {
			return fmt.Errorf("node effect proposal %d ownership, compensation, or deadline is invalid", index)
		}
		if proposal.Ownership != contracts.EffectOwned && proposal.CompensationPolicy != contracts.CompensationNone {
			return fmt.Errorf("node effect proposal %d lets a non-owner compensate", index)
		}
		if !sort.StringsAreSorted(proposal.RequiredCapabilityRefs) {
			return fmt.Errorf("node effect proposal %d capability refs are unsorted", index)
		}
		for capabilityIndex, capability := range proposal.RequiredCapabilityRefs {
			if !contracts.ValidIdentifier(capability) || !grants[capability] || (capabilityIndex > 0 && proposal.RequiredCapabilityRefs[capabilityIndex-1] == capability) {
				return fmt.Errorf("node effect proposal %d requires an invalid or ungranted capability", index)
			}
		}
	}
	return nil
}

func validateWait(wait contracts.NodeWait, request contracts.NodeInvocationRequest) error {
	if !wait.Kind.Valid() || !contracts.ValidIdentifier(wait.SubjectRef) || !contracts.ValidDigest(wait.ConditionDigest) {
		return errors.New("node wait kind, subject, or condition is invalid")
	}
	if wait.ExpiresAt != nil && (!wait.ExpiresAt.After(request.RequestedAt) || wait.ExpiresAt.After(request.Deadline)) {
		return errors.New("node wait expiry is outside invocation deadline")
	}
	return nil
}

func validateFailure(failure contracts.StructuredFailure) error {
	if !failure.Class.Valid() || !contracts.ValidIdentifier(failure.Code) || failure.Message == "" ||
		len(failure.Message) > 4096 || !utf8.ValidString(failure.Message) || strings.TrimSpace(failure.Message) != failure.Message {
		return errors.New("node structured failure is invalid")
	}
	if failure.EvidenceRef != "" && !contracts.ValidIdentifier(failure.EvidenceRef) {
		return errors.New("node failure evidence ref is invalid")
	}
	return nil
}
