// Package action implements the single product-neutral admission path from a
// normalized trigger and candidate object to immutable Action inputs.
package action

import (
	"errors"
	"fmt"
	"sort"
	"strconv"
	"strings"

	"github.com/lxk36/xgc2-execution-platform/kernel/canonicaljson"
	"github.com/lxk36/xgc2-execution-platform/sdk/go/contracts"
)

type Request struct {
	Action          contracts.ActionVersion
	Trigger         contracts.TriggerEvent
	Preset          *contracts.ActionPresetVersion
	Candidate       map[string]any
	CandidateOrigin contracts.InputOriginKind
	CandidateRef    string
	MappingDigest   string
}

type Admission struct {
	ActionRef       contracts.ActionRef
	Trigger         contracts.TriggerEvent
	TriggerDigest   string
	Inputs          map[string]any
	CanonicalInputs []byte
	InputDigest     string
	PresetRef       string
	PresetDigest    string
	FieldProvenance []contracts.InputFieldProvenance
}

func Admit(request Request) (Admission, error) {
	if err := validateAction(request.Action); err != nil {
		return Admission{}, err
	}
	if err := validateTrigger(request.Trigger); err != nil {
		return Admission{}, err
	}
	if !accepts(request.Action.AcceptedTriggerKinds, request.Trigger.Kind) {
		return Admission{}, fmt.Errorf("action does not accept trigger kind %q", request.Trigger.Kind)
	}
	expectedOrigin, err := expectedOrigin(request.Trigger.Kind)
	if err != nil {
		return Admission{}, err
	}
	if request.CandidateOrigin != expectedOrigin {
		return Admission{}, fmt.Errorf("trigger %q requires candidate origin %q, got %q", request.Trigger.Kind, expectedOrigin, request.CandidateOrigin)
	}
	if request.Trigger.Kind == contracts.TriggerPanel && request.Preset == nil {
		return Admission{}, errors.New("panel trigger requires an exact preset")
	}

	inputs, defaultPaths, err := request.Action.InputSchema.ApplyDefaults(map[string]any{})
	if err != nil {
		return Admission{}, fmt.Errorf("apply action defaults: %w", err)
	}
	provenance := make(map[string]contracts.InputFieldProvenance)
	for pointer := range defaultPaths {
		provenance[pointer] = contracts.InputFieldProvenance{
			TargetPointer: pointer,
			OriginKind:    contracts.OriginSchemaDefault,
			SourceRef:     request.Action.ActionID + "@" + request.Action.Version + "#inputSchema",
			SourceDigest:  request.Action.DefinitionDigest,
		}
	}

	presetRef, presetDigest := "", ""
	if request.Preset != nil {
		if err := validatePreset(*request.Preset, request.Action.Ref()); err != nil {
			return Admission{}, err
		}
		mergeObject(inputs, request.Preset.Values)
		presetRef = request.Preset.PresetID + "@" + request.Preset.Version
		presetDigest = request.Preset.Digest
		for pointer := range leafPointers(request.Preset.Values) {
			provenance[pointer] = contracts.InputFieldProvenance{
				TargetPointer: pointer,
				OriginKind:    contracts.OriginPreset,
				SourceRef:     presetRef,
				SourcePointer: pointer,
				SourceDigest:  request.Preset.Digest,
			}
		}
		if err := validateOverrides(request.Candidate, request.Preset.OverridablePaths); err != nil {
			return Admission{}, err
		}
	}

	mergeObject(inputs, request.Candidate)
	for pointer := range leafPointers(request.Candidate) {
		provenance[pointer] = contracts.InputFieldProvenance{
			TargetPointer: pointer,
			OriginKind:    request.CandidateOrigin,
			SourceRef:     request.CandidateRef,
			SourcePointer: pointer,
			MappingDigest: request.MappingDigest,
		}
	}
	if err := request.Action.InputSchema.ValidateValue(inputs); err != nil {
		return Admission{}, fmt.Errorf("action inputs: %w", err)
	}
	canonicalInputs, err := canonicaljson.Marshal(inputs)
	if err != nil {
		return Admission{}, fmt.Errorf("canonicalize action inputs: %w", err)
	}
	limit := request.Action.InputSizeLimit
	if limit == 0 {
		limit = canonicaljson.DefaultMaxInputBytes
	}
	if len(canonicalInputs) > limit {
		return Admission{}, fmt.Errorf("action inputs exceed limit %d", limit)
	}
	inputDigest, err := canonicaljson.Digest(canonicalInputs)
	if err != nil {
		return Admission{}, err
	}
	triggerDigest, err := canonicaljson.DigestValue(request.Trigger)
	if err != nil {
		return Admission{}, fmt.Errorf("canonicalize trigger: %w", err)
	}
	orderedProvenance := make([]contracts.InputFieldProvenance, 0, len(provenance))
	for _, item := range provenance {
		orderedProvenance = append(orderedProvenance, item)
	}
	sort.Slice(orderedProvenance, func(left, right int) bool {
		return orderedProvenance[left].TargetPointer < orderedProvenance[right].TargetPointer
	})
	return Admission{
		ActionRef:       request.Action.Ref(),
		Trigger:         request.Trigger,
		TriggerDigest:   triggerDigest,
		Inputs:          inputs,
		CanonicalInputs: canonicalInputs,
		InputDigest:     inputDigest,
		PresetRef:       presetRef,
		PresetDigest:    presetDigest,
		FieldProvenance: orderedProvenance,
	}, nil
}

func validateAction(action contracts.ActionVersion) error {
	if !contracts.ValidIdentifier(action.ActionID) || !contracts.ValidIdentifier(action.Version) ||
		!contracts.ValidIdentifier(action.Entrypoint) {
		return errors.New("action identity, version, and entrypoint must be portable identifiers")
	}
	if !contracts.ValidDigest(action.DefinitionDigest) {
		return errors.New("action definition digest is invalid")
	}
	if action.InputSchema.Type != contracts.TypeObject {
		return errors.New("action input schema must be an object")
	}
	if err := action.InputSchema.ValidateDefinition(); err != nil {
		return fmt.Errorf("action input schema: %w", err)
	}
	if err := action.ResultSchema.ValidateDefinition(); err != nil {
		return fmt.Errorf("action result schema: %w", err)
	}
	if len(action.AcceptedTriggerKinds) == 0 {
		return errors.New("action must accept at least one trigger kind")
	}
	seen := make(map[contracts.TriggerKind]struct{})
	for _, kind := range action.AcceptedTriggerKinds {
		if !kind.Valid() {
			return fmt.Errorf("unsupported accepted trigger kind %q", kind)
		}
		if _, duplicate := seen[kind]; duplicate {
			return fmt.Errorf("accepted trigger kind %q is duplicated", kind)
		}
		seen[kind] = struct{}{}
	}
	for _, capability := range action.RequiredCapabilities {
		if !contracts.ValidIdentifier(capability) {
			return fmt.Errorf("invalid required capability %q", capability)
		}
	}
	if action.InputSizeLimit < 0 {
		return errors.New("action input size limit cannot be negative")
	}
	return nil
}

func validateTrigger(trigger contracts.TriggerEvent) error {
	if !contracts.ValidIdentifier(trigger.EventID) || !trigger.Kind.Valid() || !contracts.ValidIdentifier(trigger.Version) {
		return errors.New("trigger event identity, kind, or version is invalid")
	}
	if trigger.OccurredAt.IsZero() || trigger.ReceivedAt.IsZero() {
		return errors.New("trigger timestamps are required")
	}
	if trigger.ReceivedAt.Before(trigger.OccurredAt) {
		return errors.New("trigger receivedAt precedes occurredAt")
	}
	if !contracts.ValidIdentifier(trigger.SourceRef) || !contracts.ValidIdentifier(trigger.ActorRef) {
		return errors.New("trigger sourceRef and actorRef are required portable identifiers")
	}
	if trigger.SubjectRef != "" && !contracts.ValidIdentifier(trigger.SubjectRef) {
		return errors.New("trigger subjectRef is invalid")
	}
	if !contracts.ValidDigest(trigger.PayloadSchemaDigest) {
		return errors.New("trigger payload schema digest is invalid")
	}
	if trigger.Payload == nil {
		return errors.New("trigger payload must be an object")
	}
	if _, err := canonicaljson.Marshal(trigger.Payload); err != nil {
		return fmt.Errorf("trigger payload: %w", err)
	}
	return nil
}

func validatePreset(preset contracts.ActionPresetVersion, action contracts.ActionRef) error {
	if !contracts.ValidIdentifier(preset.PresetID) || !contracts.ValidIdentifier(preset.Version) ||
		!contracts.ValidDigest(preset.Digest) {
		return errors.New("preset identity, version, or digest is invalid")
	}
	if !preset.ActionRef.Equal(action) {
		return errors.New("preset does not target the exact action version")
	}
	if preset.Values == nil {
		return errors.New("preset values must be an object")
	}
	seen := make(map[string]struct{}, len(preset.OverridablePaths))
	for _, pointer := range preset.OverridablePaths {
		if err := validatePointer(pointer); err != nil {
			return fmt.Errorf("overridable path %q: %w", pointer, err)
		}
		if _, duplicate := seen[pointer]; duplicate {
			return fmt.Errorf("overridable path %q is duplicated", pointer)
		}
		seen[pointer] = struct{}{}
	}
	return nil
}

func expectedOrigin(kind contracts.TriggerKind) (contracts.InputOriginKind, error) {
	switch kind {
	case contracts.TriggerManual, contracts.TriggerAPI, contracts.TriggerPanel:
		return contracts.OriginCaller, nil
	case contracts.TriggerSchedule, contracts.TriggerWebhook:
		return contracts.OriginTriggerMap, nil
	case contracts.TriggerXGC2Experiment:
		return contracts.OriginExperimentBuilder, nil
	case contracts.TriggerActionCall:
		return contracts.OriginParentMap, nil
	default:
		return "", fmt.Errorf("unsupported trigger kind %q", kind)
	}
}

func accepts(accepted []contracts.TriggerKind, actual contracts.TriggerKind) bool {
	for _, kind := range accepted {
		if kind == actual {
			return true
		}
	}
	return false
}

func mergeObject(target, overlay map[string]any) {
	for key, value := range overlay {
		if object, ok := value.(map[string]any); ok {
			existing, exists := target[key].(map[string]any)
			if !exists {
				existing = make(map[string]any)
				target[key] = existing
			}
			mergeObject(existing, object)
			continue
		}
		target[key] = value
	}
}

func validateOverrides(candidate map[string]any, allowed []string) error {
	allowedSet := make(map[string]struct{}, len(allowed))
	for _, pointer := range allowed {
		allowedSet[pointer] = struct{}{}
	}
	for pointer := range leafPointers(candidate) {
		if _, exists := allowedSet[pointer]; !exists {
			return fmt.Errorf("candidate path %q is not overridable by preset", pointer)
		}
	}
	return nil
}

func leafPointers(value map[string]any) map[string]struct{} {
	result := make(map[string]struct{})
	for key, child := range value {
		markLeafPointers(child, "/"+escapePointer(key), result)
	}
	return result
}

func markLeafPointers(value any, pointer string, target map[string]struct{}) {
	switch typed := value.(type) {
	case map[string]any:
		if len(typed) == 0 {
			target[pointer] = struct{}{}
		}
		for key, child := range typed {
			markLeafPointers(child, pointer+"/"+escapePointer(key), target)
		}
	default:
		target[pointer] = struct{}{}
	}
}

func validatePointer(pointer string) error {
	if pointer == "" || pointer == "/" || !strings.HasPrefix(pointer, "/") {
		return errors.New("must address a non-root JSON Pointer")
	}
	for _, segment := range strings.Split(pointer[1:], "/") {
		if segment == "" {
			return errors.New("contains an empty segment")
		}
		for index := 0; index < len(segment); index++ {
			if segment[index] == '~' {
				if index+1 == len(segment) || (segment[index+1] != '0' && segment[index+1] != '1') {
					return errors.New("contains an invalid escape")
				}
				index++
			}
		}
	}
	return nil
}

func escapePointer(value string) string {
	return strings.ReplaceAll(strings.ReplaceAll(value, "~", "~0"), "/", "~1")
}

func pointerForIndex(base string, index int) string {
	return base + "/" + strconv.Itoa(index)
}
