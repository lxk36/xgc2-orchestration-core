package action

import (
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/lxk36/xgc2-execution-platform/sdk/go/contracts"
)

const testDigest = "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"

func testAction(kinds ...contracts.TriggerKind) contracts.ActionVersion {
	return contracts.ActionVersion{
		ActionID:         "demo.action",
		Version:          "v1",
		DefinitionDigest: testDigest,
		Entrypoint:       "main",
		InputSchema: contracts.Schema{
			Type: contracts.TypeObject,
			Properties: map[string]contracts.Schema{
				"name": {Type: contracts.TypeString},
				"mode": {Type: contracts.TypeString, Default: json.RawMessage(`"safe"`)},
			},
			Required: []string{"name", "mode"},
		},
		ResultSchema:         contracts.Schema{Type: contracts.TypeObject},
		AcceptedTriggerKinds: kinds,
	}
}

func testTrigger(kind contracts.TriggerKind) contracts.TriggerEvent {
	now := time.Date(2026, 8, 12, 12, 0, 0, 0, time.UTC)
	return contracts.TriggerEvent{
		EventID:             "event-1",
		Kind:                kind,
		Version:             "v1",
		OccurredAt:          now,
		ReceivedAt:          now,
		SourceRef:           "source-1",
		ActorRef:            "actor-1",
		PayloadSchemaDigest: testDigest,
		Payload:             map[string]any{},
	}
}

func TestAdmitAppliesDefaultsPresetOverrideAndProvenance(t *testing.T) {
	action := testAction(contracts.TriggerPanel)
	preset := contracts.ActionPresetVersion{
		PresetID: "panel.default", Version: "v1", Digest: testDigest, ActionRef: action.Ref(),
		Values: map[string]any{"name": "preset"}, OverridablePaths: []string{"/name"},
	}
	admission, err := Admit(Request{
		Action: action, Trigger: testTrigger(contracts.TriggerPanel), Preset: &preset,
		Candidate: map[string]any{"name": "caller"}, CandidateOrigin: contracts.OriginCaller, CandidateRef: "request-1",
	})
	if err != nil {
		t.Fatal(err)
	}
	if got := string(admission.CanonicalInputs); got != `{"mode":"safe","name":"caller"}` {
		t.Fatalf("inputs = %s", got)
	}
	if len(admission.FieldProvenance) != 2 || admission.FieldProvenance[0].OriginKind != contracts.OriginSchemaDefault ||
		admission.FieldProvenance[1].OriginKind != contracts.OriginCaller {
		t.Fatalf("provenance = %#v", admission.FieldProvenance)
	}
}

func TestAllPublicTriggerKindsUseOneAdmissionContract(t *testing.T) {
	cases := []struct {
		kind   contracts.TriggerKind
		origin contracts.InputOriginKind
	}{
		{contracts.TriggerManual, contracts.OriginCaller},
		{contracts.TriggerSchedule, contracts.OriginTriggerMap},
		{contracts.TriggerWebhook, contracts.OriginTriggerMap},
		{contracts.TriggerAPI, contracts.OriginCaller},
		{contracts.TriggerXGC2Experiment, contracts.OriginExperimentBuilder},
	}
	for _, test := range cases {
		t.Run(string(test.kind), func(t *testing.T) {
			admission, err := Admit(Request{Action: testAction(test.kind), Trigger: testTrigger(test.kind), Candidate: map[string]any{"name": "same"}, CandidateOrigin: test.origin, CandidateRef: "source"})
			if err != nil {
				t.Fatal(err)
			}
			if admission.InputDigest == "" {
				t.Fatal("missing digest")
			}
		})
	}
}

func TestAdmitRejectsOriginAndPresetEscape(t *testing.T) {
	_, err := Admit(Request{Action: testAction(contracts.TriggerWebhook), Trigger: testTrigger(contracts.TriggerWebhook), Candidate: map[string]any{"name": "bad"}, CandidateOrigin: contracts.OriginCaller})
	if err == nil || !strings.Contains(err.Error(), "requires candidate origin") {
		t.Fatalf("origin error = %v", err)
	}
	action := testAction(contracts.TriggerPanel)
	preset := contracts.ActionPresetVersion{PresetID: "panel", Version: "v1", Digest: testDigest, ActionRef: action.Ref(), Values: map[string]any{"name": "preset"}}
	_, err = Admit(Request{Action: action, Trigger: testTrigger(contracts.TriggerPanel), Preset: &preset, Candidate: map[string]any{"name": "escape"}, CandidateOrigin: contracts.OriginCaller})
	if err == nil || !strings.Contains(err.Error(), "not overridable") {
		t.Fatalf("override error = %v", err)
	}
}
