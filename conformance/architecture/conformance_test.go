package architecture

import (
	"bytes"
	"encoding/json"
	"errors"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"testing"

	"github.com/lxk36/xgc2-orchestration-core/kernel/action"
	"github.com/lxk36/xgc2-orchestration-core/kernel/canonicaljson"
	"github.com/lxk36/xgc2-orchestration-core/kernel/expression"
	"github.com/lxk36/xgc2-orchestration-core/kernel/workflow"
	"github.com/lxk36/xgc2-orchestration-core/sdk/go/contracts"
)

const fixtureDigest = "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"

func repositoryRoot(t *testing.T) string {
	t.Helper()
	_, filename, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("cannot locate conformance source")
	}
	return filepath.Clean(filepath.Join(filepath.Dir(filename), "..", ".."))
}

func readFixture(t *testing.T, segments ...string) []byte {
	t.Helper()
	path := filepath.Join(append([]string{repositoryRoot(t)}, segments...)...)
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	return raw
}

func strictDecode(raw []byte, target any) error {
	return canonicaljson.UnmarshalStrict(raw, target)
}

func TestCanonicalGoldenFixtures(t *testing.T) {
	left := readFixture(t, "conformance", "fixtures", "canonical", "semantic-a.json")
	right := readFixture(t, "conformance", "fixtures", "canonical", "semantic-b.json")
	leftDigest, err := canonicaljson.Digest(left)
	if err != nil {
		t.Fatal(err)
	}
	rightDigest, err := canonicaljson.Digest(right)
	if err != nil {
		t.Fatal(err)
	}
	if leftDigest != rightDigest {
		t.Fatalf("semantic fixture digests differ: %s != %s", leftDigest, rightDigest)
	}
	want := strings.TrimSpace(string(readFixture(t, "conformance", "fixtures", "canonical", "expected.sha256")))
	if leftDigest != want {
		t.Fatalf("golden digest = %s, want %s", leftDigest, want)
	}
}

func TestSixIngressFixturesUseOneAdmissionPath(t *testing.T) {
	fixtures := []struct {
		name   string
		origin contracts.InputOriginKind
	}{
		{"manual", contracts.OriginCaller},
		{"schedule", contracts.OriginTriggerMap},
		{"webhook", contracts.OriginTriggerMap},
		{"api", contracts.OriginCaller},
		{"panel", contracts.OriginCaller},
		{"xgc2-experiment", contracts.OriginExperimentBuilder},
	}
	var inputDigest string
	for _, fixture := range fixtures {
		t.Run(fixture.name, func(t *testing.T) {
			var trigger contracts.TriggerEvent
			if err := strictDecode(readFixture(t, "conformance", "fixtures", "trigger", fixture.name+".json"), &trigger); err != nil {
				t.Fatal(err)
			}
			actionVersion := fixtureAction(trigger.Kind)
			var preset *contracts.ActionPresetVersion
			if trigger.Kind == contracts.TriggerPanel {
				preset = &contracts.ActionPresetVersion{
					PresetID: "panel.default", Version: "v1", Digest: fixtureDigest,
					ActionRef: actionVersion.Ref(), Values: map[string]any{"name": "same"}, OverridablePaths: []string{"/name"},
				}
			}
			admitted, err := action.Admit(action.Request{
				Action: actionVersion, Trigger: trigger, Preset: preset,
				Candidate: map[string]any{"name": "same"}, CandidateOrigin: fixture.origin, CandidateRef: fixture.name,
			})
			if err != nil {
				t.Fatal(err)
			}
			if inputDigest == "" {
				inputDigest = admitted.InputDigest
			} else if admitted.InputDigest != inputDigest {
				t.Fatalf("same candidate produced %s, want %s", admitted.InputDigest, inputDigest)
			}
		})
	}
}

func fixtureAction(kind contracts.TriggerKind) contracts.ActionVersion {
	mode := json.RawMessage(`"safe"`)
	return contracts.ActionVersion{
		ActionID: "fixture.action", Version: "v1", DefinitionDigest: fixtureDigest, Entrypoint: "main",
		InputSchema: contracts.Schema{
			Type: contracts.TypeObject,
			Properties: map[string]contracts.Schema{
				"name": {Type: contracts.TypeString},
				"mode": {Type: contracts.TypeString, Default: mode},
			},
			Required: []string{"name", "mode"},
		},
		ResultSchema: contracts.Schema{Type: contracts.TypeObject}, AcceptedTriggerKinds: []contracts.TriggerKind{kind},
	}
}

func TestReferenceDefinitionStrictDecodeAndCompile(t *testing.T) {
	raw := readFixture(t, "examples", "reference-workflow", "workflow.json")
	var definition contracts.WorkflowDefinition
	if err := strictDecode(raw, &definition); err != nil {
		t.Fatal(err)
	}
	plan, err := workflow.Compile(definition)
	if err != nil {
		t.Fatal(err)
	}
	if len(plan.NodeOrder) != 2 || plan.PlanDigest == "" {
		t.Fatalf("compiled plan = %#v", plan)
	}
	var actionVersion contracts.ActionVersion
	if err := strictDecode(readFixture(t, "examples", "reference-workflow", "action.json"), &actionVersion); err != nil {
		t.Fatal(err)
	}
	if actionVersion.DefinitionDigest != plan.DefinitionDigest {
		t.Fatalf("reference action digest = %s, definition digest = %s", actionVersion.DefinitionDigest, plan.DefinitionDigest)
	}
	var trigger contracts.TriggerEvent
	if err := strictDecode(readFixture(t, "conformance", "fixtures", "trigger", "manual.json"), &trigger); err != nil {
		t.Fatal(err)
	}
	admitted, err := action.Admit(action.Request{
		Action: actionVersion, Trigger: trigger, Candidate: map[string]any{"name": "Ada"},
		CandidateOrigin: contracts.OriginCaller, CandidateRef: "reference-request",
	})
	if err != nil {
		t.Fatal(err)
	}
	environment := expression.Environment{
		Inputs: actionVersion.InputSchema, Trigger: definition.TriggerSchema, Scope: definition.ScopeSchema,
		NodeOutputs: map[string]contracts.Schema{"prepare": definition.Nodes[0].OutputSchema},
	}
	prepareInput, err := expression.Evaluate(
		definition.Nodes[0].Bindings[0].Value,
		environment,
		expression.Values{Inputs: admitted.Inputs, Trigger: map[string]any{}, Scope: map[string]any{}},
	)
	if err != nil {
		t.Fatal(err)
	}
	environment.VisibleNodes = map[string]bool{"prepare": true}
	renderInput, err := expression.Evaluate(
		definition.Nodes[1].Bindings[0].Value,
		environment,
		expression.Values{
			Inputs: admitted.Inputs, Trigger: map[string]any{}, Scope: map[string]any{},
			NodeOutputs: map[string]any{"prepare": map[string]any{"greeting": prepareInput}},
		},
	)
	if err != nil {
		t.Fatal(err)
	}
	if renderInput != "Hello, Ada" {
		t.Fatalf("reference binding result = %#v", renderInput)
	}

	withUnknown := bytes.Replace(raw, []byte(`"schemaVersion"`), []byte(`"unknownLegacyField":true,"schemaVersion"`), 1)
	if err := strictDecode(withUnknown, &definition); err == nil || !strings.Contains(err.Error(), "unknown field") {
		t.Fatalf("unknown-field error = %v", err)
	}
}

func TestLegacyAmbientExpressionFailsClosed(t *testing.T) {
	var expr contracts.ValueExpr
	if err := strictDecode(readFixture(t, "conformance", "fixtures", "expression", "legacy-env.invalid.json"), &expr); err != nil {
		t.Fatal(err)
	}
	environment := expression.Environment{
		Inputs: contracts.Schema{Type: contracts.TypeObject}, Trigger: contracts.Schema{Type: contracts.TypeObject},
		Scope: contracts.Schema{Type: contracts.TypeObject},
	}
	if _, err := expression.Check(expr, environment); err == nil || !strings.Contains(err.Error(), "unknown expression namespace") {
		t.Fatalf("legacy expression error = %v", err)
	}
}

func TestPublishedSchemasAreStrictJSON(t *testing.T) {
	paths := [][]string{
		{"spec", "action-input", "v1", "schema.json"},
		{"spec", "action-input", "v1", "preset.schema.json"},
		{"spec", "trigger-event", "v1", "schema.json"},
		{"spec", "value-expression", "v1", "schema.json"},
		{"spec", "workflow-definition", "v1", "schema.json"},
		{"spec", "orchestration-state", "v1", "schema.json"},
		{"spec", "node-protocol", "v1", "descriptor.schema.json"},
		{"spec", "node-protocol", "v1", "invocation.schema.json"},
		{"spec", "node-protocol", "v1", "result.schema.json"},
	}
	for _, segments := range paths {
		if _, err := canonicaljson.Canonicalize(readFixture(t, segments...)); err != nil {
			t.Fatalf("%s: %v", filepath.Join(segments...), err)
		}
	}
}

func TestPublicStateOmitsPrivateDispatchTokens(t *testing.T) {
	value := contracts.CommandEnvelope{
		CommandID: "command-1", EffectID: "effect-1", IdempotencyKey: "raw-idempotency-key",
		CapabilityToken: "raw-capability-token", IdempotencyKeyHash: fixtureDigest,
		CapabilityTokenHash: fixtureDigest,
	}
	raw, err := json.Marshal(value)
	if err != nil {
		t.Fatal(err)
	}
	for _, private := range []string{"raw-idempotency-key", "raw-capability-token", "IdempotencyKey", "CapabilityToken"} {
		if strings.Contains(string(raw), private) {
			t.Fatalf("public command JSON leaked %q: %s", private, raw)
		}
	}
}

func TestKernelImportGate(t *testing.T) {
	root := repositoryRoot(t)
	forbidden := []string{"products/", "core-xgc", "gorm.io", "studio", "controller"}
	err := filepath.WalkDir(filepath.Join(root, "kernel"), func(path string, entry os.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if entry.IsDir() || filepath.Ext(path) != ".go" {
			return nil
		}
		parsed, err := parser.ParseFile(token.NewFileSet(), path, nil, parser.ImportsOnly)
		if err != nil {
			return err
		}
		for _, imported := range parsed.Imports {
			pathValue, err := strconv.Unquote(imported.Path.Value)
			if err != nil {
				return err
			}
			for _, fragment := range forbidden {
				if strings.Contains(strings.ToLower(pathValue), fragment) {
					return errors.New("forbidden kernel import " + pathValue)
				}
			}
			if strings.Contains(pathValue, ".") && !strings.HasPrefix(pathValue, "github.com/lxk36/xgc2-orchestration-core/") {
				return errors.New("external kernel import " + pathValue)
			}
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
}

func TestReferenceTerminologyGate(t *testing.T) {
	root := filepath.Join(repositoryRoot(t), "examples", "reference-workflow")
	forbidden := []string{"experiment", "robot", "agent hub", "agent-hub", "release", "ros"}
	violations := make([]string, 0)
	err := filepath.WalkDir(root, func(path string, entry os.DirEntry, walkErr error) error {
		if walkErr != nil || entry.IsDir() {
			return walkErr
		}
		raw, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		lower := strings.ToLower(string(raw))
		for _, term := range forbidden {
			if strings.Contains(lower, term) {
				violations = append(violations, filepath.Base(path)+":"+term)
			}
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	sort.Strings(violations)
	if len(violations) != 0 {
		t.Fatalf("reference terminology violations: %s", strings.Join(violations, ", "))
	}
}
