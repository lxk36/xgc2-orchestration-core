package workflow

import (
	"encoding/json"
	"strings"
	"testing"

	"github.com/lxk36/xgc2-execution-platform/sdk/go/contracts"
)

const workflowDigest = "sha256:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"

func literal(raw string) *json.RawMessage {
	message := json.RawMessage(raw)
	return &message
}

func object(properties map[string]contracts.Schema, required ...string) contracts.Schema {
	return contracts.Schema{Type: contracts.TypeObject, Properties: properties, Required: required}
}

func baseDefinition() contracts.WorkflowDefinition {
	return contracts.WorkflowDefinition{
		SchemaVersion: SchemaVersion,
		WorkflowID:    "reference.workflow",
		Version:       "v1",
		InputSchema:   object(map[string]contracts.Schema{"name": {Type: contracts.TypeString}}, "name"),
		ResultSchema:  object(nil),
		TriggerSchema: object(map[string]contracts.Schema{"eventId": {Type: contracts.TypeString}}),
		ScopeSchema:   object(map[string]contracts.Schema{"runId": {Type: contracts.TypeString}}),
		Entrypoints:   map[string]string{"main": "prepare"},
		Nodes: []contracts.WorkflowNodeDefinition{
			{
				NodeID: "prepare", TypeRef: "xgc.node.transform/v1", DescriptorDigest: workflowDigest,
				InputSchema:  object(map[string]contracts.Schema{"name": {Type: contracts.TypeString}}, "name"),
				OutputSchema: object(map[string]contracts.Schema{"greeting": {Type: contracts.TypeString}}, "greeting"),
				Bindings:     []contracts.ValueBinding{{Target: "/name", Value: contracts.ValueExpr{Ref: "inputs.name"}}},
			},
			{
				NodeID: "render", TypeRef: "xgc.node.transform/v1", DescriptorDigest: workflowDigest,
				InputSchema:  object(map[string]contracts.Schema{"message": {Type: contracts.TypeString}}, "message"),
				OutputSchema: object(map[string]contracts.Schema{"text": {Type: contracts.TypeString}}, "text"),
				Bindings:     []contracts.ValueBinding{{Target: "/message", Value: contracts.ValueExpr{Ref: "nodes.prepare.output.greeting"}}},
			},
		},
		Edges: []contracts.WorkflowEdge{{From: "prepare", To: "render", Kind: contracts.EdgeData}},
	}
}

func TestCompileReferenceWorkflowIsDeterministic(t *testing.T) {
	definition := baseDefinition()
	left, err := Compile(definition)
	if err != nil {
		t.Fatal(err)
	}
	right, err := Compile(definition)
	if err != nil {
		t.Fatal(err)
	}
	if left.PlanDigest != right.PlanDigest || left.DefinitionDigest != right.DefinitionDigest {
		t.Fatalf("plans differ: %#v %#v", left, right)
	}
	if strings.Join(left.NodeOrder, ",") != "prepare,render" {
		t.Fatalf("order = %#v", left.NodeOrder)
	}
}

func TestCompileRejectsCycleDanglingAndUnreachableNodes(t *testing.T) {
	t.Run("cycle", func(t *testing.T) {
		definition := baseDefinition()
		definition.Edges = append(definition.Edges, contracts.WorkflowEdge{From: "render", To: "prepare", Kind: contracts.EdgeControl})
		if _, err := Compile(definition); err == nil || !strings.Contains(err.Error(), "cycle") {
			t.Fatalf("cycle error = %v", err)
		}
	})
	t.Run("dangling", func(t *testing.T) {
		definition := baseDefinition()
		definition.Edges[0].To = "missing"
		if _, err := Compile(definition); err == nil || !strings.Contains(err.Error(), "does not exist") {
			t.Fatalf("dangling error = %v", err)
		}
	})
	t.Run("unreachable", func(t *testing.T) {
		definition := baseDefinition()
		definition.Nodes = append(definition.Nodes, contracts.WorkflowNodeDefinition{NodeID: "orphan", TypeRef: "xgc.node.noop/v1", DescriptorDigest: workflowDigest, InputSchema: object(nil), OutputSchema: object(nil)})
		if _, err := Compile(definition); err == nil || !strings.Contains(err.Error(), "unreachable") {
			t.Fatalf("unreachable error = %v", err)
		}
	})
}

func TestCompileRejectsNonDominatingNodeOutput(t *testing.T) {
	definition := baseDefinition()
	definition.Nodes = []contracts.WorkflowNodeDefinition{
		{NodeID: "start", TypeRef: "xgc.node.noop/v1", DescriptorDigest: workflowDigest, InputSchema: object(nil), OutputSchema: object(nil)},
		{NodeID: "left", TypeRef: "xgc.node.transform/v1", DescriptorDigest: workflowDigest, InputSchema: object(nil), OutputSchema: object(map[string]contracts.Schema{"value": {Type: contracts.TypeString}}, "value")},
		{NodeID: "right", TypeRef: "xgc.node.noop/v1", DescriptorDigest: workflowDigest, InputSchema: object(nil), OutputSchema: object(nil)},
		{NodeID: "join", TypeRef: "xgc.node.transform/v1", DescriptorDigest: workflowDigest, InputSchema: object(map[string]contracts.Schema{"value": {Type: contracts.TypeString}}, "value"), OutputSchema: object(nil), Bindings: []contracts.ValueBinding{{Target: "/value", Value: contracts.ValueExpr{Ref: "nodes.left.output.value"}}}},
	}
	definition.Entrypoints = map[string]string{"main": "start"}
	definition.Edges = []contracts.WorkflowEdge{
		{From: "start", To: "left", Kind: contracts.EdgeControl},
		{From: "start", To: "right", Kind: contracts.EdgeControl},
		{From: "left", To: "join", Kind: contracts.EdgeData},
		{From: "right", To: "join", Kind: contracts.EdgeControl},
	}
	if _, err := Compile(definition); err == nil || !strings.Contains(err.Error(), "not visible") {
		t.Fatalf("dominance error = %v", err)
	}
}

func TestCompileRejectsMissingChildInputAndAcceptsExplicitMap(t *testing.T) {
	definition := baseDefinition()
	child := contracts.CallAction{
		TargetActionRef: contracts.ActionRef{ActionID: "child.action", Version: "v1", Digest: workflowDigest},
		InputSchema:     object(map[string]contracts.Schema{"message": {Type: contracts.TypeString}}, "message"),
		ResultSchema:    object(map[string]contracts.Schema{"text": {Type: contracts.TypeString}}, "text"),
		ResultMap:       []contracts.ResultBinding{{Source: "/text", Target: "/text"}},
	}
	definition.Nodes[1].CallAction = &child
	if _, err := Compile(definition); err == nil || !strings.Contains(err.Error(), "required input") {
		t.Fatalf("missing map error = %v", err)
	}
	child.InputMap = []contracts.ValueBinding{{Target: "/message", Value: contracts.ValueExpr{Ref: "nodes.prepare.output.greeting"}}}
	definition.Nodes[1].CallAction = &child
	if _, err := Compile(definition); err != nil {
		t.Fatal(err)
	}
}

func TestCompileRejectsDuplicateBindingAndMissingRequired(t *testing.T) {
	definition := baseDefinition()
	definition.Nodes[0].Bindings = append(definition.Nodes[0].Bindings, contracts.ValueBinding{Target: "/name", Value: contracts.ValueExpr{Literal: literal(`"duplicate"`)}})
	if _, err := Compile(definition); err == nil || !strings.Contains(err.Error(), "more than once") {
		t.Fatalf("duplicate error = %v", err)
	}
	definition = baseDefinition()
	definition.Nodes[0].Bindings = nil
	if _, err := Compile(definition); err == nil || !strings.Contains(err.Error(), "required input") {
		t.Fatalf("required error = %v", err)
	}
}

func TestCompileAllowsOnlyDirectSecretBinding(t *testing.T) {
	definition := baseDefinition()
	definition.Secrets = []string{"token"}
	definition.Nodes[0].InputSchema.Properties["credential"] = contracts.Schema{Type: contracts.TypeString, Format: contracts.FormatSecretHandle}
	definition.Nodes[0].InputSchema.Required = append(definition.Nodes[0].InputSchema.Required, "credential")
	definition.Nodes[0].Bindings = append(definition.Nodes[0].Bindings, contracts.ValueBinding{Target: "/credential", Value: contracts.ValueExpr{Ref: "secrets.token"}})
	if _, err := Compile(definition); err != nil {
		t.Fatal(err)
	}
	definition.Nodes[0].Bindings[len(definition.Nodes[0].Bindings)-1].Value = contracts.ValueExpr{Op: "concat", Args: []contracts.ValueExpr{
		{Ref: "secrets.token"}, {Literal: literal(`"suffix"`)},
	}}
	if _, err := Compile(definition); err == nil || !strings.Contains(err.Error(), "secret handle") {
		t.Fatalf("secret interpolation error = %v", err)
	}
	definition.Nodes[0].Bindings = definition.Nodes[0].Bindings[:1]
	definition.Nodes[0].FixedInputs = map[string]any{"credential": "raw-value"}
	if _, err := Compile(definition); err == nil || !strings.Contains(err.Error(), "secret slot") {
		t.Fatalf("fixed secret error = %v", err)
	}
	definition = baseDefinition()
	definition.Nodes[0].OutputSchema.Properties["credential"] = contracts.Schema{Type: contracts.TypeString, Format: contracts.FormatSecretHandle}
	if _, err := Compile(definition); err == nil || !strings.Contains(err.Error(), "output schema cannot expose") {
		t.Fatalf("secret output error = %v", err)
	}
}
