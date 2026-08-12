package expression

import (
	"encoding/json"
	"strings"
	"testing"

	"github.com/lxk36/xgc2-execution-platform/sdk/go/contracts"
)

func raw(value string) *json.RawMessage {
	message := json.RawMessage(value)
	return &message
}

func expressionEnvironment() Environment {
	return Environment{
		Inputs: contracts.Schema{Type: contracts.TypeObject, Properties: map[string]contracts.Schema{
			"count": {Type: contracts.TypeInteger},
			"mode":  {Type: contracts.TypeString},
		}},
		Trigger: contracts.Schema{Type: contracts.TypeObject, Properties: map[string]contracts.Schema{"eventId": {Type: contracts.TypeString}}},
		Scope:   contracts.Schema{Type: contracts.TypeObject, Properties: map[string]contracts.Schema{"runId": {Type: contracts.TypeString}}},
		NodeOutputs: map[string]contracts.Schema{
			"prepare": {Type: contracts.TypeObject, Properties: map[string]contracts.Schema{"workspaceRef": {Type: contracts.TypeString}}},
		},
		VisibleNodes: map[string]bool{"prepare": true},
		Secrets:      map[string]bool{"token": true},
	}
}

func TestCheckAndEvaluateDeterministically(t *testing.T) {
	expr := contracts.ValueExpr{Op: "add", Args: []contracts.ValueExpr{
		{Ref: "inputs.count"}, {Literal: raw(`2`)},
	}}
	typed, err := Check(expr, expressionEnvironment())
	if err != nil {
		t.Fatal(err)
	}
	if typed.Type != contracts.TypeInteger {
		t.Fatalf("type = %s", typed.Type)
	}
	value, err := Evaluate(expr, expressionEnvironment(), Values{Inputs: map[string]any{"count": json.Number("3")}})
	if err != nil {
		t.Fatal(err)
	}
	if value != json.Number("5") {
		t.Fatalf("value = %#v", value)
	}
}

func TestCheckRejectsUnknownNamespaceInvisibleNodeAndUnknownOperator(t *testing.T) {
	tests := []contracts.ValueExpr{
		{Ref: "env.PATH"},
		{Ref: "nodes.missing.output.value"},
		{Op: "model-call", Args: []contracts.ValueExpr{{Literal: raw(`1`)}}},
	}
	for _, expr := range tests {
		if _, err := Check(expr, expressionEnvironment()); err == nil {
			t.Fatalf("expected rejection for %#v", expr)
		}
	}
}

func TestSecretMustBeDirectAndTargetASecretSlot(t *testing.T) {
	secret := contracts.ValueExpr{Ref: "secrets.token"}
	if err := CheckAssignable(secret, expressionEnvironment(), contracts.Schema{Type: contracts.TypeString, Format: contracts.FormatSecretHandle}); err != nil {
		t.Fatal(err)
	}
	if err := CheckAssignable(secret, expressionEnvironment(), contracts.Schema{Type: contracts.TypeString}); err == nil || !strings.Contains(err.Error(), "secret handle") {
		t.Fatalf("public target error = %v", err)
	}
	concat := contracts.ValueExpr{Op: "concat", Args: []contracts.ValueExpr{secret, {Literal: raw(`"x"`)}}}
	if _, err := Check(concat, expressionEnvironment()); err == nil || !strings.Contains(err.Error(), "secret handle") {
		t.Fatalf("secret operator error = %v", err)
	}
}

func TestExpressionLimitsAndNonTerminatingDivision(t *testing.T) {
	limits := DefaultLimits()
	limits.MaxNodes = 1
	expr := contracts.ValueExpr{Op: "not", Args: []contracts.ValueExpr{{Literal: raw(`true`)}}}
	if _, err := CheckWithLimits(expr, expressionEnvironment(), limits); err == nil {
		t.Fatal("expected node limit rejection")
	}
	division := contracts.ValueExpr{Op: "div", Args: []contracts.ValueExpr{{Literal: raw(`1`)}, {Literal: raw(`3`)}}}
	if _, err := Evaluate(division, expressionEnvironment(), Values{}); err == nil || !strings.Contains(err.Error(), "finite decimal") {
		t.Fatalf("division error = %v", err)
	}
}

func TestEvaluateRejectsValuesThatViolateCheckedSchemas(t *testing.T) {
	expr := contracts.ValueExpr{Op: "add", Args: []contracts.ValueExpr{
		{Ref: "inputs.count"}, {Literal: raw(`1`)},
	}}
	_, err := Evaluate(expr, expressionEnvironment(), Values{Inputs: map[string]any{"count": "not-an-integer"}})
	if err == nil || !strings.Contains(err.Error(), "inputs values") {
		t.Fatalf("value validation error = %v", err)
	}
}

func TestCoalesceHandlesMissingOptionalReference(t *testing.T) {
	expr := contracts.ValueExpr{Op: "coalesce", Args: []contracts.ValueExpr{
		{Ref: "inputs.mode"}, {Literal: raw(`"fallback"`)},
	}}
	value, err := Evaluate(expr, expressionEnvironment(), Values{Inputs: map[string]any{"count": json.Number("1")}})
	if err != nil {
		t.Fatal(err)
	}
	if value != "fallback" {
		t.Fatalf("coalesced value = %#v", value)
	}
	if _, err := Evaluate(contracts.ValueExpr{Ref: "inputs.mode"}, expressionEnvironment(), Values{Inputs: map[string]any{"count": json.Number("1")}}); err == nil || !strings.Contains(err.Error(), "missing") {
		t.Fatalf("bare optional ref error = %v", err)
	}
}

func TestAssignableRequiresGuaranteedObjectFields(t *testing.T) {
	environment := expressionEnvironment()
	environment.Inputs.Properties["config"] = contracts.Schema{
		Type: contracts.TypeObject, Properties: map[string]contracts.Schema{"name": {Type: contracts.TypeString}},
	}
	target := contracts.Schema{
		Type: contracts.TypeObject, Properties: map[string]contracts.Schema{"name": {Type: contracts.TypeString}}, Required: []string{"name"},
	}
	if err := CheckAssignable(contracts.ValueExpr{Ref: "inputs.config"}, environment, target); err == nil || !strings.Contains(err.Error(), "not guaranteed") {
		t.Fatalf("required guarantee error = %v", err)
	}
}
