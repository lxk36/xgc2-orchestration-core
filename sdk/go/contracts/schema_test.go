package contracts

import (
	"encoding/json"
	"strings"
	"testing"
)

func TestSchemaValidateAndDefaults(t *testing.T) {
	defaultMode := json.RawMessage(`"safe"`)
	schema := Schema{
		Type: TypeObject,
		Properties: map[string]Schema{
			"name": {Type: TypeString},
			"mode": {Type: TypeString, Default: defaultMode},
		},
		Required: []string{"name", "mode"},
	}
	value, applied, err := schema.ApplyDefaults(map[string]any{"name": "demo"})
	if err != nil {
		t.Fatal(err)
	}
	if value["mode"] != "safe" {
		t.Fatalf("mode = %#v", value["mode"])
	}
	if _, ok := applied["/mode"]; !ok {
		t.Fatalf("default provenance = %#v", applied)
	}
	if err := schema.ValidateValue(value); err != nil {
		t.Fatal(err)
	}
}

func TestSchemaRejectsUnknownAndWrongType(t *testing.T) {
	schema := Schema{Type: TypeObject, Properties: map[string]Schema{"count": {Type: TypeInteger}}, Required: []string{"count"}}
	if err := schema.ValidateValue(map[string]any{"count": json.Number("2"), "extra": true}); err == nil || !strings.Contains(err.Error(), "unknown property") {
		t.Fatalf("unknown property error = %v", err)
	}
	if err := schema.ValidateValue(map[string]any{"count": json.Number("2.5")}); err == nil || !strings.Contains(err.Error(), "expected integer") {
		t.Fatalf("integer error = %v", err)
	}
}

func TestSchemaSecretFormatIsNarrow(t *testing.T) {
	valid := Schema{Type: TypeString, Format: FormatSecretHandle}
	if err := valid.ValidateDefinition(); err != nil {
		t.Fatal(err)
	}
	invalid := Schema{Type: TypeInteger, Format: FormatSecretHandle}
	if err := invalid.ValidateDefinition(); err == nil {
		t.Fatal("expected invalid secret format")
	}
}
