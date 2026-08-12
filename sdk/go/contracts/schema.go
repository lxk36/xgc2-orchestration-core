package contracts

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"math/big"
	"reflect"
	"sort"
	"strconv"
	"strings"
)

type JSONType string

const (
	TypeObject  JSONType = "object"
	TypeArray   JSONType = "array"
	TypeString  JSONType = "string"
	TypeNumber  JSONType = "number"
	TypeInteger JSONType = "integer"
	TypeBoolean JSONType = "boolean"
	TypeNull    JSONType = "null"
)

const FormatSecretHandle = "xgc-secret-handle"

// Schema is the deliberately small JSON Schema profile understood by the S1
// compiler. Unsupported JSON Schema keywords fail at the wire decoder instead
// of being silently ignored by different language implementations.
type Schema struct {
	Type                 JSONType          `json:"type"`
	Properties           map[string]Schema `json:"properties,omitempty"`
	Required             []string          `json:"required,omitempty"`
	AdditionalProperties bool              `json:"additionalProperties"`
	Items                *Schema           `json:"items,omitempty"`
	Default              json.RawMessage   `json:"default,omitempty"`
	Enum                 []json.RawMessage `json:"enum,omitempty"`
	Format               string            `json:"format,omitempty"`
}

func (schema Schema) ValidateDefinition() error {
	return schema.validateDefinition("$")
}

func (schema Schema) validateDefinition(path string) error {
	switch schema.Type {
	case TypeObject:
		if schema.Items != nil {
			return fmt.Errorf("%s: object schema cannot declare items", path)
		}
		required := make(map[string]struct{}, len(schema.Required))
		for _, name := range schema.Required {
			if !ValidIdentifier(name) {
				return fmt.Errorf("%s: required property %q is invalid", path, name)
			}
			if _, duplicate := required[name]; duplicate {
				return fmt.Errorf("%s: required property %q is duplicated", path, name)
			}
			if _, exists := schema.Properties[name]; !exists {
				return fmt.Errorf("%s: required property %q has no schema", path, name)
			}
			required[name] = struct{}{}
		}
		for name, property := range schema.Properties {
			if !ValidIdentifier(name) {
				return fmt.Errorf("%s: property %q is invalid", path, name)
			}
			if err := property.validateDefinition(path + "." + name); err != nil {
				return err
			}
		}
	case TypeArray:
		if schema.Items == nil {
			return fmt.Errorf("%s: array schema requires items", path)
		}
		if len(schema.Properties) != 0 || len(schema.Required) != 0 || schema.AdditionalProperties {
			return fmt.Errorf("%s: array schema contains object-only keywords", path)
		}
		if err := schema.Items.validateDefinition(path + "[]"); err != nil {
			return err
		}
	case TypeString, TypeNumber, TypeInteger, TypeBoolean, TypeNull:
		if len(schema.Properties) != 0 || len(schema.Required) != 0 || schema.Items != nil || schema.AdditionalProperties {
			return fmt.Errorf("%s: scalar schema contains container-only keywords", path)
		}
	default:
		return fmt.Errorf("%s: unsupported schema type %q", path, schema.Type)
	}
	if schema.Format != "" && !(schema.Type == TypeString && schema.Format == FormatSecretHandle) {
		return fmt.Errorf("%s: unsupported format %q", path, schema.Format)
	}
	if schema.Format == FormatSecretHandle && (len(schema.Default) != 0 || len(schema.Enum) != 0) {
		return fmt.Errorf("%s: secret handle schema cannot declare default or enum", path)
	}
	if len(schema.Default) != 0 {
		value, err := decodeJSON(schema.Default)
		if err != nil {
			return fmt.Errorf("%s: default: %w", path, err)
		}
		if err := schema.validateValue(value, path+".default"); err != nil {
			return err
		}
	}
	for index, raw := range schema.Enum {
		value, err := decodeJSON(raw)
		if err != nil {
			return fmt.Errorf("%s: enum[%d]: %w", path, index, err)
		}
		if err := schema.validateValueWithoutEnum(value, path+".enum["+strconv.Itoa(index)+"]"); err != nil {
			return err
		}
	}
	return nil
}

func (schema Schema) ContainsFormat(format string) bool {
	if schema.Format == format {
		return true
	}
	if schema.Items != nil && schema.Items.ContainsFormat(format) {
		return true
	}
	for _, property := range schema.Properties {
		if property.ContainsFormat(format) {
			return true
		}
	}
	return false
}

func (schema Schema) ValidateValue(value any) error {
	if err := schema.ValidateDefinition(); err != nil {
		return err
	}
	return schema.validateValue(value, "$")
}

func (schema Schema) validateValue(value any, path string) error {
	if err := schema.validateValueWithoutEnum(value, path); err != nil {
		return err
	}
	if len(schema.Enum) == 0 {
		return nil
	}
	for _, raw := range schema.Enum {
		candidate, err := decodeJSON(raw)
		if err == nil && valuesEqual(candidate, value) {
			return nil
		}
	}
	return fmt.Errorf("%s: value is not in enum", path)
}

func (schema Schema) validateValueWithoutEnum(value any, path string) error {
	switch schema.Type {
	case TypeObject:
		object, ok := value.(map[string]any)
		if !ok {
			return typeError(path, schema.Type, value)
		}
		for _, name := range schema.Required {
			if _, exists := object[name]; !exists {
				return fmt.Errorf("%s: required property %q is missing", path, name)
			}
		}
		for name, child := range object {
			property, exists := schema.Properties[name]
			if !exists {
				if schema.AdditionalProperties {
					continue
				}
				return fmt.Errorf("%s: unknown property %q", path, name)
			}
			if err := property.validateValue(child, path+"."+name); err != nil {
				return err
			}
		}
	case TypeArray:
		array, ok := value.([]any)
		if !ok {
			return typeError(path, schema.Type, value)
		}
		for index, child := range array {
			if err := schema.Items.validateValue(child, fmt.Sprintf("%s[%d]", path, index)); err != nil {
				return err
			}
		}
	case TypeString:
		if _, ok := value.(string); !ok {
			return typeError(path, schema.Type, value)
		}
	case TypeNumber:
		if !isNumber(value, false) {
			return typeError(path, schema.Type, value)
		}
	case TypeInteger:
		if !isNumber(value, true) {
			return typeError(path, schema.Type, value)
		}
	case TypeBoolean:
		if _, ok := value.(bool); !ok {
			return typeError(path, schema.Type, value)
		}
	case TypeNull:
		if value != nil {
			return typeError(path, schema.Type, value)
		}
	default:
		return fmt.Errorf("%s: unsupported schema type %q", path, schema.Type)
	}
	return nil
}

func typeError(path string, expected JSONType, value any) error {
	return fmt.Errorf("%s: expected %s, got %T", path, expected, value)
}

func isNumber(value any, integer bool) bool {
	var rational *big.Rat
	switch typed := value.(type) {
	case json.Number:
		rational, _ = new(big.Rat).SetString(typed.String())
	case int:
		rational = new(big.Rat).SetInt64(int64(typed))
	case int8:
		rational = new(big.Rat).SetInt64(int64(typed))
	case int16:
		rational = new(big.Rat).SetInt64(int64(typed))
	case int32:
		rational = new(big.Rat).SetInt64(int64(typed))
	case int64:
		rational = new(big.Rat).SetInt64(typed)
	case uint:
		rational = new(big.Rat).SetInt(new(big.Int).SetUint64(uint64(typed)))
	case uint8:
		rational = new(big.Rat).SetInt64(int64(typed))
	case uint16:
		rational = new(big.Rat).SetInt64(int64(typed))
	case uint32:
		rational = new(big.Rat).SetInt(new(big.Int).SetUint64(uint64(typed)))
	case uint64:
		rational = new(big.Rat).SetInt(new(big.Int).SetUint64(typed))
	case float32:
		if math.IsNaN(float64(typed)) || math.IsInf(float64(typed), 0) {
			return false
		}
		rational = new(big.Rat).SetFloat64(float64(typed))
	case float64:
		if math.IsNaN(typed) || math.IsInf(typed, 0) {
			return false
		}
		rational = new(big.Rat).SetFloat64(typed)
	default:
		return false
	}
	return rational != nil && (!integer || rational.IsInt())
}

func valuesEqual(left, right any) bool {
	if isNumber(left, false) && isNumber(right, false) {
		leftRat := numberRat(left)
		rightRat := numberRat(right)
		return leftRat != nil && rightRat != nil && leftRat.Cmp(rightRat) == 0
	}
	return reflect.DeepEqual(left, right)
}

func numberRat(value any) *big.Rat {
	raw, err := json.Marshal(value)
	if err != nil {
		return nil
	}
	rational, _ := new(big.Rat).SetString(string(raw))
	return rational
}

func (schema Schema) Resolve(segments []string) (Schema, error) {
	current := schema
	for index, segment := range segments {
		switch current.Type {
		case TypeObject:
			next, exists := current.Properties[segment]
			if !exists {
				return Schema{}, fmt.Errorf("schema path segment %q at %d does not exist", segment, index)
			}
			current = next
		case TypeArray:
			if current.Items == nil {
				return Schema{}, fmt.Errorf("schema path reaches array without an item schema at segment %q", segment)
			}
			if segment == "" || (len(segment) > 1 && segment[0] == '0') {
				return Schema{}, fmt.Errorf("schema path segment %q at %d is not a canonical array index", segment, index)
			}
			if _, err := strconv.ParseUint(segment, 10, 31); err != nil {
				return Schema{}, fmt.Errorf("schema path segment %q at %d is not an array index", segment, index)
			}
			current = *current.Items
		default:
			return Schema{}, fmt.Errorf("schema path crosses scalar %s at segment %q", current.Type, segment)
		}
	}
	return current, nil
}

func (schema Schema) ApplyDefaults(value map[string]any) (map[string]any, map[string]struct{}, error) {
	if err := schema.ValidateDefinition(); err != nil {
		return nil, nil, err
	}
	if schema.Type != TypeObject {
		return nil, nil, errors.New("defaults require an object schema")
	}
	cloned, err := CloneObject(value)
	if err != nil {
		return nil, nil, err
	}
	applied := make(map[string]struct{})
	if err := applyObjectDefaults(schema, cloned, "", applied); err != nil {
		return nil, nil, err
	}
	return cloned, applied, nil
}

func applyObjectDefaults(schema Schema, object map[string]any, pointer string, applied map[string]struct{}) error {
	names := make([]string, 0, len(schema.Properties))
	for name := range schema.Properties {
		names = append(names, name)
	}
	sort.Strings(names)
	for _, name := range names {
		property := schema.Properties[name]
		childPointer := pointer + "/" + escapePointer(name)
		value, exists := object[name]
		if !exists && len(property.Default) != 0 {
			decoded, err := decodeJSON(property.Default)
			if err != nil {
				return err
			}
			object[name] = decoded
			value, exists = decoded, true
			markLeaves(decoded, childPointer, applied)
		}
		if exists && property.Type == TypeObject {
			child, ok := value.(map[string]any)
			if ok {
				if err := applyObjectDefaults(property, child, childPointer, applied); err != nil {
					return err
				}
			}
		}
	}
	return nil
}

func markLeaves(value any, pointer string, target map[string]struct{}) {
	switch typed := value.(type) {
	case map[string]any:
		if len(typed) == 0 {
			target[pointer] = struct{}{}
			return
		}
		for name, child := range typed {
			markLeaves(child, pointer+"/"+escapePointer(name), target)
		}
	case []any:
		if len(typed) == 0 {
			target[pointer] = struct{}{}
			return
		}
		for index, child := range typed {
			markLeaves(child, pointer+"/"+strconv.Itoa(index), target)
		}
	default:
		target[pointer] = struct{}{}
	}
}

func CloneObject(source map[string]any) (map[string]any, error) {
	if source == nil {
		source = map[string]any{}
	}
	raw, err := json.Marshal(source)
	if err != nil {
		return nil, fmt.Errorf("clone object: %w", err)
	}
	decoded, err := decodeJSON(raw)
	if err != nil {
		return nil, err
	}
	object, ok := decoded.(map[string]any)
	if !ok {
		return nil, errors.New("clone object: source is not an object")
	}
	return object, nil
}

func decodeJSON(raw []byte) (any, error) {
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.UseNumber()
	var value any
	if err := decoder.Decode(&value); err != nil {
		return nil, err
	}
	var trailing any
	if err := decoder.Decode(&trailing); !errors.Is(err, io.EOF) {
		return nil, errors.New("multiple JSON values")
	}
	return value, nil
}

func escapePointer(value string) string {
	return strings.ReplaceAll(strings.ReplaceAll(value, "~", "~0"), "/", "~1")
}
