// Package expression statically checks and deterministically evaluates the
// bounded ValueExpr AST. It has no I/O, clock, random, process, or model hooks.
package expression

import (
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"unicode/utf8"

	"github.com/lxk36/xgc2-execution-platform/kernel/canonicaljson"
	"github.com/lxk36/xgc2-execution-platform/sdk/go/contracts"
)

type Limits struct {
	MaxDepth       int
	MaxNodes       int
	MaxStringBytes int
	MaxResultBytes int
}

func DefaultLimits() Limits {
	return Limits{MaxDepth: 64, MaxNodes: 2_048, MaxStringBytes: 64 << 10, MaxResultBytes: 1 << 20}
}

type Environment struct {
	Inputs       contracts.Schema
	Trigger      contracts.Schema
	Scope        contracts.Schema
	NodeOutputs  map[string]contracts.Schema
	VisibleNodes map[string]bool
	Iteration    *contracts.Schema
	Secrets      map[string]bool
}

func Check(expr contracts.ValueExpr, environment Environment) (contracts.Schema, error) {
	return CheckWithLimits(expr, environment, DefaultLimits())
}

func CheckWithLimits(expr contracts.ValueExpr, environment Environment, limits Limits) (contracts.Schema, error) {
	if err := validateLimits(limits); err != nil {
		return contracts.Schema{}, err
	}
	checker := checker{environment: environment, limits: limits}
	return checker.expression(expr, 1)
}

func CheckAssignable(expr contracts.ValueExpr, environment Environment, target contracts.Schema) error {
	if err := target.ValidateDefinition(); err != nil {
		return fmt.Errorf("target schema: %w", err)
	}
	source, err := Check(expr, environment)
	if err != nil {
		return err
	}
	if target.Format == contracts.FormatSecretHandle {
		if !isDirectSecretRef(expr) || source.Format != contracts.FormatSecretHandle {
			return errors.New("secret slot requires a direct secrets.<name> reference")
		}
		return nil
	}
	if source.Format == contracts.FormatSecretHandle {
		return errors.New("secret handle cannot flow into a public value slot")
	}
	if err := assignable(source, target, "$target"); err != nil {
		return err
	}
	if expr.Literal != nil {
		value, err := canonicaljson.Decode(*expr.Literal, canonicaljson.DefaultLimits())
		if err != nil {
			return err
		}
		if err := target.ValidateValue(value); err != nil {
			return fmt.Errorf("literal is not assignable: %w", err)
		}
	}
	return nil
}

type checker struct {
	environment Environment
	limits      Limits
	nodes       int
}

func (checker *checker) expression(expr contracts.ValueExpr, depth int) (contracts.Schema, error) {
	if depth > checker.limits.MaxDepth {
		return contracts.Schema{}, fmt.Errorf("expression exceeds depth %d", checker.limits.MaxDepth)
	}
	checker.nodes++
	if checker.nodes > checker.limits.MaxNodes {
		return contracts.Schema{}, fmt.Errorf("expression exceeds %d nodes", checker.limits.MaxNodes)
	}
	variants := 0
	if expr.Literal != nil {
		variants++
	}
	if expr.Ref != "" {
		variants++
	}
	if expr.Op != "" {
		variants++
	}
	if expr.Object != nil {
		variants++
	}
	if expr.Array != nil {
		variants++
	}
	if variants != 1 {
		return contracts.Schema{}, errors.New("ValueExpr must contain exactly one variant")
	}
	if expr.Op == "" && expr.Args != nil {
		return contracts.Schema{}, errors.New("ValueExpr args require an op")
	}
	switch {
	case expr.Literal != nil:
		value, err := canonicaljson.Decode(*expr.Literal, canonicaljson.DefaultLimits())
		if err != nil {
			return contracts.Schema{}, fmt.Errorf("literal: %w", err)
		}
		return inferLiteral(value)
	case expr.Ref != "":
		if len(expr.Ref) > checker.limits.MaxStringBytes || !utf8.ValidString(expr.Ref) {
			return contracts.Schema{}, errors.New("reference is invalid or too long")
		}
		return checker.reference(expr.Ref)
	case expr.Op != "":
		return checker.operation(expr.Op, expr.Args, depth)
	case expr.Object != nil:
		properties := make(map[string]contracts.Schema, len(expr.Object))
		required := make([]string, 0, len(expr.Object))
		for name, child := range expr.Object {
			if !contracts.ValidIdentifier(name) {
				return contracts.Schema{}, fmt.Errorf("object constructor key %q is invalid", name)
			}
			typed, err := checker.expression(child, depth+1)
			if err != nil {
				return contracts.Schema{}, fmt.Errorf("object key %q: %w", name, err)
			}
			if typed.Format == contracts.FormatSecretHandle {
				return contracts.Schema{}, errors.New("secret handles cannot be embedded in objects")
			}
			properties[name] = typed
			required = append(required, name)
		}
		return contracts.Schema{Type: contracts.TypeObject, Properties: properties, Required: required}, nil
	case expr.Array != nil:
		items := *expr.Array
		if len(items) == 0 {
			return contracts.Schema{}, errors.New("empty array constructor has no provable item type")
		}
		first, err := checker.expression(items[0], depth+1)
		if err != nil {
			return contracts.Schema{}, err
		}
		if first.Format == contracts.FormatSecretHandle {
			return contracts.Schema{}, errors.New("secret handles cannot be embedded in arrays")
		}
		for index := 1; index < len(items); index++ {
			current, err := checker.expression(items[index], depth+1)
			if err != nil {
				return contracts.Schema{}, fmt.Errorf("array item %d: %w", index, err)
			}
			if err := sameType(first, current); err != nil {
				return contracts.Schema{}, fmt.Errorf("array item %d: %w", index, err)
			}
		}
		return contracts.Schema{Type: contracts.TypeArray, Items: &first}, nil
	default:
		panic("unreachable")
	}
}

func (checker *checker) reference(ref string) (contracts.Schema, error) {
	segments := strings.Split(ref, ".")
	if len(segments) < 2 {
		return contracts.Schema{}, errors.New("reference must contain a namespace and path")
	}
	for _, segment := range segments {
		if !validPathSegment(segment) {
			return contracts.Schema{}, fmt.Errorf("reference path segment %q is invalid", segment)
		}
	}
	switch segments[0] {
	case "inputs":
		return resolve(checker.environment.Inputs, segments[1:], ref)
	case "trigger":
		return resolve(checker.environment.Trigger, segments[1:], ref)
	case "scope":
		return resolve(checker.environment.Scope, segments[1:], ref)
	case "iteration":
		if checker.environment.Iteration == nil {
			return contracts.Schema{}, errors.New("iteration namespace is not visible here")
		}
		return resolve(*checker.environment.Iteration, segments[1:], ref)
	case "nodes":
		if len(segments) < 4 || segments[2] != "output" {
			return contracts.Schema{}, errors.New("node reference must be nodes.<nodeId>.output.<path>")
		}
		nodeID := segments[1]
		if !checker.environment.VisibleNodes[nodeID] {
			return contracts.Schema{}, fmt.Errorf("node %q is not visible through a dominating data edge", nodeID)
		}
		schema, exists := checker.environment.NodeOutputs[nodeID]
		if !exists {
			return contracts.Schema{}, fmt.Errorf("node %q has no declared output schema", nodeID)
		}
		return resolve(schema, segments[3:], ref)
	case "secrets":
		if len(segments) != 2 || !checker.environment.Secrets[segments[1]] {
			return contracts.Schema{}, fmt.Errorf("secret %q is not declared", strings.Join(segments[1:], "."))
		}
		return contracts.Schema{Type: contracts.TypeString, Format: contracts.FormatSecretHandle}, nil
	default:
		return contracts.Schema{}, fmt.Errorf("unknown expression namespace %q", segments[0])
	}
}

func (checker *checker) operation(name string, expressions []contracts.ValueExpr, depth int) (contracts.Schema, error) {
	if !contracts.ValidIdentifier(name) {
		return contracts.Schema{}, fmt.Errorf("invalid operator %q", name)
	}
	arguments := make([]contracts.Schema, len(expressions))
	for index, argument := range expressions {
		typed, err := checker.expression(argument, depth+1)
		if err != nil {
			return contracts.Schema{}, fmt.Errorf("operator %s argument %d: %w", name, index, err)
		}
		if typed.Format == contracts.FormatSecretHandle {
			return contracts.Schema{}, fmt.Errorf("operator %s cannot consume a secret handle", name)
		}
		arguments[index] = typed
	}
	requireCount := func(minimum, maximum int) error {
		if len(arguments) < minimum || (maximum >= 0 && len(arguments) > maximum) {
			return fmt.Errorf("operator %s expects %d..%d arguments, got %d", name, minimum, maximum, len(arguments))
		}
		return nil
	}
	requireAll := func(types ...contracts.JSONType) error {
		allowed := make(map[contracts.JSONType]bool, len(types))
		for _, valueType := range types {
			allowed[valueType] = true
		}
		for index, argument := range arguments {
			if !allowed[argument.Type] {
				return fmt.Errorf("operator %s argument %d has type %s", name, index, argument.Type)
			}
		}
		return nil
	}
	switch name {
	case "and", "or":
		if err := requireCount(2, -1); err != nil {
			return contracts.Schema{}, err
		}
		if err := requireAll(contracts.TypeBoolean); err != nil {
			return contracts.Schema{}, err
		}
		return contracts.Schema{Type: contracts.TypeBoolean}, nil
	case "not":
		if err := requireCount(1, 1); err != nil {
			return contracts.Schema{}, err
		}
		if err := requireAll(contracts.TypeBoolean); err != nil {
			return contracts.Schema{}, err
		}
		return contracts.Schema{Type: contracts.TypeBoolean}, nil
	case "eq", "ne":
		if err := requireCount(2, 2); err != nil {
			return contracts.Schema{}, err
		}
		if err := sameComparableType(arguments[0], arguments[1]); err != nil {
			return contracts.Schema{}, err
		}
		return contracts.Schema{Type: contracts.TypeBoolean}, nil
	case "lt", "lte", "gt", "gte":
		if err := requireCount(2, 2); err != nil {
			return contracts.Schema{}, err
		}
		if err := requireAll(contracts.TypeString, contracts.TypeNumber, contracts.TypeInteger); err != nil {
			return contracts.Schema{}, err
		}
		if err := sameComparableType(arguments[0], arguments[1]); err != nil {
			return contracts.Schema{}, err
		}
		return contracts.Schema{Type: contracts.TypeBoolean}, nil
	case "add", "sub", "mul", "div":
		if err := requireCount(2, 2); err != nil {
			return contracts.Schema{}, err
		}
		if err := requireAll(contracts.TypeNumber, contracts.TypeInteger); err != nil {
			return contracts.Schema{}, err
		}
		if name != "div" && arguments[0].Type == contracts.TypeInteger && arguments[1].Type == contracts.TypeInteger {
			return contracts.Schema{Type: contracts.TypeInteger}, nil
		}
		return contracts.Schema{Type: contracts.TypeNumber}, nil
	case "coalesce":
		if err := requireCount(2, -1); err != nil {
			return contracts.Schema{}, err
		}
		var result *contracts.Schema
		for _, argument := range arguments {
			if argument.Type == contracts.TypeNull {
				continue
			}
			if result == nil {
				copy := argument
				result = &copy
				continue
			}
			if err := sameType(*result, argument); err != nil {
				return contracts.Schema{}, fmt.Errorf("operator coalesce: %w", err)
			}
		}
		if result == nil {
			return contracts.Schema{Type: contracts.TypeNull}, nil
		}
		return *result, nil
	case "concat":
		if err := requireCount(2, -1); err != nil {
			return contracts.Schema{}, err
		}
		if err := requireAll(contracts.TypeString); err != nil {
			return contracts.Schema{}, err
		}
		return contracts.Schema{Type: contracts.TypeString}, nil
	case "length":
		if err := requireCount(1, 1); err != nil {
			return contracts.Schema{}, err
		}
		if err := requireAll(contracts.TypeString, contracts.TypeArray, contracts.TypeObject); err != nil {
			return contracts.Schema{}, err
		}
		return contracts.Schema{Type: contracts.TypeInteger}, nil
	case "contains":
		if err := requireCount(2, 2); err != nil {
			return contracts.Schema{}, err
		}
		if arguments[0].Type == contracts.TypeString && arguments[1].Type == contracts.TypeString {
			return contracts.Schema{Type: contracts.TypeBoolean}, nil
		}
		if arguments[0].Type == contracts.TypeArray && assignable(arguments[1], *arguments[0].Items, "contains") == nil {
			return contracts.Schema{Type: contracts.TypeBoolean}, nil
		}
		return contracts.Schema{}, errors.New("contains requires string/string or array/item arguments")
	default:
		return contracts.Schema{}, fmt.Errorf("unknown expression operator %q", name)
	}
}

func resolve(schema contracts.Schema, segments []string, ref string) (contracts.Schema, error) {
	resolved, err := schema.Resolve(segments)
	if err != nil {
		return contracts.Schema{}, fmt.Errorf("reference %q: %w", ref, err)
	}
	return resolved, nil
}

func validPathSegment(segment string) bool {
	if contracts.ValidIdentifier(segment) {
		return true
	}
	if segment == "" {
		return false
	}
	_, err := strconv.ParseUint(segment, 10, 31)
	return err == nil
}

func inferLiteral(value any) (contracts.Schema, error) {
	switch typed := value.(type) {
	case nil:
		return contracts.Schema{Type: contracts.TypeNull}, nil
	case bool:
		return contracts.Schema{Type: contracts.TypeBoolean}, nil
	case string:
		return contracts.Schema{Type: contracts.TypeString}, nil
	case json.Number:
		if rational := number(typed); rational == nil || !rational.IsInt() {
			return contracts.Schema{Type: contracts.TypeNumber}, nil
		}
		return contracts.Schema{Type: contracts.TypeInteger}, nil
	case []any:
		if len(typed) == 0 {
			return contracts.Schema{}, errors.New("empty array literal has no provable item type")
		}
		item, err := inferLiteral(typed[0])
		if err != nil {
			return contracts.Schema{}, err
		}
		for index := 1; index < len(typed); index++ {
			current, err := inferLiteral(typed[index])
			if err != nil {
				return contracts.Schema{}, err
			}
			if err := sameType(item, current); err != nil {
				return contracts.Schema{}, fmt.Errorf("array literal item %d: %w", index, err)
			}
		}
		return contracts.Schema{Type: contracts.TypeArray, Items: &item}, nil
	case map[string]any:
		properties := make(map[string]contracts.Schema, len(typed))
		required := make([]string, 0, len(typed))
		for name, child := range typed {
			schema, err := inferLiteral(child)
			if err != nil {
				return contracts.Schema{}, err
			}
			properties[name] = schema
			required = append(required, name)
		}
		return contracts.Schema{Type: contracts.TypeObject, Properties: properties, Required: required}, nil
	default:
		return contracts.Schema{}, fmt.Errorf("unsupported literal %T", value)
	}
}

func sameComparableType(left, right contracts.Schema) error {
	if left.Type == contracts.TypeObject || left.Type == contracts.TypeArray || left.Type == contracts.TypeNull ||
		right.Type == contracts.TypeObject || right.Type == contracts.TypeArray || right.Type == contracts.TypeNull {
		return errors.New("comparison operands must be non-null scalar values")
	}
	if (left.Type == contracts.TypeInteger || left.Type == contracts.TypeNumber) &&
		(right.Type == contracts.TypeInteger || right.Type == contracts.TypeNumber) {
		return nil
	}
	return sameType(left, right)
}

func sameType(left, right contracts.Schema) error {
	if left.Type != right.Type || left.Format != right.Format {
		return fmt.Errorf("types %s and %s are incompatible", left.Type, right.Type)
	}
	return nil
}

func assignable(source, target contracts.Schema, path string) error {
	if source.Format != target.Format {
		return fmt.Errorf("%s: format %q is not assignable to %q", path, source.Format, target.Format)
	}
	if source.Type != target.Type && !(source.Type == contracts.TypeInteger && target.Type == contracts.TypeNumber) {
		return fmt.Errorf("%s: type %s is not assignable to %s", path, source.Type, target.Type)
	}
	switch target.Type {
	case contracts.TypeObject:
		for _, required := range target.Required {
			property, exists := source.Properties[required]
			if !exists {
				return fmt.Errorf("%s: required property %q is not guaranteed", path, required)
			}
			if err := assignable(property, target.Properties[required], path+"."+required); err != nil {
				return err
			}
		}
		if !target.AdditionalProperties {
			for name := range source.Properties {
				targetProperty, exists := target.Properties[name]
				if !exists {
					return fmt.Errorf("%s: property %q is not accepted", path, name)
				}
				if err := assignable(source.Properties[name], targetProperty, path+"."+name); err != nil {
					return err
				}
			}
		}
	case contracts.TypeArray:
		if source.Items == nil || target.Items == nil {
			return fmt.Errorf("%s: array item schema is missing", path)
		}
		return assignable(*source.Items, *target.Items, path+"[]")
	}
	return nil
}

func isDirectSecretRef(expr contracts.ValueExpr) bool {
	return expr.Ref != "" && strings.HasPrefix(expr.Ref, "secrets.") && strings.Count(expr.Ref, ".") == 1
}

func validateLimits(limits Limits) error {
	if limits.MaxDepth <= 0 || limits.MaxNodes <= 0 || limits.MaxStringBytes <= 0 || limits.MaxResultBytes <= 0 {
		return errors.New("expression limits must all be positive")
	}
	return nil
}
