package expression

import (
	"encoding/json"
	"errors"
	"fmt"
	"math/big"
	"reflect"
	"strconv"
	"strings"
	"unicode/utf8"

	"github.com/lxk36/xgc2-execution-platform/kernel/canonicaljson"
	"github.com/lxk36/xgc2-execution-platform/sdk/go/contracts"
)

type Values struct {
	Inputs      map[string]any
	Trigger     map[string]any
	Scope       map[string]any
	NodeOutputs map[string]any
	Iteration   any
	Secrets     map[string]contracts.SecretHandle
}

func Evaluate(expr contracts.ValueExpr, environment Environment, values Values) (any, error) {
	return EvaluateWithLimits(expr, environment, values, DefaultLimits())
}

func EvaluateWithLimits(expr contracts.ValueExpr, environment Environment, values Values, limits Limits) (any, error) {
	if _, err := CheckWithLimits(expr, environment, limits); err != nil {
		return nil, err
	}
	if err := validateValues(environment, values); err != nil {
		return nil, err
	}
	evaluator := evaluator{values: values}
	result, err := evaluator.expression(expr)
	if err != nil {
		return nil, err
	}
	if _, missing := result.(missingValue); missing {
		return nil, errors.New("expression result is missing")
	}
	if _, secret := result.(contracts.SecretHandle); secret {
		return result, nil
	}
	canonical, err := canonicaljson.Marshal(result)
	if err != nil {
		return nil, fmt.Errorf("expression result: %w", err)
	}
	if len(canonical) > limits.MaxResultBytes {
		return nil, fmt.Errorf("expression result exceeds %d bytes", limits.MaxResultBytes)
	}
	return result, nil
}

func validateValues(environment Environment, values Values) error {
	objects := []struct {
		name   string
		schema contracts.Schema
		value  map[string]any
	}{
		{"inputs", environment.Inputs, values.Inputs},
		{"trigger", environment.Trigger, values.Trigger},
		{"scope", environment.Scope, values.Scope},
	}
	for _, item := range objects {
		value := item.value
		if value == nil {
			value = map[string]any{}
		}
		if err := item.schema.ValidateValue(value); err != nil {
			return fmt.Errorf("%s values: %w", item.name, err)
		}
	}
	for nodeID, value := range values.NodeOutputs {
		schema, exists := environment.NodeOutputs[nodeID]
		if !exists {
			return fmt.Errorf("node output value %q has no schema", nodeID)
		}
		if err := schema.ValidateValue(value); err != nil {
			return fmt.Errorf("node output %q: %w", nodeID, err)
		}
	}
	if environment.Iteration != nil {
		if err := environment.Iteration.ValidateValue(values.Iteration); err != nil {
			return fmt.Errorf("iteration value: %w", err)
		}
	}
	for name, handle := range values.Secrets {
		if !environment.Secrets[name] || !handle.Valid() {
			return fmt.Errorf("secret handle %q is undeclared or invalid", name)
		}
	}
	return nil
}

type evaluator struct {
	values Values
}

type missingValue struct{}

func (evaluator evaluator) expression(expr contracts.ValueExpr) (any, error) {
	switch {
	case expr.Literal != nil:
		return canonicaljson.Decode(*expr.Literal, canonicaljson.DefaultLimits())
	case expr.Ref != "":
		return evaluator.reference(expr.Ref)
	case expr.Op != "":
		if expr.Op == "coalesce" {
			for _, child := range expr.Args {
				value, err := evaluator.expression(child)
				if err != nil {
					return nil, err
				}
				if value == nil {
					continue
				}
				if _, missing := value.(missingValue); missing {
					continue
				}
				return value, nil
			}
			return nil, nil
		}
		arguments := make([]any, len(expr.Args))
		for index, child := range expr.Args {
			value, err := evaluator.expression(child)
			if err != nil {
				return nil, err
			}
			if _, missing := value.(missingValue); missing {
				return nil, fmt.Errorf("operator %s argument %d is missing", expr.Op, index)
			}
			arguments[index] = value
		}
		return evaluateOperation(expr.Op, arguments)
	case expr.Object != nil:
		result := make(map[string]any, len(expr.Object))
		for name, child := range expr.Object {
			value, err := evaluator.expression(child)
			if err != nil {
				return nil, err
			}
			if _, missing := value.(missingValue); missing {
				return nil, fmt.Errorf("object member %q is missing", name)
			}
			result[name] = value
		}
		return result, nil
	case expr.Array != nil:
		result := make([]any, len(*expr.Array))
		for index, child := range *expr.Array {
			value, err := evaluator.expression(child)
			if err != nil {
				return nil, err
			}
			if _, missing := value.(missingValue); missing {
				return nil, fmt.Errorf("array item %d is missing", index)
			}
			result[index] = value
		}
		return result, nil
	default:
		return nil, errors.New("invalid expression")
	}
}

func (evaluator evaluator) reference(ref string) (any, error) {
	segments := strings.Split(ref, ".")
	switch segments[0] {
	case "inputs":
		return resolveValue(evaluator.values.Inputs, segments[1:], ref)
	case "trigger":
		return resolveValue(evaluator.values.Trigger, segments[1:], ref)
	case "scope":
		return resolveValue(evaluator.values.Scope, segments[1:], ref)
	case "iteration":
		return resolveValue(evaluator.values.Iteration, segments[1:], ref)
	case "nodes":
		output, exists := evaluator.values.NodeOutputs[segments[1]]
		if !exists {
			return nil, fmt.Errorf("reference %q has no node output value", ref)
		}
		return resolveValue(output, segments[3:], ref)
	case "secrets":
		handle, exists := evaluator.values.Secrets[segments[1]]
		if !exists || !handle.Valid() {
			return nil, fmt.Errorf("reference %q has no valid secret handle", ref)
		}
		return handle, nil
	default:
		return nil, fmt.Errorf("unknown reference namespace %q", segments[0])
	}
}

func resolveValue(value any, segments []string, ref string) (any, error) {
	current := value
	for _, segment := range segments {
		switch typed := current.(type) {
		case map[string]any:
			next, exists := typed[segment]
			if !exists {
				return missingValue{}, nil
			}
			current = next
		case []any:
			index, err := strconv.Atoi(segment)
			if err != nil || index < 0 || index >= len(typed) {
				return missingValue{}, nil
			}
			current = typed[index]
		default:
			if _, missing := current.(missingValue); missing {
				return current, nil
			}
			return nil, fmt.Errorf("reference %q crosses scalar at %q", ref, segment)
		}
	}
	return current, nil
}

func evaluateOperation(name string, arguments []any) (any, error) {
	switch name {
	case "and":
		for _, argument := range arguments {
			if !argument.(bool) {
				return false, nil
			}
		}
		return true, nil
	case "or":
		for _, argument := range arguments {
			if argument.(bool) {
				return true, nil
			}
		}
		return false, nil
	case "not":
		return !arguments[0].(bool), nil
	case "eq", "ne":
		equal, err := equalValues(arguments[0], arguments[1])
		if err != nil {
			return nil, err
		}
		if name == "ne" {
			equal = !equal
		}
		return equal, nil
	case "lt", "lte", "gt", "gte":
		comparison, err := compareValues(arguments[0], arguments[1])
		if err != nil {
			return nil, err
		}
		switch name {
		case "lt":
			return comparison < 0, nil
		case "lte":
			return comparison <= 0, nil
		case "gt":
			return comparison > 0, nil
		default:
			return comparison >= 0, nil
		}
	case "add", "sub", "mul", "div":
		left, right := number(arguments[0]), number(arguments[1])
		if left == nil || right == nil {
			return nil, errors.New("numeric operator received a non-number")
		}
		result := new(big.Rat)
		switch name {
		case "add":
			result.Add(left, right)
		case "sub":
			result.Sub(left, right)
		case "mul":
			result.Mul(left, right)
		case "div":
			if right.Sign() == 0 {
				return nil, errors.New("division by zero")
			}
			result.Quo(left, right)
		}
		normalized, err := terminatingDecimal(result, canonicaljson.DefaultMaxExponent)
		if err != nil {
			return nil, err
		}
		return json.Number(normalized), nil
	case "coalesce":
		for _, argument := range arguments {
			if argument != nil {
				return argument, nil
			}
		}
		return nil, nil
	case "concat":
		var result strings.Builder
		for _, argument := range arguments {
			result.WriteString(argument.(string))
		}
		return result.String(), nil
	case "length":
		switch typed := arguments[0].(type) {
		case string:
			return json.Number(strconv.Itoa(utf8.RuneCountInString(typed))), nil
		case []any:
			return json.Number(strconv.Itoa(len(typed))), nil
		case map[string]any:
			return json.Number(strconv.Itoa(len(typed))), nil
		default:
			return nil, fmt.Errorf("length received %T", arguments[0])
		}
	case "contains":
		switch container := arguments[0].(type) {
		case string:
			return strings.Contains(container, arguments[1].(string)), nil
		case []any:
			for _, item := range container {
				equal, err := equalValues(item, arguments[1])
				if err != nil {
					return nil, err
				}
				if equal {
					return true, nil
				}
			}
			return false, nil
		default:
			return nil, fmt.Errorf("contains received %T", arguments[0])
		}
	default:
		return nil, fmt.Errorf("unknown operator %q", name)
	}
}

func equalValues(left, right any) (bool, error) {
	leftNumber, rightNumber := number(left), number(right)
	if leftNumber != nil || rightNumber != nil {
		if leftNumber == nil || rightNumber == nil {
			return false, errors.New("cannot compare number with non-number")
		}
		return leftNumber.Cmp(rightNumber) == 0, nil
	}
	return reflect.DeepEqual(left, right), nil
}

func compareValues(left, right any) (int, error) {
	leftNumber, rightNumber := number(left), number(right)
	if leftNumber != nil && rightNumber != nil {
		return leftNumber.Cmp(rightNumber), nil
	}
	leftString, leftOK := left.(string)
	rightString, rightOK := right.(string)
	if leftOK && rightOK {
		return strings.Compare(leftString, rightString), nil
	}
	return 0, errors.New("ordered comparison requires two numbers or two strings")
}

func number(value any) *big.Rat {
	raw, err := json.Marshal(value)
	if err != nil {
		return nil
	}
	rational, _ := new(big.Rat).SetString(string(raw))
	return rational
}

func terminatingDecimal(value *big.Rat, maxScale int) (string, error) {
	numerator := new(big.Int).Set(value.Num())
	denominator := new(big.Int).Set(value.Denom())
	two, five := big.NewInt(2), big.NewInt(5)
	remainder := new(big.Int)
	twos, fives := 0, 0
	for remainder.Mod(denominator, two).Sign() == 0 {
		denominator.Div(denominator, two)
		twos++
	}
	for remainder.Mod(denominator, five).Sign() == 0 {
		denominator.Div(denominator, five)
		fives++
	}
	if denominator.Cmp(big.NewInt(1)) != 0 {
		return "", errors.New("numeric result has no finite decimal representation")
	}
	scale := twos
	if fives > scale {
		scale = fives
	}
	if scale > maxScale {
		return "", errors.New("numeric result exceeds decimal scale limit")
	}
	if twos < scale {
		numerator.Mul(numerator, new(big.Int).Exp(two, big.NewInt(int64(scale-twos)), nil))
	}
	if fives < scale {
		numerator.Mul(numerator, new(big.Int).Exp(five, big.NewInt(int64(scale-fives)), nil))
	}
	negative := numerator.Sign() < 0
	if negative {
		numerator.Abs(numerator)
	}
	digits := numerator.String()
	if scale > 0 {
		if len(digits) <= scale {
			digits = strings.Repeat("0", scale-len(digits)+1) + digits
		}
		position := len(digits) - scale
		digits = strings.TrimRight(digits[:position]+"."+digits[position:], "0")
		digits = strings.TrimSuffix(digits, ".")
	}
	if digits == "" || digits == "0" {
		return "0", nil
	}
	if negative {
		digits = "-" + digits
	}
	return digits, nil
}
