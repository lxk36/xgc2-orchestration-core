package workflow

import (
	"errors"
	"fmt"
	"strconv"
	"strings"

	"github.com/lxk36/xgc2-orchestration-core/kernel/expression"
	"github.com/lxk36/xgc2-orchestration-core/sdk/go/contracts"
)

// ResolveNodeInputs materializes one immutable node input object from a
// previously compiled definition. It performs no I/O and exposes secret
// handles only as opaque identifiers; secret values never enter the result.
func ResolveNodeInputs(
	definition contracts.WorkflowDefinition,
	nodeID string,
	inputs map[string]any,
	trigger map[string]any,
	scope map[string]any,
	outputs map[string]map[string]any,
	secrets map[string]contracts.SecretHandle,
) (map[string]any, error) {
	node, ok := workflowNode(definition, nodeID)
	if !ok {
		return nil, fmt.Errorf("workflow node %q does not exist", nodeID)
	}
	return resolveObject(definition, node.InputSchema, node.FixedInputs, node.Bindings, inputs, trigger, scope, outputs, secrets)
}

// ResolveCallActionContext freezes the only values visible to a child Action.
// Parent inputs, trigger, scope, and node outputs do not cross the boundary
// unless the authored CallAction maps them explicitly.
func ResolveCallActionContext(
	definition contracts.WorkflowDefinition,
	nodeID string,
	inputs map[string]any,
	trigger map[string]any,
	scope map[string]any,
	outputs map[string]map[string]any,
) (map[string]any, map[string]any, map[string]any, error) {
	node, ok := workflowNode(definition, nodeID)
	if !ok || node.CallAction == nil {
		return nil, nil, nil, fmt.Errorf("workflow node %q is not a child Action call", nodeID)
	}
	call := *node.CallAction
	childInputs, err := resolveObject(definition, call.InputSchema, nil, call.InputMap, inputs, trigger, scope, outputs, nil)
	if err != nil {
		return nil, nil, nil, fmt.Errorf("child Action inputs: %w", err)
	}
	childTrigger, err := resolveObject(definition, call.TriggerSchema, nil, call.TriggerMap, inputs, trigger, scope, outputs, nil)
	if err != nil {
		return nil, nil, nil, fmt.Errorf("child Action trigger: %w", err)
	}
	childScope, err := resolveObject(definition, call.ScopeSchema, nil, call.ScopeMap, inputs, trigger, scope, outputs, nil)
	if err != nil {
		return nil, nil, nil, fmt.Errorf("child Action scope: %w", err)
	}
	return childInputs, childTrigger, childScope, nil
}

// ResolveCallActionResult validates the exact child result contract and maps
// it into the parent call node's ordinary structured output.
func ResolveCallActionResult(
	definition contracts.WorkflowDefinition,
	nodeID string,
	childResult map[string]any,
) (map[string]any, error) {
	node, ok := workflowNode(definition, nodeID)
	if !ok || node.CallAction == nil {
		return nil, fmt.Errorf("workflow node %q is not a child Action call", nodeID)
	}
	call := *node.CallAction
	if err := call.ResultSchema.ValidateValue(childResult); err != nil {
		return nil, fmt.Errorf("child Action result: %w", err)
	}
	bindings := make([]contracts.ValueBinding, len(call.ResultMap))
	for index, mapping := range call.ResultMap {
		segments, err := pointerSegments(mapping.Source)
		if err != nil {
			return nil, fmt.Errorf("child resultMap source %q: %w", mapping.Source, err)
		}
		bindings[index] = contracts.ValueBinding{
			Target: mapping.Target,
			Value:  contracts.ValueExpr{Ref: "inputs." + strings.Join(segments, ".")},
		}
	}
	childDefinition := contracts.WorkflowDefinition{
		InputSchema:   call.ResultSchema,
		TriggerSchema: contracts.Schema{Type: contracts.TypeObject, Properties: map[string]contracts.Schema{}},
		ScopeSchema:   contracts.Schema{Type: contracts.TypeObject, Properties: map[string]contracts.Schema{}},
		Nodes:         []contracts.WorkflowNodeDefinition{},
	}
	return resolveObject(childDefinition, node.OutputSchema, nil, bindings, childResult, map[string]any{}, map[string]any{}, map[string]map[string]any{}, nil)
}

// ResolveResult materializes the public result for one Action entrypoint.
func ResolveResult(
	definition contracts.WorkflowDefinition,
	entrypoint string,
	inputs map[string]any,
	trigger map[string]any,
	scope map[string]any,
	outputs map[string]map[string]any,
) (map[string]any, error) {
	if _, exists := definition.Entrypoints[entrypoint]; !exists {
		return nil, fmt.Errorf("workflow entrypoint %q does not exist", entrypoint)
	}
	return resolveObject(definition, definition.ResultSchema, nil, definition.ResultBindings[entrypoint], inputs, trigger, scope, outputs, nil)
}

func resolveObject(
	definition contracts.WorkflowDefinition,
	schema contracts.Schema,
	fixed map[string]any,
	bindings []contracts.ValueBinding,
	inputs map[string]any,
	trigger map[string]any,
	scope map[string]any,
	outputs map[string]map[string]any,
	secrets map[string]contracts.SecretHandle,
) (map[string]any, error) {
	resolved, _, err := schema.ApplyDefaults(fixed)
	if err != nil {
		return nil, err
	}
	nodeSchemas := make(map[string]contracts.Schema, len(definition.Nodes))
	visible := make(map[string]bool, len(outputs))
	values := make(map[string]any, len(outputs))
	for _, node := range definition.Nodes {
		nodeSchemas[node.NodeID] = node.OutputSchema
	}
	for nodeID, output := range outputs {
		visible[nodeID] = true
		values[nodeID] = output
	}
	environment := expression.Environment{
		Inputs: definition.InputSchema, Trigger: definition.TriggerSchema, Scope: definition.ScopeSchema,
		NodeOutputs: nodeSchemas, VisibleNodes: visible, Secrets: make(map[string]bool, len(definition.Secrets)),
	}
	for _, name := range definition.Secrets {
		environment.Secrets[name] = true
	}
	valueSet := expression.Values{Inputs: inputs, Trigger: trigger, Scope: scope, NodeOutputs: values, Secrets: secrets}
	for index, binding := range bindings {
		value, evaluateErr := expression.Evaluate(binding.Value, environment, valueSet)
		if evaluateErr != nil {
			return nil, fmt.Errorf("binding %d target %s: %w", index, binding.Target, evaluateErr)
		}
		if handle, ok := value.(contracts.SecretHandle); ok {
			value = handle.ID
		}
		segments, pointerErr := pointerSegments(binding.Target)
		if pointerErr != nil {
			return nil, pointerErr
		}
		if err := setBoundValue(resolved, segments, value); err != nil {
			return nil, fmt.Errorf("binding %d target %s: %w", index, binding.Target, err)
		}
	}
	if err := schema.ValidateValue(resolved); err != nil {
		return nil, fmt.Errorf("resolved object: %w", err)
	}
	return resolved, nil
}

func workflowNode(definition contracts.WorkflowDefinition, nodeID string) (contracts.WorkflowNodeDefinition, bool) {
	for _, node := range definition.Nodes {
		if node.NodeID == nodeID {
			return node, true
		}
	}
	return contracts.WorkflowNodeDefinition{}, false
}

func setBoundValue(root map[string]any, segments []string, value any) error {
	if len(segments) == 0 {
		return errors.New("binding target cannot be the root")
	}
	updated, err := setPath(root, segments, value)
	if err != nil {
		return err
	}
	_, ok := updated.(map[string]any)
	if !ok {
		return errors.New("binding replaced the root object")
	}
	return nil
}

func setPath(current any, segments []string, value any) (any, error) {
	if len(segments) == 0 {
		return value, nil
	}
	segment := segments[0]
	switch typed := current.(type) {
	case map[string]any:
		child, exists := typed[segment]
		if !exists {
			if _, indexErr := strconv.ParseUint(nextSegment(segments), 10, 31); indexErr == nil && len(segments) > 1 {
				child = []any{}
			} else {
				child = map[string]any{}
			}
		}
		updated, err := setPath(child, segments[1:], value)
		if err != nil {
			return nil, err
		}
		typed[segment] = updated
		return typed, nil
	case []any:
		index, err := strconv.ParseUint(segment, 10, 31)
		if err != nil {
			return nil, fmt.Errorf("array segment %q is not an index", segment)
		}
		for len(typed) <= int(index) {
			typed = append(typed, nil)
		}
		child := typed[index]
		if child == nil && len(segments) > 1 {
			if _, indexErr := strconv.ParseUint(nextSegment(segments), 10, 31); indexErr == nil {
				child = []any{}
			} else {
				child = map[string]any{}
			}
		}
		updated, setErr := setPath(child, segments[1:], value)
		if setErr != nil {
			return nil, setErr
		}
		typed[index] = updated
		return typed, nil
	default:
		return nil, fmt.Errorf("binding crosses scalar %T at %q", current, segment)
	}
}

func nextSegment(segments []string) string {
	if len(segments) < 2 {
		return ""
	}
	return segments[1]
}
