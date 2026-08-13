// Package workflow compiles product-neutral Workflow definitions into a
// deterministic plan after all graph, binding, and child-input checks pass.
package workflow

import (
	"errors"
	"fmt"
	"sort"
	"strings"

	"github.com/lxk36/xgc2-orchestration-core/kernel/canonicaljson"
	"github.com/lxk36/xgc2-orchestration-core/kernel/expression"
	"github.com/lxk36/xgc2-orchestration-core/sdk/go/contracts"
)

const SchemaVersion = "xgc.workflow/v1"

func Compile(definition contracts.WorkflowDefinition) (contracts.CompiledWorkflowPlan, error) {
	if err := validateIdentityAndSchemas(definition); err != nil {
		return contracts.CompiledWorkflowPlan{}, err
	}
	nodes, err := indexNodes(definition.Nodes)
	if err != nil {
		return contracts.CompiledWorkflowPlan{}, err
	}
	graph, dataEdges, controlEdges, err := buildGraph(definition, nodes)
	if err != nil {
		return contracts.CompiledWorkflowPlan{}, err
	}
	entries, err := validateEntrypoints(definition.Entrypoints, nodes)
	if err != nil {
		return contracts.CompiledWorkflowPlan{}, err
	}
	order, err := topologicalOrder(graph)
	if err != nil {
		return contracts.CompiledWorkflowPlan{}, err
	}
	if err := validateReachability(graph, entries, nodes); err != nil {
		return contracts.CompiledWorkflowPlan{}, err
	}
	baseEnvironment := expression.Environment{
		Inputs: definition.InputSchema, Trigger: definition.TriggerSchema, Scope: definition.ScopeSchema,
		NodeOutputs: make(map[string]contracts.Schema, len(nodes)), Secrets: make(map[string]bool, len(definition.Secrets)),
	}
	for _, name := range definition.Secrets {
		baseEnvironment.Secrets[name] = true
	}
	for nodeID, node := range nodes {
		baseEnvironment.NodeOutputs[nodeID] = node.OutputSchema
	}
	for _, nodeID := range order {
		node := nodes[nodeID]
		environment := baseEnvironment
		environment.VisibleNodes = visiblePredecessors(nodeID, dataEdges, controlEdges, graph, entries)
		if err := validateInputAssembly(node.InputSchema, node.FixedInputs, node.Bindings, environment, "node "+nodeID); err != nil {
			return contracts.CompiledWorkflowPlan{}, err
		}
		if node.CallAction != nil {
			if err := validateCallAction(*node.CallAction, node.OutputSchema, environment, nodeID); err != nil {
				return contracts.CompiledWorkflowPlan{}, err
			}
		}
	}
	for name := range definition.ResultBindings {
		if _, exists := definition.Entrypoints[name]; !exists {
			return contracts.CompiledWorkflowPlan{}, fmt.Errorf("workflow result binding targets unknown entrypoint %q", name)
		}
	}
	for name, entryNodeID := range definition.Entrypoints {
		resultEnvironment := baseEnvironment
		resultEnvironment.VisibleNodes = resultVisibleNodes(graph, []string{entryNodeID})
		if err := validateInputAssembly(definition.ResultSchema, nil, definition.ResultBindings[name], resultEnvironment, "workflow result "+name); err != nil {
			return contracts.CompiledWorkflowPlan{}, err
		}
	}
	definitionDigest, err := canonicaljson.DigestValue(definition)
	if err != nil {
		return contracts.CompiledWorkflowPlan{}, fmt.Errorf("definition digest: %w", err)
	}
	entrypointOrders := make(map[string][]string, len(definition.Entrypoints))
	for name, nodeID := range definition.Entrypoints {
		reachableNodes := reachable(graph, []string{nodeID}, "")
		entryOrder := make([]string, 0, len(reachableNodes))
		for _, candidate := range order {
			if reachableNodes[candidate] {
				entryOrder = append(entryOrder, candidate)
			}
		}
		entrypointOrders[name] = entryOrder
	}
	unsigned := struct {
		WorkflowID          string              `json:"workflowId"`
		Version             string              `json:"version"`
		DefinitionDigest    string              `json:"definitionDigest"`
		NodeOrder           []string            `json:"nodeOrder"`
		EntrypointNodeOrder map[string][]string `json:"entrypointNodeOrder"`
	}{definition.WorkflowID, definition.Version, definitionDigest, order, entrypointOrders}
	planDigest, err := canonicaljson.DigestValue(unsigned)
	if err != nil {
		return contracts.CompiledWorkflowPlan{}, fmt.Errorf("plan digest: %w", err)
	}
	return contracts.CompiledWorkflowPlan{
		WorkflowID: definition.WorkflowID, Version: definition.Version, DefinitionDigest: definitionDigest,
		NodeOrder: order, EntrypointNodeOrder: entrypointOrders, PlanDigest: planDigest,
	}, nil
}

func resultVisibleNodes(graph dependencyGraph, entries []string) map[string]bool {
	reached := reachable(graph, entries, "")
	terminals := make([]string, 0)
	for nodeID, successors := range graph {
		if reached[nodeID] && len(successors) == 0 {
			terminals = append(terminals, nodeID)
		}
	}
	visible := make(map[string]bool)
	for candidate := range graph {
		if !reached[candidate] {
			continue
		}
		dominatesEveryTerminal := true
		for _, terminal := range terminals {
			if candidate != terminal && !dominates(candidate, terminal, graph, entries) {
				dominatesEveryTerminal = false
				break
			}
		}
		visible[candidate] = dominatesEveryTerminal
	}
	return visible
}

func validateIdentityAndSchemas(definition contracts.WorkflowDefinition) error {
	if definition.SchemaVersion != SchemaVersion {
		return fmt.Errorf("schemaVersion must be %q", SchemaVersion)
	}
	if !contracts.ValidIdentifier(definition.WorkflowID) || !contracts.ValidIdentifier(definition.Version) {
		return errors.New("workflow identity and version must be portable identifiers")
	}
	seenSecrets := make(map[string]bool, len(definition.Secrets))
	for _, name := range definition.Secrets {
		if !contracts.ValidIdentifier(name) {
			return fmt.Errorf("workflow secret name %q is invalid", name)
		}
		if seenSecrets[name] {
			return fmt.Errorf("workflow secret name %q is duplicated", name)
		}
		seenSecrets[name] = true
	}
	for name, schema := range map[string]contracts.Schema{
		"input": definition.InputSchema, "result": definition.ResultSchema,
		"trigger": definition.TriggerSchema, "scope": definition.ScopeSchema,
	} {
		if err := schema.ValidateDefinition(); err != nil {
			return fmt.Errorf("workflow %s schema: %w", name, err)
		}
		if schema.ContainsFormat(contracts.FormatSecretHandle) {
			return fmt.Errorf("workflow %s schema cannot expose secret handles", name)
		}
	}
	if definition.InputSchema.Type != contracts.TypeObject || definition.ResultSchema.Type != contracts.TypeObject ||
		definition.TriggerSchema.Type != contracts.TypeObject || definition.ScopeSchema.Type != contracts.TypeObject {
		return errors.New("workflow input, result, trigger, and scope schemas must be objects")
	}
	return nil
}

func indexNodes(definitions []contracts.WorkflowNodeDefinition) (map[string]contracts.WorkflowNodeDefinition, error) {
	if len(definitions) == 0 {
		return nil, errors.New("workflow must contain at least one node")
	}
	nodes := make(map[string]contracts.WorkflowNodeDefinition, len(definitions))
	for _, node := range definitions {
		if !contracts.ValidIdentifier(node.NodeID) || !contracts.ValidTypeRef(node.TypeRef) {
			return nil, fmt.Errorf("node identity or typeRef %q/%q is invalid", node.NodeID, node.TypeRef)
		}
		if !contracts.ValidDigest(node.DescriptorDigest) {
			return nil, fmt.Errorf("node %q descriptor digest is invalid", node.NodeID)
		}
		if _, duplicate := nodes[node.NodeID]; duplicate {
			return nil, fmt.Errorf("node %q is duplicated", node.NodeID)
		}
		if node.InputSchema.Type != contracts.TypeObject || node.OutputSchema.Type != contracts.TypeObject {
			return nil, fmt.Errorf("node %q input and output schemas must be objects", node.NodeID)
		}
		if err := node.InputSchema.ValidateDefinition(); err != nil {
			return nil, fmt.Errorf("node %q input schema: %w", node.NodeID, err)
		}
		if err := node.OutputSchema.ValidateDefinition(); err != nil {
			return nil, fmt.Errorf("node %q output schema: %w", node.NodeID, err)
		}
		if node.OutputSchema.ContainsFormat(contracts.FormatSecretHandle) {
			return nil, fmt.Errorf("node %q output schema cannot expose secret handles", node.NodeID)
		}
		nodes[node.NodeID] = node
	}
	return nodes, nil
}

type dependencyGraph map[string]map[string]struct{}

func buildGraph(definition contracts.WorkflowDefinition, nodes map[string]contracts.WorkflowNodeDefinition) (dependencyGraph, map[string]map[string]bool, map[string]map[string]bool, error) {
	graph := make(dependencyGraph, len(nodes))
	dataEdges := make(map[string]map[string]bool)
	controlEdges := make(map[string]map[string]bool)
	for nodeID := range nodes {
		graph[nodeID] = make(map[string]struct{})
	}
	seen := make(map[string]struct{})
	for _, edge := range definition.Edges {
		if !edge.Kind.Valid() {
			return nil, nil, nil, fmt.Errorf("edge %s -> %s has invalid kind %q", edge.From, edge.To, edge.Kind)
		}
		if _, exists := nodes[edge.From]; !exists {
			return nil, nil, nil, fmt.Errorf("edge source %q does not exist", edge.From)
		}
		if _, exists := nodes[edge.To]; !exists {
			return nil, nil, nil, fmt.Errorf("edge target %q does not exist", edge.To)
		}
		if edge.From == edge.To {
			return nil, nil, nil, fmt.Errorf("node %q cannot depend on itself", edge.From)
		}
		identity := edge.From + "\x00" + edge.To + "\x00" + string(edge.Kind)
		if _, duplicate := seen[identity]; duplicate {
			return nil, nil, nil, fmt.Errorf("edge %s -> %s (%s) is duplicated", edge.From, edge.To, edge.Kind)
		}
		seen[identity] = struct{}{}
		graph[edge.From][edge.To] = struct{}{}
		if edge.Kind == contracts.EdgeData {
			if dataEdges[edge.To] == nil {
				dataEdges[edge.To] = make(map[string]bool)
			}
			dataEdges[edge.To][edge.From] = true
		} else {
			if controlEdges[edge.To] == nil {
				controlEdges[edge.To] = make(map[string]bool)
			}
			controlEdges[edge.To][edge.From] = true
		}
	}
	return graph, dataEdges, controlEdges, nil
}

func validateEntrypoints(entrypoints map[string]string, nodes map[string]contracts.WorkflowNodeDefinition) ([]string, error) {
	if len(entrypoints) == 0 {
		return nil, errors.New("workflow must declare at least one entrypoint")
	}
	entries := make([]string, 0, len(entrypoints))
	seenNodes := make(map[string]bool)
	for name, nodeID := range entrypoints {
		if !contracts.ValidIdentifier(name) {
			return nil, fmt.Errorf("entrypoint %q is invalid", name)
		}
		if _, exists := nodes[nodeID]; !exists {
			return nil, fmt.Errorf("entrypoint %q targets missing node %q", name, nodeID)
		}
		if !seenNodes[nodeID] {
			entries = append(entries, nodeID)
			seenNodes[nodeID] = true
		}
	}
	sort.Strings(entries)
	return entries, nil
}

func topologicalOrder(graph dependencyGraph) ([]string, error) {
	indegree := make(map[string]int, len(graph))
	for nodeID := range graph {
		indegree[nodeID] = 0
	}
	for _, successors := range graph {
		for successor := range successors {
			indegree[successor]++
		}
	}
	ready := make([]string, 0)
	for nodeID, degree := range indegree {
		if degree == 0 {
			ready = append(ready, nodeID)
		}
	}
	sort.Strings(ready)
	order := make([]string, 0, len(graph))
	for len(ready) != 0 {
		nodeID := ready[0]
		ready = ready[1:]
		order = append(order, nodeID)
		for successor := range graph[nodeID] {
			indegree[successor]--
			if indegree[successor] == 0 {
				ready = append(ready, successor)
				sort.Strings(ready)
			}
		}
	}
	if len(order) != len(graph) {
		return nil, errors.New("workflow dependency graph contains a cycle")
	}
	return order, nil
}

func validateReachability(graph dependencyGraph, entries []string, nodes map[string]contracts.WorkflowNodeDefinition) error {
	reached := reachable(graph, entries, "")
	missing := make([]string, 0)
	for nodeID := range nodes {
		if !reached[nodeID] {
			missing = append(missing, nodeID)
		}
	}
	if len(missing) != 0 {
		sort.Strings(missing)
		return fmt.Errorf("unreachable workflow nodes: %s", strings.Join(missing, ", "))
	}
	return nil
}

func reachable(graph dependencyGraph, starts []string, excluded string) map[string]bool {
	result := make(map[string]bool)
	queue := append([]string(nil), starts...)
	for len(queue) != 0 {
		nodeID := queue[0]
		queue = queue[1:]
		if nodeID == excluded || result[nodeID] {
			continue
		}
		result[nodeID] = true
		for successor := range graph[nodeID] {
			queue = append(queue, successor)
		}
	}
	return result
}

func visiblePredecessors(target string, dataEdges, controlEdges map[string]map[string]bool, graph dependencyGraph, entries []string) map[string]bool {
	// Data edges are conjunctive dependencies, not alternative execution paths.
	// Including any of them in reachability manufactures shortcuts around later
	// producers and even around downstream control barriers. Judge dominance on
	// the pure control projection while still requiring a direct data edge for
	// every referenced producer.
	dominanceGraph := make(dependencyGraph, len(graph))
	for nodeID := range graph {
		dominanceGraph[nodeID] = make(map[string]struct{})
	}
	for successor, predecessors := range controlEdges {
		for predecessor := range predecessors {
			dominanceGraph[predecessor][successor] = struct{}{}
		}
	}
	visible := make(map[string]bool)
	for source := range dataEdges[target] {
		if dominates(source, target, dominanceGraph, entries) {
			visible[source] = true
		}
	}
	return visible
}

func dominates(source, target string, graph dependencyGraph, entries []string) bool {
	if source == target {
		return true
	}
	return !reachable(graph, entries, source)[target]
}

func validateInputAssembly(schema contracts.Schema, fixed map[string]any, bindings []contracts.ValueBinding, environment expression.Environment, context string) error {
	provided := make(map[string]bool)
	if fixed == nil {
		fixed = map[string]any{}
	}
	if err := validateFixed(schema, fixed, "", provided); err != nil {
		return fmt.Errorf("%s fixed inputs: %w", context, err)
	}
	seenTargets := make(map[string]bool)
	for index, binding := range bindings {
		segments, err := pointerSegments(binding.Target)
		if err != nil {
			return fmt.Errorf("%s binding %d: %w", context, index, err)
		}
		if seenTargets[binding.Target] || covered(binding.Target, provided) {
			return fmt.Errorf("%s binding target %q is assigned more than once", context, binding.Target)
		}
		target, err := schema.Resolve(segments)
		if err != nil {
			return fmt.Errorf("%s binding target %q: %w", context, binding.Target, err)
		}
		if err := expression.CheckAssignable(binding.Value, environment, target); err != nil {
			return fmt.Errorf("%s binding target %q: %w", context, binding.Target, err)
		}
		seenTargets[binding.Target] = true
		provided[binding.Target] = true
	}
	defaults, defaultPaths, err := schema.ApplyDefaults(map[string]any{})
	_ = defaults
	if err != nil {
		return fmt.Errorf("%s defaults: %w", context, err)
	}
	for pointer := range defaultPaths {
		provided[pointer] = true
	}
	for _, required := range requiredPointers(schema, "") {
		if !covered(required, provided) {
			return fmt.Errorf("%s required input %q is not assigned", context, required)
		}
	}
	return nil
}

func validateFixed(schema contracts.Schema, value map[string]any, pointer string, provided map[string]bool) error {
	for name, child := range value {
		property, exists := schema.Properties[name]
		if !exists {
			return fmt.Errorf("unknown property %q", pointer+"/"+name)
		}
		childPointer := pointer + "/" + escapePointer(name)
		if property.Format == contracts.FormatSecretHandle {
			return fmt.Errorf("property %q is a secret slot and cannot have an authored fixed value", childPointer)
		}
		if object, ok := child.(map[string]any); ok && property.Type == contracts.TypeObject {
			if err := validateFixed(property, object, childPointer, provided); err != nil {
				return err
			}
			if len(object) == 0 {
				provided[childPointer] = true
			}
			continue
		}
		if err := property.ValidateValue(child); err != nil {
			return fmt.Errorf("property %q: %w", childPointer, err)
		}
		provided[childPointer] = true
	}
	return nil
}

func validateCallAction(call contracts.CallAction, nodeOutput contracts.Schema, environment expression.Environment, nodeID string) error {
	if !contracts.ValidIdentifier(call.TargetActionRef.ActionID) || !contracts.ValidIdentifier(call.TargetActionRef.Version) ||
		!contracts.ValidDigest(call.TargetActionRef.Digest) {
		return fmt.Errorf("node %q child action ref is invalid", nodeID)
	}
	if call.InputSchema.Type != contracts.TypeObject || call.TriggerSchema.Type != contracts.TypeObject ||
		call.ScopeSchema.Type != contracts.TypeObject || call.ResultSchema.Type != contracts.TypeObject {
		return fmt.Errorf("node %q child action input, trigger, scope, and result schemas must be objects", nodeID)
	}
	for _, field := range []struct {
		label  string
		schema contracts.Schema
	}{
		{label: "input", schema: call.InputSchema},
		{label: "trigger", schema: call.TriggerSchema},
		{label: "scope", schema: call.ScopeSchema},
		{label: "result", schema: call.ResultSchema},
	} {
		if err := field.schema.ValidateDefinition(); err != nil {
			return fmt.Errorf("node %q child %s schema: %w", nodeID, field.label, err)
		}
	}
	if call.TriggerSchema.ContainsFormat(contracts.FormatSecretHandle) ||
		call.ScopeSchema.ContainsFormat(contracts.FormatSecretHandle) ||
		call.ResultSchema.ContainsFormat(contracts.FormatSecretHandle) {
		return fmt.Errorf("node %q child trigger, scope, and result schemas cannot expose secret handles", nodeID)
	}
	if err := validateInputAssembly(call.InputSchema, nil, call.InputMap, environment, "node "+nodeID+" child inputMap"); err != nil {
		return err
	}
	if err := validateInputAssembly(call.TriggerSchema, nil, call.TriggerMap, environment, "node "+nodeID+" child triggerMap"); err != nil {
		return err
	}
	if err := validateInputAssembly(call.ScopeSchema, nil, call.ScopeMap, environment, "node "+nodeID+" child scopeMap"); err != nil {
		return err
	}
	resultBindings := make([]contracts.ValueBinding, len(call.ResultMap))
	for index, mapping := range call.ResultMap {
		segments, err := pointerSegments(mapping.Source)
		if err != nil {
			return fmt.Errorf("node %q child resultMap source %q: %w", nodeID, mapping.Source, err)
		}
		resultBindings[index] = contracts.ValueBinding{
			Target: mapping.Target,
			Value:  contracts.ValueExpr{Ref: "inputs." + strings.Join(segments, ".")},
		}
	}
	resultEnvironment := expression.Environment{
		Inputs: call.ResultSchema, Trigger: contracts.Schema{Type: contracts.TypeObject}, Scope: contracts.Schema{Type: contracts.TypeObject},
	}
	if err := validateInputAssembly(nodeOutput, nil, resultBindings, resultEnvironment, "node "+nodeID+" child resultMap"); err != nil {
		return err
	}
	return nil
}

func pointerSegments(pointer string) ([]string, error) {
	if pointer == "" || pointer == "/" || !strings.HasPrefix(pointer, "/") {
		return nil, errors.New("binding target must be a non-root JSON Pointer")
	}
	rawSegments := strings.Split(pointer[1:], "/")
	segments := make([]string, len(rawSegments))
	for index, raw := range rawSegments {
		if raw == "" {
			return nil, errors.New("binding target contains an empty path segment")
		}
		var decoded strings.Builder
		for position := 0; position < len(raw); position++ {
			if raw[position] != '~' {
				decoded.WriteByte(raw[position])
				continue
			}
			if position+1 == len(raw) {
				return nil, errors.New("binding target contains an invalid JSON Pointer escape")
			}
			position++
			switch raw[position] {
			case '0':
				decoded.WriteByte('~')
			case '1':
				decoded.WriteByte('/')
			default:
				return nil, errors.New("binding target contains an invalid JSON Pointer escape")
			}
		}
		segments[index] = decoded.String()
	}
	return segments, nil
}

func requiredPointers(schema contracts.Schema, pointer string) []string {
	result := make([]string, 0)
	for _, name := range schema.Required {
		property := schema.Properties[name]
		childPointer := pointer + "/" + escapePointer(name)
		children := requiredPointers(property, childPointer)
		if property.Type != contracts.TypeObject || len(children) == 0 {
			result = append(result, childPointer)
		} else {
			result = append(result, children...)
		}
	}
	return result
}

func covered(pointer string, provided map[string]bool) bool {
	for assigned := range provided {
		if assigned == pointer || strings.HasPrefix(pointer, assigned+"/") || strings.HasPrefix(assigned, pointer+"/") {
			return true
		}
	}
	return false
}

func escapePointer(value string) string {
	return strings.ReplaceAll(strings.ReplaceAll(value, "~", "~0"), "/", "~1")
}
