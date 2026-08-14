package controller

import (
	"errors"
	"fmt"
	"time"

	"github.com/lxk36/xgc2-orchestration-core/sdk/go/contracts"
)

type nodeDisposition uint8

const (
	nodeBlocked nodeDisposition = iota
	nodeReady
	nodeSkipped
)

// currentNodeDisposition resolves the AND-join for the current stable
// topological position. Every in-scope incoming edge must have resolved and
// matched. A successful source can match more than one outgoing edge, which is
// a deterministic logical fork even though this controller executes ready
// nodes one at a time.
func currentNodeDisposition(snapshot RunSnapshot) (nodeDisposition, error) {
	if snapshot.NextNode < 0 || snapshot.NextNode >= len(snapshot.NodeOrder) {
		return nodeBlocked, errors.New("current node position is outside the execution plan")
	}
	nodeID := snapshot.NodeOrder[snapshot.NextNode]
	active := make(map[string]bool, len(snapshot.NodeOrder))
	for _, candidate := range snapshot.NodeOrder {
		active[candidate] = true
	}
	incoming := 0
	for _, edge := range snapshot.Plan.Edges {
		if edge.To != nodeID || !active[edge.From] {
			continue
		}
		incoming++
		outcome, resolved := snapshot.NodeOutcomes[edge.From]
		if !resolved {
			return nodeBlocked, nil
		}
		if !edgeMatches(edge, outcome) {
			return nodeSkipped, nil
		}
	}
	if incoming == 0 {
		return nodeReady, nil
	}
	return nodeReady, nil
}

func edgeMatches(edge contracts.WorkflowEdge, outcome NodeOutcome) bool {
	matched := false
	switch edge.Condition {
	case contracts.EdgeSuccess:
		matched = outcome.Status == contracts.InvocationSucceeded
	case contracts.EdgeFailure:
		matched = outcome.Status == contracts.InvocationFailed
	case contracts.EdgeAlways:
		matched = outcome.Status == contracts.InvocationSucceeded || outcome.Status == contracts.InvocationFailed
	}
	if !matched {
		return false
	}
	if edge.Route != "" && outcome.Route != edge.Route {
		return false
	}
	if edge.SourcePort != "" && !outcomePublishesPort(outcome, edge.SourcePort) {
		return false
	}
	return true
}

func outcomePublishesPort(outcome NodeOutcome, port string) bool {
	if outcome.Status != contracts.InvocationSucceeded {
		return false
	}
	if port == "main" || port == outcome.Route {
		return true
	}
	for _, published := range outcome.SourcePorts {
		if published == port {
			return true
		}
	}
	return false
}

func failureHandled(snapshot RunSnapshot, nodeID string, outcome NodeOutcome) bool {
	if outcome.Status != contracts.InvocationFailed {
		return false
	}
	active := make(map[string]bool, len(snapshot.NodeOrder))
	for _, candidate := range snapshot.NodeOrder {
		active[candidate] = true
	}
	for _, edge := range snapshot.Plan.Edges {
		if edge.From == nodeID && active[edge.To] && edgeMatches(edge, outcome) {
			return true
		}
	}
	return false
}

func recordSucceededNode(snapshot *RunSnapshot, nodeID string, result contracts.NodeResult) error {
	if err := requireCurrentNode(*snapshot, nodeID); err != nil {
		return err
	}
	snapshot.NodeOutputs[nodeID] = result.Output
	snapshot.NodeOutcomes[nodeID] = NodeOutcome{
		Status: contracts.InvocationSucceeded, OutputDigest: result.OutputDigest,
		Route: result.Route, SourcePorts: append([]string(nil), result.SourcePorts...),
	}
	snapshot.NextNode++
	snapshot.Waiting = nil
	snapshot.ActionCall = nil
	snapshot.RetryWait = nil
	return nil
}

func recordFailedNode(snapshot *RunSnapshot, nodeID string, failure contracts.StructuredFailure) (bool, error) {
	if err := requireCurrentNode(*snapshot, nodeID); err != nil {
		return false, err
	}
	outcome := NodeOutcome{Status: contracts.InvocationFailed, Failure: &failure}
	snapshot.NodeOutcomes[nodeID] = outcome
	snapshot.NextNode++
	snapshot.Waiting = nil
	snapshot.ActionCall = nil
	snapshot.RetryWait = nil
	return failureHandled(*snapshot, nodeID, outcome), nil
}

func recordSkippedNode(snapshot *RunSnapshot, nodeID string) error {
	if err := requireCurrentNode(*snapshot, nodeID); err != nil {
		return err
	}
	snapshot.NodeOutcomes[nodeID] = NodeOutcome{Status: contracts.InvocationSkipped}
	snapshot.NextNode++
	return nil
}

func requireCurrentNode(snapshot RunSnapshot, nodeID string) error {
	if snapshot.NextNode < 0 || snapshot.NextNode >= len(snapshot.NodeOrder) || snapshot.NodeOrder[snapshot.NextNode] != nodeID {
		return fmt.Errorf("node %q is not the current workflow position", nodeID)
	}
	if _, terminal := snapshot.NodeOutcomes[nodeID]; terminal {
		return fmt.Errorf("node %q already has a terminal routing outcome", nodeID)
	}
	return nil
}

func retryPolicy(node contracts.WorkflowNodeDefinition) contracts.NodeRetryPolicy {
	if node.Retry == nil {
		return contracts.NodeRetryPolicy{MaxAttempts: 1}
	}
	return *node.Retry
}

func retryDelay(policy contracts.NodeRetryPolicy, failedAttempt uint32) (time.Duration, bool) {
	if failedAttempt == 0 || failedAttempt >= policy.MaxAttempts || policy.InitialBackoffMillis == 0 {
		return 0, false
	}
	delay := policy.InitialBackoffMillis
	for ordinal := uint32(1); ordinal < failedAttempt && delay < policy.MaxBackoffMillis; ordinal++ {
		if delay > policy.MaxBackoffMillis/2 {
			delay = policy.MaxBackoffMillis
			break
		}
		delay *= 2
	}
	if delay > policy.MaxBackoffMillis {
		delay = policy.MaxBackoffMillis
	}
	return time.Duration(delay) * time.Millisecond, true
}
