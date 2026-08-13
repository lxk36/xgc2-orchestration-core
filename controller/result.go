package controller

import (
	"github.com/lxk36/xgc2-orchestration-core/kernel/canonicaljson"
	workflowkernel "github.com/lxk36/xgc2-orchestration-core/kernel/workflow"
)

func workflowResult(snapshot RunSnapshot) (map[string]any, error) {
	return workflowkernel.ResolveResult(
		snapshot.Definition, snapshot.Entrypoint, snapshot.Inputs, snapshot.Trigger, snapshot.Scope, snapshot.NodeOutputs,
	)
}

func digestResult(result map[string]any) (string, error) {
	return canonicaljson.DigestValue(result)
}
