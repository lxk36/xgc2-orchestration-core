// Package node is the only in-process extension API implemented by Go node
// packs. Executors receive immutable data and opaque grant references. They do
// not receive stores, providers, clocks, or a mutable orchestration context.
package node

import (
	"context"

	"github.com/lxk36/xgc2-orchestration-core/sdk/go/contracts"
)

type Executor interface {
	Descriptor() contracts.NodeDescriptor
	Execute(context.Context, contracts.NodeInvocationRequest) (contracts.NodeResult, error)
}

type ExecutorFunc struct {
	NodeDescriptor contracts.NodeDescriptor
	Function       func(context.Context, contracts.NodeInvocationRequest) (contracts.NodeResult, error)
}

func (executor ExecutorFunc) Descriptor() contracts.NodeDescriptor { return executor.NodeDescriptor }

func (executor ExecutorFunc) Execute(ctx context.Context, request contracts.NodeInvocationRequest) (contracts.NodeResult, error) {
	return executor.Function(ctx, request)
}
