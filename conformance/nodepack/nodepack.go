// Package nodepack is the reusable acceptance suite for independently
// published Go node packs.
package nodepack

import (
	"context"
	"errors"
	"fmt"

	"github.com/lxk36/xgc2-orchestration-core/kernel/canonicaljson"
	kernelnode "github.com/lxk36/xgc2-orchestration-core/kernel/node"
	"github.com/lxk36/xgc2-orchestration-core/sdk/go/contracts"
	nodesdk "github.com/lxk36/xgc2-orchestration-core/sdk/go/node"
)

type Case struct {
	Name           string
	Executor       nodesdk.Executor
	Request        contracts.NodeInvocationRequest
	ExpectedStatus contracts.NodeResultStatus
}

type Suite struct {
	PackageRef string
	Executors  []nodesdk.Executor
	Cases      []Case
}

type Report struct {
	CatalogDigest   string
	DescriptorCount int
	CaseCount       int
}

func Validate(ctx context.Context, suite Suite) (Report, error) {
	if ctx == nil || !contracts.ValidIdentifier(suite.PackageRef) || len(suite.Executors) == 0 || len(suite.Cases) == 0 {
		return Report{}, errors.New("node pack conformance context, package, executors, and cases are required")
	}
	registry := kernelnode.NewRegistry()
	executors := make(map[string]string, len(suite.Executors))
	for index, executor := range suite.Executors {
		if executor == nil {
			return Report{}, fmt.Errorf("executor %d is nil", index)
		}
		descriptor := executor.Descriptor()
		if descriptor.PackageRef != suite.PackageRef {
			return Report{}, fmt.Errorf("executor %s belongs to package %s", descriptor.TypeRef, descriptor.PackageRef)
		}
		if err := registry.Register(executor); err != nil {
			return Report{}, fmt.Errorf("executor %s: %w", descriptor.TypeRef, err)
		}
		executors[descriptor.TypeRef] = descriptor.DescriptorDigest
	}
	catalogDigest, err := registry.Seal()
	if err != nil {
		return Report{}, err
	}
	covered := make(map[string]bool, len(suite.Cases))
	for index, testCase := range suite.Cases {
		if testCase.Name == "" || testCase.Executor == nil {
			return Report{}, fmt.Errorf("case %d name or executor is missing", index)
		}
		descriptor := testCase.Executor.Descriptor()
		registeredDigest, exists := executors[descriptor.TypeRef]
		if !exists || registeredDigest != descriptor.DescriptorDigest {
			return Report{}, fmt.Errorf("case %s executor is not the registered instance", testCase.Name)
		}
		result, err := registry.Execute(ctx, testCase.Request)
		if err != nil {
			return Report{}, fmt.Errorf("case %s: %w", testCase.Name, err)
		}
		if result.Status != testCase.ExpectedStatus {
			return Report{}, fmt.Errorf("case %s status = %s, want %s", testCase.Name, result.Status, testCase.ExpectedStatus)
		}
		if descriptor.Determinism == contracts.NodeDeterministic {
			replayed, err := registry.Execute(ctx, testCase.Request)
			if err != nil {
				return Report{}, fmt.Errorf("case %s deterministic replay: %w", testCase.Name, err)
			}
			left, leftErr := canonicaljson.Marshal(result)
			right, rightErr := canonicaljson.Marshal(replayed)
			if leftErr != nil || rightErr != nil || string(left) != string(right) {
				return Report{}, fmt.Errorf("case %s is not byte-deterministic", testCase.Name)
			}
		}
		covered[descriptor.TypeRef] = true
	}
	for typeRef := range executors {
		if !covered[typeRef] {
			return Report{}, fmt.Errorf("node type %s has no conformance case", typeRef)
		}
	}
	return Report{CatalogDigest: catalogDigest, DescriptorCount: len(executors), CaseCount: len(suite.Cases)}, nil
}
