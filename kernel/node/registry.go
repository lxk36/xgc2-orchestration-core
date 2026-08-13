package node

import (
	"context"
	"errors"
	"sort"
	"sync"

	"github.com/lxk36/xgc2-orchestration-core/kernel/canonicaljson"
	"github.com/lxk36/xgc2-orchestration-core/sdk/go/contracts"
	nodesdk "github.com/lxk36/xgc2-orchestration-core/sdk/go/node"
)

var (
	ErrRegistrySealed   = errors.New("node registry is sealed")
	ErrNodeConflict     = errors.New("node type registration conflict")
	ErrNodeNotFound     = errors.New("node type is not registered")
	ErrNodeNotResumable = errors.New("node type does not implement pure wait resumption")
)

type registration struct {
	descriptor contracts.NodeDescriptor
	executor   nodesdk.Executor
}

type Registry struct {
	mu            sync.RWMutex
	registrations map[string]registration
	sealed        bool
	catalogDigest string
}

func NewRegistry() *Registry {
	return &Registry{registrations: make(map[string]registration)}
}

func (registry *Registry) Register(executor nodesdk.Executor) error {
	registry.mu.RLock()
	sealed := registry.sealed
	registry.mu.RUnlock()
	if sealed {
		return ErrRegistrySealed
	}
	if executor == nil {
		return errors.New("node executor is required")
	}
	descriptor, err := cloneDescriptor(executor.Descriptor())
	if err != nil {
		return err
	}
	if err := ValidateDescriptor(descriptor); err != nil {
		return err
	}
	registry.mu.Lock()
	defer registry.mu.Unlock()
	if registry.sealed {
		return ErrRegistrySealed
	}
	if existing, exists := registry.registrations[descriptor.TypeRef]; exists {
		if existing.descriptor.DescriptorDigest == descriptor.DescriptorDigest && existing.descriptor.PackageDigest == descriptor.PackageDigest {
			return nil
		}
		return ErrNodeConflict
	}
	registry.registrations[descriptor.TypeRef] = registration{descriptor: descriptor, executor: executor}
	return nil
}

func (registry *Registry) Seal() (string, error) {
	registry.mu.Lock()
	defer registry.mu.Unlock()
	if registry.sealed {
		return registry.catalogDigest, nil
	}
	descriptors := make([]contracts.NodeDescriptor, 0, len(registry.registrations))
	for _, registered := range registry.registrations {
		descriptors = append(descriptors, registered.descriptor)
	}
	sort.Slice(descriptors, func(left, right int) bool { return descriptors[left].TypeRef < descriptors[right].TypeRef })
	digest, err := canonicaljson.DigestValue(descriptors)
	if err != nil {
		return "", err
	}
	registry.catalogDigest = digest
	registry.sealed = true
	return digest, nil
}

func (registry *Registry) Catalog() ([]contracts.NodeDescriptor, string, error) {
	registry.mu.RLock()
	defer registry.mu.RUnlock()
	if !registry.sealed {
		return nil, "", errors.New("node registry must be sealed before publication")
	}
	result := make([]contracts.NodeDescriptor, 0, len(registry.registrations))
	for _, registered := range registry.registrations {
		descriptor, err := cloneDescriptor(registered.descriptor)
		if err != nil {
			return nil, "", err
		}
		result = append(result, descriptor)
	}
	sort.Slice(result, func(left, right int) bool { return result[left].TypeRef < result[right].TypeRef })
	return result, registry.catalogDigest, nil
}

func (registry *Registry) Execute(ctx context.Context, request contracts.NodeInvocationRequest) (contracts.NodeResult, error) {
	if ctx == nil {
		return contracts.NodeResult{}, errors.New("node execution context is required")
	}
	registry.mu.RLock()
	if !registry.sealed {
		registry.mu.RUnlock()
		return contracts.NodeResult{}, errors.New("node registry must be sealed before execution")
	}
	registered, exists := registry.registrations[request.TypeRef]
	registry.mu.RUnlock()
	if !exists {
		return contracts.NodeResult{}, ErrNodeNotFound
	}
	if err := ValidateRequest(registered.descriptor, request); err != nil {
		return contracts.NodeResult{}, err
	}
	result, err := registered.executor.Execute(ctx, request)
	if err != nil {
		return contracts.NodeResult{}, err
	}
	if err := ValidateResult(registered.descriptor, request, result); err != nil {
		return contracts.NodeResult{}, err
	}
	return result, nil
}

func (registry *Registry) Resume(ctx context.Context, request contracts.NodeResumeRequest) (contracts.NodeResult, error) {
	if ctx == nil {
		return contracts.NodeResult{}, errors.New("node resume context is required")
	}
	registry.mu.RLock()
	if !registry.sealed {
		registry.mu.RUnlock()
		return contracts.NodeResult{}, errors.New("node registry must be sealed before resumption")
	}
	registered, exists := registry.registrations[request.TypeRef]
	registry.mu.RUnlock()
	if !exists {
		return contracts.NodeResult{}, ErrNodeNotFound
	}
	resumer, ok := registered.executor.(nodesdk.Resumer)
	if !ok {
		return contracts.NodeResult{}, ErrNodeNotResumable
	}
	if err := ValidateResumeRequest(registered.descriptor, request); err != nil {
		return contracts.NodeResult{}, err
	}
	result, err := resumer.Resume(ctx, request)
	if err != nil {
		return contracts.NodeResult{}, err
	}
	if err := ValidateResumeResult(registered.descriptor, result); err != nil {
		return contracts.NodeResult{}, err
	}
	return result, nil
}

func cloneDescriptor(descriptor contracts.NodeDescriptor) (contracts.NodeDescriptor, error) {
	raw, err := canonicaljson.Marshal(descriptor)
	if err != nil {
		return contracts.NodeDescriptor{}, err
	}
	var clone contracts.NodeDescriptor
	if err := canonicaljson.UnmarshalStrict(raw, &clone); err != nil {
		return contracts.NodeDescriptor{}, err
	}
	return clone, nil
}
