// Package processadapter converts public process Effect intents into private
// process-provider dispatches. Reference resolution is an explicit host port;
// node packs never receive executable paths, arguments, environment values,
// working directories, log paths, or capability tokens.
package processadapter

import (
	"context"
	"errors"
	"time"

	"github.com/lxk36/xgc2-orchestration-core/kernel/canonicaljson"
	effectport "github.com/lxk36/xgc2-orchestration-core/provider/effect"
	processport "github.com/lxk36/xgc2-orchestration-core/provider/process"
	"github.com/lxk36/xgc2-orchestration-core/sdk/go/contracts"
)

const (
	KindStart = "xgc.process-start/v1"
	KindStop  = "xgc.process-stop/v1"
)

type Intent struct {
	Spec          contracts.ProcessSpec      `json:"spec"`
	KnownIdentity *contracts.ProcessIdentity `json:"knownIdentity,omitempty"`
}

type Resolution struct {
	Executable       string
	Arguments        []string
	Environment      []string
	WorkingDirectory string
	StdoutPath       string
	StderrPath       string
}

type Resolver interface {
	ResolveProcess(context.Context, contracts.EffectIntent, Intent) (Resolution, error)
}

type Clock interface{ Now() time.Time }

type wallClock struct{}

func (wallClock) Now() time.Time { return time.Now().UTC() }

type Config struct {
	Kind           string
	ProviderRef    string
	ProviderDigest string
	Provider       processport.Provider
	Resolver       Resolver
	Clock          Clock
}

type Adapter struct {
	descriptor effectport.AdapterDescriptor
	provider   processport.Provider
	resolver   Resolver
	clock      Clock
}

func New(config Config) (*Adapter, error) {
	if config.Kind != KindStart && config.Kind != KindStop {
		return nil, errors.New("process adapter kind is invalid")
	}
	if !contracts.ValidIdentifier(config.ProviderRef) || !contracts.ValidDigest(config.ProviderDigest) || config.Provider == nil || config.Resolver == nil {
		return nil, errors.New("process adapter provider identity, provider, or resolver is invalid")
	}
	if config.Clock == nil {
		config.Clock = wallClock{}
	}
	return &Adapter{
		descriptor: effectport.AdapterDescriptor{
			Kind: config.Kind, ProviderRef: config.ProviderRef, ProviderDigest: config.ProviderDigest,
		},
		provider: config.Provider, resolver: config.Resolver, clock: config.Clock,
	}, nil
}

func (adapter *Adapter) Descriptor() effectport.AdapterDescriptor { return adapter.descriptor }

func (adapter *Adapter) Dispatch(
	ctx context.Context,
	prepared contracts.EffectIntent,
	envelope contracts.CommandEnvelope,
	authorizationDigest string,
) (contracts.CommandLedger, error) {
	if prepared.Kind != adapter.descriptor.Kind {
		return contracts.CommandLedger{}, errors.New("process adapter received another effect kind")
	}
	raw, err := canonicaljson.Marshal(prepared.Intent)
	if err != nil {
		return contracts.CommandLedger{}, err
	}
	var intent Intent
	if err := canonicaljson.UnmarshalStrict(raw, &intent); err != nil {
		return contracts.CommandLedger{}, err
	}
	resolved, err := adapter.resolver.ResolveProcess(ctx, prepared, intent)
	if err != nil {
		return contracts.CommandLedger{}, err
	}
	dispatch := processport.Dispatch{
		Envelope: envelope, Spec: intent.Spec,
		Executable: resolved.Executable, Arguments: append([]string(nil), resolved.Arguments...),
		Environment: append([]string(nil), resolved.Environment...), WorkingDirectory: resolved.WorkingDirectory,
		StdoutPath: resolved.StdoutPath, StderrPath: resolved.StderrPath,
		KnownIdentity: intent.KnownIdentity, AuthorizationDigest: authorizationDigest,
		At: adapter.clock.Now().UTC(),
	}
	var result processport.Result
	if adapter.descriptor.Kind == KindStart {
		result, err = adapter.provider.Start(ctx, dispatch)
	} else {
		result, err = adapter.provider.Stop(ctx, dispatch)
	}
	if err != nil {
		return contracts.CommandLedger{}, err
	}
	return result.Ledger, nil
}

var _ effectport.Adapter = (*Adapter)(nil)
