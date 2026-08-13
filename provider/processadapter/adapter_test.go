package processadapter

import (
	"context"
	"errors"
	"reflect"
	"testing"
	"time"

	processport "github.com/lxk36/xgc2-orchestration-core/provider/process"
	"github.com/lxk36/xgc2-orchestration-core/sdk/go/contracts"
)

const adapterTestDigest = "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"

type staticResolver struct {
	resolution Resolution
	err        error
}

func (resolver staticResolver) ResolveProcess(context.Context, contracts.EffectIntent, Intent) (Resolution, error) {
	return resolver.resolution, resolver.err
}

type recordingProvider struct {
	start *processport.Dispatch
	stop  *processport.Dispatch
}

func (provider *recordingProvider) Start(_ context.Context, dispatch processport.Dispatch) (processport.Result, error) {
	provider.start = &dispatch
	return processport.Result{}, nil
}

func (provider *recordingProvider) Stop(_ context.Context, dispatch processport.Dispatch) (processport.Result, error) {
	provider.stop = &dispatch
	return processport.Result{}, nil
}

func (*recordingProvider) Inspect(context.Context, processport.InspectRequest) (contracts.ProcessObservation, error) {
	return contracts.ProcessObservation{}, errors.New("not used")
}

func TestStopResolverPrivatelyRestoresSpecAndExactIdentity(t *testing.T) {
	provider := &recordingProvider{}
	spec := contracts.ProcessSpec{
		ProcessID: "simulator", Version: "v1", DescriptorDigest: adapterTestDigest,
		DefinitionDigest: adapterTestDigest, ExecutableRef: "simulator-bin",
		ArgumentTemplateDigest: adapterTestDigest, ParameterSetRef: "defaults",
		ParameterSetDigest: adapterTestDigest, StdoutArtifactRef: "stdout-sim",
		StderrArtifactRef: "stderr-sim", GracePeriodMillis: 100, KillWaitMillis: 1000,
	}
	identity := contracts.ProcessIdentity{PID: 123, PGID: 123, StartTicks: 456}
	adapter, err := New(Config{
		Kind: KindStop, ProviderRef: "local-process", ProviderDigest: adapterTestDigest,
		Provider: provider, Resolver: staticResolver{resolution: Resolution{Spec: &spec, KnownIdentity: &identity}},
	})
	if err != nil {
		t.Fatal(err)
	}
	prepared := contracts.EffectIntent{
		Kind: KindStop,
		Intent: map[string]any{
			"externalIdentityRef": "process-instance-1", "ownerRunRef": "start-run-1",
		},
	}
	if _, err := adapter.Dispatch(t.Context(), prepared, contracts.CommandEnvelope{}, adapterTestDigest); err != nil {
		t.Fatal(err)
	}
	if provider.stop == nil || !reflect.DeepEqual(provider.stop.Spec, spec) || provider.stop.KnownIdentity == nil || *provider.stop.KnownIdentity != identity {
		t.Fatalf("stop dispatch did not use private resolution: %#v", provider.stop)
	}
}

func TestStartResolverCannotReplacePublicProcessPin(t *testing.T) {
	provider := &recordingProvider{}
	override := contracts.ProcessSpec{ProcessID: "replacement"}
	adapter, err := New(Config{
		Kind: KindStart, ProviderRef: "local-process", ProviderDigest: adapterTestDigest,
		Provider: provider, Resolver: staticResolver{resolution: Resolution{Spec: &override}},
		Clock: fixedClock{at: time.Date(2026, 8, 13, 6, 0, 0, 0, time.UTC)},
	})
	if err != nil {
		t.Fatal(err)
	}
	prepared := contracts.EffectIntent{Kind: KindStart, Intent: map[string]any{"spec": contracts.ProcessSpec{ProcessID: "authored"}}}
	if _, err := adapter.Dispatch(t.Context(), prepared, contracts.CommandEnvelope{}, adapterTestDigest); err == nil {
		t.Fatal("start resolver replaced the immutable public process pin")
	}
	if provider.start != nil {
		t.Fatal("provider was called after a start pin replacement")
	}
}

type fixedClock struct{ at time.Time }

func (clock fixedClock) Now() time.Time { return clock.at }
