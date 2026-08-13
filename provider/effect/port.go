// Package effect defines the host ports used to dispatch a prepared external
// Effect without coupling providers or node packs to the orchestration
// controller implementation.
package effect

import (
	"context"

	"github.com/lxk36/xgc2-orchestration-core/sdk/go/contracts"
)

type DispatchCredentials struct {
	IdempotencyKey      string
	CapabilityToken     string
	AuthorizationDigest string
}

type CredentialBroker interface {
	ResolveEffectCredentials(context.Context, contracts.EffectRecord, contracts.CommandLedger) (DispatchCredentials, error)
}

type AdapterDescriptor struct {
	Kind           string
	ProviderRef    string
	ProviderDigest string
}

type Adapter interface {
	Descriptor() AdapterDescriptor
	Dispatch(context.Context, contracts.EffectIntent, contracts.CommandEnvelope, string) (contracts.CommandLedger, error)
}
