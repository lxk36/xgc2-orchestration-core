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

// Compensator is the optional Saga port for an adapter whose applied Effects
// can be reversed. The core durably records the compensation command before
// calling this method. Implementations must address only the exact external
// identity recorded on applied and use the command envelope idempotently.
type Compensator interface {
	Adapter
	Compensate(context.Context, contracts.EffectRecord, contracts.CommandEnvelope, string) (contracts.CommandLedger, error)
}

// CompensationRecoverer is the optional observation port for a compensation
// whose Accepted receipt was persisted before the host stopped. Accepted is
// proof that the provider already executed the fenced command; recovery must
// observe that same command and append evidence, never dispatch a new mutation.
type CompensationRecoverer interface {
	RecoverCompensation(context.Context, contracts.EffectRecord, contracts.CommandLedger, string) (contracts.CommandLedger, error)
}
