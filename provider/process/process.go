// Package process defines the private dispatch port for managed process
// providers. Public ProcessSpec contains references and digests; resolved host
// paths, arguments, environment values, and capability tokens stay private.
package process

import (
	"context"
	"time"

	"github.com/lxk36/xgc2-orchestration-core/sdk/go/contracts"
)

const (
	ActionStart   = "process.start"
	ActionStop    = "process.stop"
	ActionInspect = "process.inspect"
)

type Dispatch struct {
	Envelope            contracts.CommandEnvelope
	Spec                contracts.ProcessSpec
	Executable          string   `json:"-"`
	Arguments           []string `json:"-"`
	Environment         []string `json:"-"`
	WorkingDirectory    string   `json:"-"`
	StdoutPath          string   `json:"-"`
	StderrPath          string   `json:"-"`
	KnownIdentity       *contracts.ProcessIdentity
	AuthorizationDigest string
	At                  time.Time
}

type Result struct {
	Ledger      contracts.CommandLedger
	Observation *contracts.ProcessObservation
}

type InspectRequest struct {
	BindingID    string
	Generation   uint64
	FencingToken uint64
	Identity     contracts.ProcessIdentity
	At           time.Time
}

type Provider interface {
	Start(context.Context, Dispatch) (Result, error)
	Stop(context.Context, Dispatch) (Result, error)
	Inspect(context.Context, InspectRequest) (contracts.ProcessObservation, error)
}
