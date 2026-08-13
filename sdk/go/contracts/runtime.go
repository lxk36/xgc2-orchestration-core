package contracts

import "time"

type RuntimeBindingState string

const (
	RuntimeBindingPreparing RuntimeBindingState = "preparing"
	RuntimeBindingActive    RuntimeBindingState = "active"
	RuntimeBindingStopping  RuntimeBindingState = "stopping"
	RuntimeBindingReleased  RuntimeBindingState = "released"
	RuntimeBindingLost      RuntimeBindingState = "lost"
)

func (state RuntimeBindingState) Valid() bool {
	switch state {
	case RuntimeBindingPreparing, RuntimeBindingActive, RuntimeBindingStopping, RuntimeBindingReleased, RuntimeBindingLost:
		return true
	default:
		return false
	}
}

func (state RuntimeBindingState) Terminal() bool {
	return state == RuntimeBindingReleased || state == RuntimeBindingLost
}

type RuntimeObservedState string

const (
	RuntimeObservedUnknown  RuntimeObservedState = "unknown"
	RuntimeObservedStarting RuntimeObservedState = "starting"
	RuntimeObservedRunning  RuntimeObservedState = "running"
	RuntimeObservedStopping RuntimeObservedState = "stopping"
	RuntimeObservedStopped  RuntimeObservedState = "stopped"
	RuntimeObservedFailed   RuntimeObservedState = "failed"
	RuntimeObservedLost     RuntimeObservedState = "lost"
)

func (state RuntimeObservedState) Valid() bool {
	switch state {
	case RuntimeObservedUnknown, RuntimeObservedStarting, RuntimeObservedRunning, RuntimeObservedStopping,
		RuntimeObservedStopped, RuntimeObservedFailed, RuntimeObservedLost:
		return true
	default:
		return false
	}
}

type RuntimeHealth string

const (
	RuntimeHealthUnknown   RuntimeHealth = "unknown"
	RuntimeHealthHealthy   RuntimeHealth = "healthy"
	RuntimeHealthDegraded  RuntimeHealth = "degraded"
	RuntimeHealthUnhealthy RuntimeHealth = "unhealthy"
	RuntimeHealthLost      RuntimeHealth = "lost"
)

func (health RuntimeHealth) Valid() bool {
	switch health {
	case RuntimeHealthUnknown, RuntimeHealthHealthy, RuntimeHealthDegraded, RuntimeHealthUnhealthy, RuntimeHealthLost:
		return true
	default:
		return false
	}
}

type RuntimeCleanupPolicy string

const (
	RuntimeCleanupNone        RuntimeCleanupPolicy = "none"
	RuntimeCleanupOnRunClose  RuntimeCleanupPolicy = "on-run-close"
	RuntimeCleanupOnScopeExit RuntimeCleanupPolicy = "on-scope-exit"
)

func (policy RuntimeCleanupPolicy) Valid() bool {
	switch policy {
	case RuntimeCleanupNone, RuntimeCleanupOnRunClose, RuntimeCleanupOnScopeExit:
		return true
	default:
		return false
	}
}

type RuntimeBinding struct {
	BindingID         string               `json:"bindingId"`
	NamespaceID       string               `json:"namespaceId"`
	RunID             string               `json:"runId"`
	InvocationID      string               `json:"invocationId"`
	NodeID            string               `json:"nodeId"`
	RuntimeKey        string               `json:"runtimeKey"`
	Kind              string               `json:"kind"`
	SpecDigest        string               `json:"specDigest"`
	ProviderRef       string               `json:"providerRef"`
	ProviderDigest    string               `json:"providerDigest"`
	Ownership         EffectOwnership      `json:"ownership"`
	CleanupPolicy     RuntimeCleanupPolicy `json:"cleanupPolicy"`
	State             RuntimeBindingState  `json:"state"`
	ObservedState     RuntimeObservedState `json:"observedState"`
	Health            RuntimeHealth        `json:"health"`
	ExternalIdentity  string               `json:"externalIdentity,omitempty"`
	Generation        uint64               `json:"generation"`
	FencingToken      uint64               `json:"fencingToken"`
	LeaseOwner        string               `json:"leaseOwner"`
	LeaseTokenHash    string               `json:"leaseTokenHash"`
	LeaseExpiresAt    time.Time            `json:"leaseExpiresAt"`
	ObservationDigest string               `json:"observationDigest,omitempty"`
	ObservationAt     *time.Time           `json:"observationAt,omitempty"`
	PrimaryFailure    *StructuredFailure   `json:"primaryFailure,omitempty"`
	CreatedAt         time.Time            `json:"createdAt"`
	ActivatedAt       *time.Time           `json:"activatedAt,omitempty"`
	ReleaseStartedAt  *time.Time           `json:"releaseStartedAt,omitempty"`
	FinishedAt        *time.Time           `json:"finishedAt,omitempty"`
	UpdatedAt         time.Time            `json:"updatedAt"`
	Revision          uint64               `json:"revision"`
}

type ProcessSpec struct {
	ProcessID        string `json:"processId"`
	Version          string `json:"version"`
	DescriptorDigest string `json:"descriptorDigest"`
	// DefinitionDigest pins the provider-owned immutable process definition;
	// changing a catalog entry without changing this digest fails closed.
	DefinitionDigest       string `json:"definitionDigest"`
	ExecutableRef          string `json:"executableRef"`
	ArgumentTemplateDigest string `json:"argumentTemplateDigest"`
	// ParameterSetRef names an immutable host-side parameter artifact. The
	// public workflow persists only its reference and digest, never resolved
	// secret values or host paths.
	ParameterSetRef     string            `json:"parameterSetRef"`
	ParameterSetDigest  string            `json:"parameterSetDigest"`
	EnvironmentRefs     map[string]string `json:"environmentRefs,omitempty"`
	WorkingDirectoryRef string            `json:"workingDirectoryRef,omitempty"`
	StdoutArtifactRef   string            `json:"stdoutArtifactRef"`
	StderrArtifactRef   string            `json:"stderrArtifactRef"`
	GracePeriodMillis   uint64            `json:"gracePeriodMillis"`
	KillWaitMillis      uint64            `json:"killWaitMillis"`
}

type ProcessIdentity struct {
	PID        int    `json:"pid"`
	PGID       int    `json:"pgid"`
	StartTicks uint64 `json:"startTicks"`
}

type ProcessObservation struct {
	BindingID         string               `json:"bindingId"`
	Generation        uint64               `json:"generation"`
	FencingToken      uint64               `json:"fencingToken"`
	Identity          *ProcessIdentity     `json:"identity,omitempty"`
	State             RuntimeObservedState `json:"state"`
	Health            RuntimeHealth        `json:"health"`
	ExitCode          *int                 `json:"exitCode,omitempty"`
	ObservationDigest string               `json:"observationDigest"`
	ObservedAt        time.Time            `json:"observedAt"`
}
