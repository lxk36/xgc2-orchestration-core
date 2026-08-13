package controller

import (
	"context"
	"errors"
	"reflect"
	"sync"
	"testing"
	"time"

	"github.com/lxk36/xgc2-orchestration-core/durable/filestore"
	"github.com/lxk36/xgc2-orchestration-core/durable/store"
	"github.com/lxk36/xgc2-orchestration-core/kernel/execution"
	"github.com/lxk36/xgc2-orchestration-core/sdk/go/contracts"
)

const experimentOwnerKind = "configuration.resource-branch"

type activeOwnerFixture struct {
	base       controllerFixture
	controller *Controller
	policy     *ReservedIngressPolicy
	spec       ReservedIngressPolicySpec
	permit     *IngressPermit
	key        contracts.ActiveOwnerKey
}

func newActiveOwnerFixture(t *testing.T) activeOwnerFixture {
	t.Helper()
	base := newControllerFixture(t)
	spec := ReservedIngressPolicySpec{
		PolicyRef: "experiment-builder-v1", NamespaceID: "xgc2-experiments",
		TriggerKind: contracts.TriggerProductBuilder, TriggerVersion: "v1", SourceRef: "xgc2-experiment-builder",
		CandidateOrigin: contracts.OriginProductBuilder, RootOnly: true, RequireActiveOwner: true,
		ActiveOwnerKind: experimentOwnerKind, ActiveOwnerIdentityFields: []string{"branch", "domain", "resourceId"},
	}
	policy, permit, err := NewReservedIngressPolicy(spec)
	if err != nil {
		t.Fatal(err)
	}
	workflowController, err := New(Config{
		Store: base.store, Nodes: base.registry, OwnerRef: "controller-reserved",
		LeaseDuration: time.Minute, InvocationTimeout: 10 * time.Second, Clock: base.clock,
		ReservedIngressPolicies: []*ReservedIngressPolicy{policy},
	})
	if err != nil {
		t.Fatal(err)
	}
	return activeOwnerFixture{
		base: base, controller: workflowController, policy: policy, spec: spec, permit: permit,
		key: contracts.ActiveOwnerKey{
			NamespaceID: "xgc2-experiments", Kind: experimentOwnerKind,
			Identity: map[string]string{"domain": "default", "resourceId": "experiment-alpha", "branch": "main"},
		},
	}
}

func (fixture activeOwnerFixture) request(runID, commandID string) InvokeRequest {
	request := fixture.base.request(runID, commandID)
	request.NamespaceID = fixture.key.NamespaceID
	request.Action.AcceptedTriggerKinds = []contracts.TriggerKind{contracts.TriggerProductBuilder}
	request.Trigger.EventID = "event-" + runID
	request.Trigger.Kind = contracts.TriggerProductBuilder
	request.Trigger.SourceRef = "xgc2-experiment-builder"
	request.CandidateOrigin = contracts.OriginProductBuilder
	request.CandidateRef = "experiment-alpha-main"
	request.IngressPermit = fixture.permit
	key := fixture.key
	key.Identity = map[string]string{}
	for field, value := range fixture.key.Identity {
		key.Identity[field] = value
	}
	request.ActiveOwnerKey = &key
	return request
}

func TestReservedProductBuilderRequiresRegisteredUnforgeablePermit(t *testing.T) {
	fixture := newActiveOwnerFixture(t)
	request := fixture.request("run-reserved", "invoke-reserved")

	without := request
	without.IngressPermit = nil
	if _, err := fixture.controller.Invoke(t.Context(), without); !errors.Is(err, ErrReservedIngressDenied) {
		t.Fatalf("missing permit error = %v", err)
	}
	zero := request
	zero.IngressPermit = &IngressPermit{}
	if _, err := fixture.controller.Invoke(t.Context(), zero); !errors.Is(err, ErrReservedIngressDenied) {
		t.Fatalf("zero permit error = %v", err)
	}
	foreignPolicy, foreignPermit, err := NewReservedIngressPolicy(fixture.spec)
	if err != nil || foreignPolicy.Authorizes(fixture.permit) || fixture.policy.Authorizes(foreignPermit) {
		t.Fatalf("foreign capability was not unique: err=%v", err)
	}
	foreign := request
	foreign.IngressPermit = foreignPermit
	if _, err := fixture.controller.Invoke(t.Context(), foreign); !errors.Is(err, ErrReservedIngressDenied) {
		t.Fatalf("foreign permit error = %v", err)
	}
	wrongFields := request
	wrongKey := *request.ActiveOwnerKey
	wrongKey.Identity = map[string]string{"domain": "default", "resourceId": "experiment-alpha", "commit": "main"}
	wrongFields.ActiveOwnerKey = &wrongKey
	if _, err := fixture.controller.Invoke(t.Context(), wrongFields); !errors.Is(err, ErrReservedIngressDenied) {
		t.Fatalf("wrong owner fields error = %v", err)
	}

	invoked, err := fixture.controller.Invoke(t.Context(), request)
	if err != nil || invoked.Replay || invoked.Run.Status != contracts.RunAccepted ||
		invoked.Run.AdmissionPolicyRef != fixture.spec.PolicyRef ||
		invoked.Run.AdmissionPolicyDigest != fixture.policy.Digest() ||
		invoked.Run.ActiveOwnerRef == "" || invoked.Run.ActiveOwnerGeneration != 1 {
		t.Fatalf("reserved invoke = %#v, err = %v", invoked, err)
	}
	manual := fixture.base.request("run-manual-open", "invoke-manual-open")
	if _, err := fixture.controller.Invoke(t.Context(), manual); err != nil {
		t.Fatalf("ordinary manual ingress regressed: %v", err)
	}
	manual.NamespaceID = fixture.key.NamespaceID
	manual.RunID = "run-manual-reserved"
	manual.CommandID = "invoke-manual-reserved"
	manual.Trigger.EventID = "event-manual-reserved"
	if _, err := fixture.controller.Invoke(t.Context(), manual); !errors.Is(err, ErrReservedIngressDenied) {
		t.Fatalf("reserved namespace admitted manual without permit: %v", err)
	}
	productWithoutRegisteredNamespace := fixture.base.request("run-product-default", "invoke-product-default")
	productWithoutRegisteredNamespace.Action.AcceptedTriggerKinds = []contracts.TriggerKind{contracts.TriggerProductBuilder}
	productWithoutRegisteredNamespace.Trigger.Kind = contracts.TriggerProductBuilder
	productWithoutRegisteredNamespace.CandidateOrigin = contracts.OriginProductBuilder
	if _, err := fixture.base.controller.Invoke(t.Context(), productWithoutRegisteredNamespace); !errors.Is(err, ErrReservedIngressDenied) {
		t.Fatalf("unregistered root product builder was admitted: %v", err)
	}
}

func TestActiveOwnerKeyCanonicalDigestIncludesBranch(t *testing.T) {
	left := contracts.ActiveOwnerKey{
		NamespaceID: "xgc2-experiments", Kind: experimentOwnerKind,
		Identity: map[string]string{"resourceId": "experiment-alpha", "branch": "main", "domain": "default"},
	}
	right := contracts.ActiveOwnerKey{
		NamespaceID: "xgc2-experiments", Kind: experimentOwnerKind,
		Identity: map[string]string{"domain": "default", "branch": "main", "resourceId": "experiment-alpha"},
	}
	leftDigest, err := ActiveOwnerKeyDigest(left)
	if err != nil {
		t.Fatal(err)
	}
	rightDigest, err := ActiveOwnerKeyDigest(right)
	if err != nil || leftDigest != rightDigest {
		t.Fatalf("map order changed digest: %s != %s, err=%v", leftDigest, rightDigest, err)
	}
	right.Identity["branch"] = "camera-calibration"
	branchDigest, err := ActiveOwnerKeyDigest(right)
	if err != nil || branchDigest == leftDigest {
		t.Fatalf("branch was absent from owner identity: %s, err=%v", branchDigest, err)
	}
	right.Identity["branch"] = " main "
	if _, err := ActiveOwnerKeyDigest(right); err == nil {
		t.Fatal("non-canonical branch value was admitted")
	}
}

func TestDifferentBranchesAcquireIndependentActiveOwners(t *testing.T) {
	fixture := newActiveOwnerFixture(t)
	mainRequest := fixture.request("run-branch-main", "invoke-branch-main")
	featureRequest := fixture.request("run-branch-feature", "invoke-branch-feature")
	featureKey := *featureRequest.ActiveOwnerKey
	featureKey.Identity = map[string]string{"domain": "default", "resourceId": "experiment-alpha", "branch": "camera-calibration"}
	featureRequest.ActiveOwnerKey = &featureKey
	mainRun, err := fixture.controller.Invoke(t.Context(), mainRequest)
	if err != nil {
		t.Fatal(err)
	}
	featureRun, err := fixture.controller.Invoke(t.Context(), featureRequest)
	if err != nil || mainRun.Run.ActiveOwnerRef == featureRun.Run.ActiveOwnerRef {
		t.Fatalf("branch-scoped owners main=%#v feature=%#v err=%v", mainRun, featureRun, err)
	}
	mainOwner, err := fixture.controller.GetActiveRunOwner(t.Context(), fixture.key)
	if err != nil || mainOwner.RunID != mainRun.Run.RunID {
		t.Fatalf("main owner = %#v, err=%v", mainOwner, err)
	}
	featureOwner, err := fixture.controller.GetActiveRunOwner(t.Context(), featureKey)
	if err != nil || featureOwner.RunID != featureRun.Run.RunID {
		t.Fatalf("feature owner = %#v, err=%v", featureOwner, err)
	}
}

func TestConcurrentActiveOwnerAcquireHasOneWinnerAndExactReplay(t *testing.T) {
	fixture := newActiveOwnerFixture(t)
	requests := []InvokeRequest{
		fixture.request("run-owner-a", "invoke-owner-a"),
		fixture.request("run-owner-b", "invoke-owner-b"),
	}
	type result struct {
		index int
		value InvokeResult
		err   error
	}
	start := make(chan struct{})
	results := make(chan result, len(requests))
	var wait sync.WaitGroup
	for index := range requests {
		wait.Add(1)
		go func(index int) {
			defer wait.Done()
			<-start
			value, err := fixture.controller.Invoke(t.Context(), requests[index])
			results <- result{index: index, value: value, err: err}
		}(index)
	}
	close(start)
	wait.Wait()
	close(results)
	winner, loser := -1, -1
	for current := range results {
		if current.err == nil {
			if winner != -1 || current.value.Replay {
				t.Fatalf("unexpected successful result: %#v", current)
			}
			winner = current.index
			continue
		}
		if !errors.Is(current.err, ErrActiveOwnerConflict) {
			t.Fatalf("loser error = %v", current.err)
		}
		loser = current.index
	}
	if winner == -1 || loser == -1 {
		t.Fatalf("winner=%d loser=%d", winner, loser)
	}
	if _, err := fixture.controller.GetRun(t.Context(), requests[loser].RunID); !errors.Is(err, store.ErrNotFound) {
		t.Fatalf("losing Run was published: %v", err)
	}
	owner, err := fixture.controller.GetActiveRunOwner(t.Context(), fixture.key)
	if err != nil || owner.State != contracts.ActiveRunOwnerActive || owner.RunID != requests[winner].RunID {
		t.Fatalf("owner = %#v, err=%v", owner, err)
	}
	active, err := fixture.controller.ResolveActiveRun(t.Context(), fixture.key)
	if err != nil || active.RunID != requests[winner].RunID {
		t.Fatalf("active Run = %#v, err=%v", active, err)
	}
	replayed, err := fixture.controller.Invoke(t.Context(), requests[winner])
	if err != nil || !replayed.Replay || replayed.Run.RunID != requests[winner].RunID || replayed.Run.Revision != 1 {
		t.Fatalf("winner replay = %#v, err=%v", replayed, err)
	}
	changed := requests[winner]
	changed.Candidate = map[string]any{"name": "changed"}
	if _, err := fixture.controller.Invoke(t.Context(), changed); !errors.Is(err, store.ErrIdentityConflict) {
		t.Fatalf("changed replay error = %v", err)
	}
}

func TestConcurrentExactStartCommandReplaysOneAcceptedOutcome(t *testing.T) {
	fixture := newActiveOwnerFixture(t)
	request := fixture.request("run-exact-concurrent", "invoke-exact-concurrent")
	start := make(chan struct{})
	results := make(chan InvokeResult, 2)
	errorsCh := make(chan error, 2)
	var wait sync.WaitGroup
	for range 2 {
		wait.Add(1)
		go func() {
			defer wait.Done()
			<-start
			value, err := fixture.controller.Invoke(t.Context(), request)
			results <- value
			errorsCh <- err
		}()
	}
	close(start)
	wait.Wait()
	close(results)
	close(errorsCh)
	for err := range errorsCh {
		if err != nil {
			t.Fatalf("exact concurrent invoke error = %v", err)
		}
	}
	values := make([]InvokeResult, 0, 2)
	for value := range results {
		values = append(values, value)
	}
	if len(values) != 2 || values[0].Run.RunID != request.RunID || values[1].Run.RunID != request.RunID ||
		values[0].Run.Revision != 1 || values[1].Run.Revision != 1 || values[0].Replay == values[1].Replay {
		t.Fatalf("exact concurrent results = %#v", values)
	}
}

func TestActiveOwnerReleasesAtEveryExactTerminalStatus(t *testing.T) {
	tests := []struct {
		name string
		kind contracts.TerminationKind
		want contracts.RunStatus
	}{
		{name: "succeeded", want: contracts.RunSucceeded},
		{name: "stopped", kind: contracts.TerminationStopped, want: contracts.RunStopped},
		{name: "canceled", kind: contracts.TerminationCanceled, want: contracts.RunCanceled},
		{name: "failed", kind: contracts.TerminationFailed, want: contracts.RunFailed},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			fixture := newActiveOwnerFixture(t)
			request := fixture.request("run-terminal-"+test.name, "invoke-terminal-"+test.name)
			invoked, err := fixture.controller.Invoke(t.Context(), request)
			if err != nil {
				t.Fatal(err)
			}
			var termination TerminateRunRequest
			if test.kind != "" {
				termination = TerminateRunRequest{
					RunID: invoked.Run.RunID, ExpectedRevision: invoked.Run.Revision, Kind: test.kind,
					RequestedBy: "operator", ReasonCode: "test-terminal", CommandID: "terminate-" + test.name,
				}
				if test.kind == contracts.TerminationFailed {
					termination.PrimaryFailure = &contracts.StructuredFailure{
						Class: contracts.FailurePermanent, Code: "test-failure", Message: "test failure",
					}
				}
				if _, err := fixture.controller.RequestRunTermination(t.Context(), termination); !errors.Is(err, ErrReservedIngressDenied) {
					t.Fatalf("generic termination error = %v", err)
				}
				wrongKey := fixture.key
				wrongKey.Identity = map[string]string{"domain": "default", "resourceId": "experiment-alpha", "branch": "other"}
				if _, err := fixture.controller.RequestActiveRunTermination(t.Context(), wrongKey, termination); !errors.Is(err, ErrReservedIngressDenied) {
					t.Fatalf("wrong-key termination error = %v", err)
				}
				stopping, err := fixture.controller.RequestActiveRunTermination(t.Context(), fixture.key, termination)
				if err != nil || stopping.Run.Status != contracts.RunStopping {
					t.Fatalf("termination = %#v, err=%v", stopping, err)
				}
			}
			var terminal contracts.Run
			if test.kind == "" {
				terminal, err = fixture.controller.Drive(t.Context(), invoked.Run.RunID)
			} else {
				coordinator, coordinatorErr := NewCoordinator(CoordinatorConfig{
					Controller: fixture.controller, Store: fixture.base.store,
					OwnerRef: "terminal-coordinator-" + test.name, Clock: fixture.base.clock,
				})
				if coordinatorErr != nil {
					t.Fatal(coordinatorErr)
				}
				terminal, err = coordinator.AdvanceRun(t.Context(), invoked.Run.RunID)
			}
			if err != nil || terminal.Status != test.want {
				t.Fatalf("terminal Run = %#v, err=%v", terminal, err)
			}
			owner, err := fixture.controller.GetActiveRunOwner(t.Context(), fixture.key)
			if err != nil || owner.State != contracts.ActiveRunOwnerReleased || owner.RunID != terminal.RunID ||
				owner.TerminalStatus != terminal.Status || owner.TerminalRevision != terminal.Revision || owner.Revision != 2 {
				t.Fatalf("released owner = %#v, err=%v", owner, err)
			}
			if _, err := fixture.controller.ResolveActiveRun(t.Context(), fixture.key); !errors.Is(err, store.ErrNotFound) {
				t.Fatalf("released owner resolved an active Run: %v", err)
			}
			if test.kind != "" {
				replay, err := fixture.controller.RequestActiveRunTermination(t.Context(), fixture.key, termination)
				if err != nil || !replay.Replay || replay.Run.Status != terminal.Status {
					t.Fatalf("owner-scoped termination replay = %#v, err=%v", replay, err)
				}
			}
		})
	}
}

func TestOldStartReplaysAfterReleaseAndNextGenerationAcquire(t *testing.T) {
	fixture := newActiveOwnerFixture(t)
	firstRequest := fixture.request("run-generation-one", "invoke-generation-one")
	first, err := fixture.controller.Invoke(t.Context(), firstRequest)
	if err != nil {
		t.Fatal(err)
	}
	if terminal, err := fixture.controller.Drive(t.Context(), first.Run.RunID); err != nil || terminal.Status != contracts.RunSucceeded {
		t.Fatalf("first terminal = %#v, err=%v", terminal, err)
	}
	secondRequest := fixture.request("run-generation-two", "invoke-generation-two")
	second, err := fixture.controller.Invoke(t.Context(), secondRequest)
	if err != nil || second.Run.ActiveOwnerGeneration != 2 {
		t.Fatalf("second acquire = %#v, err=%v", second, err)
	}
	path := fixture.base.path
	if err := fixture.base.store.Close(); err != nil {
		t.Fatal(err)
	}
	reopened, err := filestore.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = reopened.Close() })
	recovered, err := New(Config{
		Store: reopened, Nodes: fixture.base.registry, OwnerRef: "controller-owner-recovered",
		LeaseDuration: time.Minute, InvocationTimeout: 10 * time.Second, Clock: fixture.base.clock,
		ReservedIngressPolicies: []*ReservedIngressPolicy{fixture.policy},
	})
	if err != nil {
		t.Fatal(err)
	}
	fixture.controller = recovered
	replayed, err := fixture.controller.Invoke(t.Context(), firstRequest)
	if err != nil || !replayed.Replay || replayed.Run.RunID != first.Run.RunID ||
		replayed.Run.ActiveOwnerGeneration != 1 || !reflect.DeepEqual(replayed.Run, first.Run) {
		t.Fatalf("old Start replay = %#v, err=%v", replayed, err)
	}
	owner, err := fixture.controller.GetActiveRunOwner(t.Context(), fixture.key)
	if err != nil || owner.State != contracts.ActiveRunOwnerActive || owner.RunID != second.Run.RunID || owner.Generation != 2 {
		t.Fatalf("generation two owner changed by replay: %#v, err=%v", owner, err)
	}
}

var errInjectedOwnerRelease = errors.New("injected owner release failure")

type ownerReleaseFailStore struct {
	store.Store
	mu   sync.Mutex
	fail bool
}

func (barrier *ownerReleaseFailStore) Commit(ctx context.Context, transaction store.Transaction) (store.CommitResult, error) {
	barrier.mu.Lock()
	fail := barrier.fail
	barrier.mu.Unlock()
	if fail {
		for _, mutation := range transaction.Mutations {
			if mutation.Key.Type == activeOwnerAggregateType && mutation.Revision%2 == 0 {
				return store.CommitResult{}, errInjectedOwnerRelease
			}
		}
	}
	return barrier.Store.Commit(ctx, transaction)
}

func (barrier *ownerReleaseFailStore) allowRelease() {
	barrier.mu.Lock()
	barrier.fail = false
	barrier.mu.Unlock()
}

func TestTerminalOwnerReleaseIsAtomicWithRunAndOwnershipGraph(t *testing.T) {
	fixture := newActiveOwnerFixture(t)
	barrier := &ownerReleaseFailStore{Store: fixture.base.store, fail: true}
	workflowController, err := New(Config{
		Store: barrier, Nodes: fixture.base.registry, OwnerRef: "controller-release-barrier",
		LeaseDuration: time.Minute, InvocationTimeout: 10 * time.Second, Clock: fixture.base.clock,
		ReservedIngressPolicies: []*ReservedIngressPolicy{fixture.policy},
	})
	if err != nil {
		t.Fatal(err)
	}
	request := fixture.request("run-atomic-release", "invoke-atomic-release")
	invoked, err := workflowController.Invoke(t.Context(), request)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := workflowController.Drive(t.Context(), invoked.Run.RunID); !errors.Is(err, errInjectedOwnerRelease) {
		t.Fatalf("terminal release fault = %v", err)
	}
	current, err := workflowController.GetRun(t.Context(), invoked.Run.RunID)
	if err != nil || current.Status.Terminal() {
		t.Fatalf("Run partially reached terminal state: %#v, err=%v", current, err)
	}
	owner, err := workflowController.GetActiveRunOwner(t.Context(), fixture.key)
	if err != nil || owner.State != contracts.ActiveRunOwnerActive || owner.RunID != current.RunID {
		t.Fatalf("owner partially released: %#v, err=%v", owner, err)
	}
	if _, err := workflowController.OwnershipGraph(t.Context(), current.RunID); !errors.Is(err, store.ErrNotFound) {
		t.Fatalf("ownership graph partially committed: %v", err)
	}
	barrier.allowRelease()
	terminal, err := workflowController.Drive(t.Context(), current.RunID)
	if err != nil || terminal.Status != contracts.RunSucceeded {
		t.Fatalf("terminal retry = %#v, err=%v", terminal, err)
	}
	owner, err = workflowController.GetActiveRunOwner(t.Context(), fixture.key)
	if err != nil || owner.State != contracts.ActiveRunOwnerReleased || owner.TerminalRevision != terminal.Revision {
		t.Fatalf("released retry owner = %#v, err=%v", owner, err)
	}
}

func TestPublicInvokeCannotForgeChildIngress(t *testing.T) {
	fixture := newControllerFixture(t)
	request := fixture.request("forged-child", "invoke-forged-child")
	request.Parent = &contracts.ParentRunLink{
		ParentRunID: "parent", ParentInvocationID: "invocation", CallNodeID: "call", MappingDigest: testPackageDigest,
	}
	request.RootRunID = "root"
	request.Trigger.Kind = contracts.TriggerActionCall
	request.CandidateOrigin = contracts.OriginParentMap
	if _, err := fixture.controller.Invoke(t.Context(), request); !errors.Is(err, ErrReservedIngressDenied) {
		t.Fatalf("forged child error = %v", err)
	}
	if _, err := fixture.controller.GetRun(t.Context(), request.RunID); !errors.Is(err, store.ErrNotFound) {
		t.Fatalf("forged child published a Run: %v", err)
	}
}

func TestRunOwnerFenceValidation(t *testing.T) {
	fixture := newActiveOwnerFixture(t)
	invoked, err := fixture.controller.Invoke(t.Context(), fixture.request("run-fence", "invoke-fence"))
	if err != nil {
		t.Fatal(err)
	}
	broken := invoked.Run
	broken.ActiveOwnerGeneration = 0
	if err := execution.ValidateRun(broken); err == nil {
		t.Fatal("partial active owner fence validated")
	}
	broken = invoked.Run
	broken.AdmissionPolicyDigest = ""
	if err := execution.ValidateRun(broken); err == nil {
		t.Fatal("partial admission policy fence validated")
	}
	broken = invoked.Run
	broken.Parent = &contracts.ParentRunLink{
		ParentRunID: "parent", ParentInvocationID: "invocation", CallNodeID: "call", MappingDigest: testPackageDigest,
	}
	broken.RootRunID = "root"
	if err := execution.ValidateRun(broken); err == nil {
		t.Fatal("child Run retained a root owner fence")
	}
}
