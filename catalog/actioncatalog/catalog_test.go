package actioncatalog

import (
	"errors"
	"testing"
	"time"

	"github.com/lxk36/xgc2-orchestration-core/durable/filestore"
	"github.com/lxk36/xgc2-orchestration-core/durable/store"
	"github.com/lxk36/xgc2-orchestration-core/kernel/ingress"
	workflowkernel "github.com/lxk36/xgc2-orchestration-core/kernel/workflow"
	"github.com/lxk36/xgc2-orchestration-core/sdk/go/contracts"
)

func TestReservedNamespaceInstallRequiresSameProductBuilderPermit(t *testing.T) {
	durable, err := filestore.Open(t.TempDir() + "/reserved-actions.db")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = durable.Close() })
	spec := ingress.ReservedIngressPolicySpec{
		PolicyRef: "experiment-builder-v1", NamespaceID: "xgc2-experiments",
		TriggerKind: contracts.TriggerProductBuilder, TriggerVersion: "v1", SourceRef: "xgc2-experiment-builder",
		CandidateOrigin: contracts.OriginProductBuilder, RootOnly: true,
	}
	policy, permit, err := ingress.NewReservedIngressPolicy(spec)
	if err != nil {
		t.Fatal(err)
	}
	catalog, err := NewWithConfig(Config{Store: durable, ReservedIngressPolicies: []*ingress.ReservedIngressPolicy{policy}})
	if err != nil {
		t.Fatal(err)
	}
	definition, version := namedFixture(t, "reserved-action")
	version.AcceptedTriggerKinds = []contracts.TriggerKind{contracts.TriggerProductBuilder}
	at := time.Date(2026, 8, 13, 7, 0, 0, 0, time.UTC)
	request := InstallRequest{
		NamespaceID: spec.NamespaceID, Action: version, Definition: definition,
		CommandID: "install-reserved-action", At: at,
	}
	if _, err := catalog.Install(t.Context(), request); !errors.Is(err, ErrReservedNamespaceDenied) {
		t.Fatalf("generic reserved install error = %v", err)
	}
	request.IngressPermit = &ingress.IngressPermit{}
	if _, err := catalog.Install(t.Context(), request); !errors.Is(err, ErrReservedNamespaceDenied) {
		t.Fatalf("zero reserved permit error = %v", err)
	}
	_, foreignPermit, err := ingress.NewReservedIngressPolicy(spec)
	if err != nil {
		t.Fatal(err)
	}
	request.IngressPermit = foreignPermit
	if _, err := catalog.Install(t.Context(), request); !errors.Is(err, ErrReservedNamespaceDenied) {
		t.Fatalf("foreign reserved permit error = %v", err)
	}
	request.IngressPermit = permit
	installed, err := catalog.Install(t.Context(), request)
	if err != nil || installed.Replay || installed.Record.AdmissionPolicyRef != spec.PolicyRef ||
		installed.Record.AdmissionPolicyDigest != policy.Digest() {
		t.Fatalf("reserved install = %#v, err=%v", installed, err)
	}
	replayRequest := request
	replayRequest.CommandID = "install-reserved-action-replay"
	replayRequest.At = at.Add(time.Second)
	replayed, err := catalog.Install(t.Context(), replayRequest)
	if err != nil || !replayed.Replay || replayed.Record.AdmissionPolicyDigest != policy.Digest() {
		t.Fatalf("reserved install replay = %#v, err=%v", replayed, err)
	}
	replayRequest.IngressPermit = nil
	if _, err := catalog.Install(t.Context(), replayRequest); !errors.Is(err, ErrReservedNamespaceDenied) {
		t.Fatalf("reserved replay bypassed permit: %v", err)
	}
	openDefinition, openVersion := namedFixture(t, "open-action")
	if _, err := catalog.Install(t.Context(), InstallRequest{
		NamespaceID: "open-team", Action: openVersion, Definition: openDefinition,
		CommandID: "install-open-action", At: at,
	}); err != nil {
		t.Fatalf("ordinary install regressed: %v", err)
	}
	if _, err := catalog.Install(t.Context(), InstallRequest{
		NamespaceID: "open-team", Action: openVersion, Definition: openDefinition,
		IngressPermit: permit, CommandID: "install-open-with-reserved-permit", At: at,
	}); !errors.Is(err, ErrReservedNamespaceDenied) {
		t.Fatalf("reserved permit escaped its namespace: %v", err)
	}
}

func TestCatalogPersistsAndResolvesOnlyExactNamespaceActionPin(t *testing.T) {
	path := t.TempDir() + "/actions.db"
	durable, err := filestore.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	catalog, _ := New(durable)
	definition, version := fixture(t)
	at := time.Date(2026, 8, 13, 8, 0, 0, 0, time.UTC)
	installed, err := catalog.Install(t.Context(), InstallRequest{
		NamespaceID: "team-a", Action: version, Definition: definition,
		CommandID: "install-action-v1", At: at,
	})
	if err != nil || installed.Replay || installed.Record.Revision != 1 {
		t.Fatalf("install = %+v err=%v", installed, err)
	}
	replay, err := catalog.Install(t.Context(), InstallRequest{
		NamespaceID: "team-a", Action: version, Definition: definition,
		CommandID: "install-action-replay", At: at.Add(time.Second),
	})
	if err != nil || !replay.Replay || !replay.Record.Action.Ref().Equal(version.Ref()) {
		t.Fatalf("semantic replay = %+v err=%v", replay, err)
	}
	if _, _, err := catalog.ResolveAction(t.Context(), "team-b", version.Ref()); !errors.Is(err, store.ErrNotFound) {
		t.Fatalf("other namespace resolution error = %v", err)
	}
	if err := durable.Close(); err != nil {
		t.Fatal(err)
	}

	reopened, err := filestore.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = reopened.Close() })
	recovered, _ := New(reopened)
	action, workflow, err := recovered.ResolveAction(t.Context(), "team-a", version.Ref())
	if err != nil || !action.Ref().Equal(version.Ref()) || workflow.WorkflowID != definition.WorkflowID {
		t.Fatalf("recovered action = %+v workflow=%+v err=%v", action, workflow, err)
	}
}

func TestCatalogListsBoundedNamespaceMetadataAndGetsExactDefinition(t *testing.T) {
	durable, err := filestore.Open(t.TempDir() + "/actions.db")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = durable.Close() })
	catalog, err := New(durable)
	if err != nil {
		t.Fatal(err)
	}
	at := time.Date(2026, 8, 13, 9, 0, 0, 0, time.UTC)
	refs := make(map[string]contracts.ActionRef)
	for index, item := range []struct {
		namespace string
		actionID  string
	}{
		{namespace: "team-a", actionID: "alpha"},
		{namespace: "team-b", actionID: "foreign"},
		{namespace: "team-a", actionID: "bravo"},
		{namespace: "team-a", actionID: "charlie"},
	} {
		definition, version := namedFixture(t, item.actionID)
		refs[item.actionID] = version.Ref()
		if _, err := catalog.Install(t.Context(), InstallRequest{
			NamespaceID: item.namespace, Action: version, Definition: definition,
			CommandID: "install-" + item.actionID, At: at.Add(time.Duration(index) * time.Second),
		}); err != nil {
			t.Fatal(err)
		}
	}

	seen := map[string]bool{}
	cursor := ""
	for pageNumber := 0; pageNumber < 10; pageNumber++ {
		page, err := catalog.List(t.Context(), ListRequest{NamespaceID: "team-a", After: cursor, Limit: 1})
		if err != nil {
			t.Fatal(err)
		}
		for _, item := range page.Items {
			if item.NamespaceID != "team-a" || item.Revision != 1 || item.InstalledAt.IsZero() || seen[item.Action.ActionID] {
				t.Fatalf("invalid or duplicated metadata: %+v seen=%v", item, seen)
			}
			seen[item.Action.ActionID] = true
		}
		if page.NextCursor == "" {
			break
		}
		if page.NextCursor == cursor {
			t.Fatal("Action catalog cursor did not advance")
		}
		cursor = page.NextCursor
	}
	if len(seen) != 3 || seen["foreign"] {
		t.Fatalf("listed actions = %v", seen)
	}
	record, err := catalog.Get(t.Context(), "team-a", refs["bravo"])
	if err != nil || record.Definition.WorkflowID != "bravo" || !record.Action.Ref().Equal(refs["bravo"]) {
		t.Fatalf("exact record = %+v err=%v", record, err)
	}
	if _, err := catalog.Get(t.Context(), "team-b", refs["bravo"]); !errors.Is(err, store.ErrNotFound) {
		t.Fatalf("cross-namespace exact get error = %v", err)
	}
	if _, err := catalog.List(t.Context(), ListRequest{NamespaceID: "team-a", Limit: 0}); err == nil {
		t.Fatal("unbounded Action catalog list was accepted")
	}
}

func fixture(t *testing.T) (contracts.WorkflowDefinition, contracts.ActionVersion) {
	return namedFixture(t, "catalog-action")
}

func namedFixture(t *testing.T, actionID string) (contracts.WorkflowDefinition, contracts.ActionVersion) {
	t.Helper()
	empty := contracts.Schema{Type: contracts.TypeObject, Properties: map[string]contracts.Schema{}}
	digest := "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
	definition := contracts.WorkflowDefinition{
		SchemaVersion: workflowkernel.SchemaVersion, WorkflowID: actionID, Version: "v1",
		InputSchema: empty, ResultSchema: empty, TriggerSchema: empty, ScopeSchema: empty,
		Entrypoints: map[string]string{"main": "noop"},
		Nodes: []contracts.WorkflowNodeDefinition{{
			NodeID: "noop", TypeRef: "xgc.test.noop/v1", DescriptorDigest: digest,
			InputSchema: empty, OutputSchema: empty,
		}},
		Edges: []contracts.WorkflowEdge{}, ResultBindings: map[string][]contracts.ValueBinding{"main": {}},
	}
	plan, err := workflowkernel.Compile(definition)
	if err != nil {
		t.Fatal(err)
	}
	return definition, contracts.ActionVersion{
		ActionID: definition.WorkflowID, Version: definition.Version, DefinitionDigest: plan.DefinitionDigest,
		Entrypoint: "main", InputSchema: empty, ResultSchema: empty,
		AcceptedTriggerKinds: []contracts.TriggerKind{contracts.TriggerActionCall},
	}
}
