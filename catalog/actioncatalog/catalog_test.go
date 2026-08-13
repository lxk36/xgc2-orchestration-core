package actioncatalog

import (
	"errors"
	"testing"
	"time"

	"github.com/lxk36/xgc2-orchestration-core/durable/filestore"
	"github.com/lxk36/xgc2-orchestration-core/durable/store"
	workflowkernel "github.com/lxk36/xgc2-orchestration-core/kernel/workflow"
	"github.com/lxk36/xgc2-orchestration-core/sdk/go/contracts"
)

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
