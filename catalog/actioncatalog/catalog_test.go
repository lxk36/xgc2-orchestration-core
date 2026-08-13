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

func fixture(t *testing.T) (contracts.WorkflowDefinition, contracts.ActionVersion) {
	t.Helper()
	empty := contracts.Schema{Type: contracts.TypeObject, Properties: map[string]contracts.Schema{}}
	digest := "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
	definition := contracts.WorkflowDefinition{
		SchemaVersion: workflowkernel.SchemaVersion, WorkflowID: "catalog-action", Version: "v1",
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
