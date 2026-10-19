package dag

import (
	"reflect"
	"testing"
)

func TestTopologicalOrder(t *testing.T) {
	content := Content{
		Meta: Metadata{Version: "v1", Parallel: true},
		Nodes: []Node{
			{ID: "order", Name: "Order", TemplateID: "tpl-order", Position: Position{X: 0, Y: 0}, Config: NodeConfig{BatchSize: 1, FailStrategy: "abort_dag"}},
			{ID: "user", Name: "User", TemplateID: "tpl-user", Position: Position{X: 500, Y: 0}, Config: NodeConfig{BatchSize: 1, FailStrategy: "abort_dag"}},
			{ID: "goods", Name: "Goods", TemplateID: "tpl-goods", Position: Position{X: 100, Y: 0}, Config: NodeConfig{BatchSize: 1, FailStrategy: "continue"}},
		},
		Edges: []Edge{
			{ID: "e1", SourceNodeID: "user", TargetNodeID: "order"},
			{ID: "e2", SourceNodeID: "goods", TargetNodeID: "order"},
		},
	}
	got, err := TopologicalOrder(content)
	if err != nil {
		t.Fatal(err)
	}
	want := []string{"user", "goods", "order"}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("TopologicalOrder() = %v, want %v", got, want)
	}
}

func TestTopologicalOrderRejectsCycle(t *testing.T) {
	content := Content{
		Meta: Metadata{Version: "v1"},
		Nodes: []Node{
			{ID: "a", Name: "A", TemplateID: "tpl-a", Config: NodeConfig{BatchSize: 1, FailStrategy: "continue"}},
			{ID: "b", Name: "B", TemplateID: "tpl-b", Config: NodeConfig{BatchSize: 1, FailStrategy: "continue"}},
		},
		Edges: []Edge{
			{ID: "e1", SourceNodeID: "a", TargetNodeID: "b"},
			{ID: "e2", SourceNodeID: "b", TargetNodeID: "a"},
		},
	}
	if _, err := TopologicalOrder(content); err == nil {
		t.Fatal("expected cycle error")
	}
}

func TestTopologicalOrderRejectsUnknownEdgeEndpoint(t *testing.T) {
	content := Content{
		Meta: Metadata{Version: "v1"},
		Nodes: []Node{
			{ID: "a", Name: "A", TemplateID: "tpl-a", Config: NodeConfig{BatchSize: 1, FailStrategy: "abort_dag"}},
		},
		Edges: []Edge{{ID: "e1", SourceNodeID: "a", TargetNodeID: "missing"}},
	}
	if _, err := TopologicalOrder(content); err == nil {
		t.Fatal("expected unknown endpoint error")
	}
}

func TestTopologicalOrderRejectsDuplicateNodeID(t *testing.T) {
	content := Content{
		Meta: Metadata{Version: "v1"},
		Nodes: []Node{
			{ID: "a", Name: "A", TemplateID: "tpl-a", Config: NodeConfig{BatchSize: 1, FailStrategy: "abort_dag"}},
			{ID: "a", Name: "A2", TemplateID: "tpl-a2", Config: NodeConfig{BatchSize: 1, FailStrategy: "continue"}},
		},
	}
	if _, err := TopologicalOrder(content); err == nil {
		t.Fatal("expected duplicate node_id error")
	}
}
