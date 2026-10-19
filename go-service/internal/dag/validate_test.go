package dag

import (
	"reflect"
	"testing"
)

func TestTopologicalOrder(t *testing.T) {
	nodes := []Node{
		{ID: "load", Dependencies: []string{"prepare"}},
		{ID: "prepare"},
		{ID: "publish", Dependencies: []string{"load"}},
	}
	got, err := TopologicalOrder(nodes)
	if err != nil {
		t.Fatal(err)
	}
	want := []string{"prepare", "load", "publish"}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("TopologicalOrder() = %v, want %v", got, want)
	}
}

func TestTopologicalOrderRejectsCycle(t *testing.T) {
	_, err := TopologicalOrder([]Node{
		{ID: "a", Dependencies: []string{"b"}},
		{ID: "b", Dependencies: []string{"a"}},
	})
	if err == nil {
		t.Fatal("expected cycle error")
	}
}

func TestTopologicalOrderRejectsUnknownDependency(t *testing.T) {
	_, err := TopologicalOrder([]Node{{ID: "a", Dependencies: []string{"missing"}}})
	if err == nil {
		t.Fatal("expected unknown dependency error")
	}
}
