package dag

import (
	"fmt"
)

type Node struct {
	ID           string   `json:"id"`
	Dependencies []string `json:"dependencies,omitempty"`
}

// TopologicalOrder validates node identifiers and dependencies, then returns a
// dependency-first ordering. It rejects duplicate IDs, unknown dependencies,
// and cycles.
func TopologicalOrder(nodes []Node) ([]string, error) {
	byID := make(map[string]Node, len(nodes))
	for _, node := range nodes {
		if node.ID == "" {
			return nil, fmt.Errorf("node id must not be empty")
		}
		if _, exists := byID[node.ID]; exists {
			return nil, fmt.Errorf("duplicate node id %q", node.ID)
		}
		byID[node.ID] = node
	}

	state := make(map[string]uint8, len(nodes))
	order := make([]string, 0, len(nodes))
	var visit func(string) error
	visit = func(id string) error {
		switch state[id] {
		case 1:
			return fmt.Errorf("cycle detected at node %q", id)
		case 2:
			return nil
		}
		state[id] = 1
		for _, dependency := range byID[id].Dependencies {
			if _, exists := byID[dependency]; !exists {
				return fmt.Errorf("node %q depends on unknown node %q", id, dependency)
			}
			if err := visit(dependency); err != nil {
				return err
			}
		}
		state[id] = 2
		order = append(order, id)
		return nil
	}

	for _, node := range nodes {
		if err := visit(node.ID); err != nil {
			return nil, err
		}
	}
	return order, nil
}
