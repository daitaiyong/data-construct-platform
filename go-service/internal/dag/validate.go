package dag

import (
	"fmt"
	"strings"
)

type Position struct {
	X float64 `json:"x"`
	Y float64 `json:"y"`
}

type NodeConfig struct {
	BatchSize    uint32            `json:"batch_size"`
	VarExport    []string          `json:"var_export,omitempty"`
	VarMapping   map[string]string `json:"var_mapping,omitempty"`
	FailStrategy string            `json:"fail_strategy"`
}

type Node struct {
	ID         string     `json:"node_id"`
	Name       string     `json:"node_name"`
	TemplateID string     `json:"template_id"`
	Position   Position   `json:"position"`
	Config     NodeConfig `json:"config"`
}

type Edge struct {
	ID           string `json:"edge_id"`
	SourceNodeID string `json:"source_node_id"`
	TargetNodeID string `json:"target_node_id"`
	Label        string `json:"label,omitempty"`
}

type Metadata struct {
	Version  string `json:"version"`
	Parallel bool   `json:"parallel"`
}

type Content struct {
	Nodes []Node   `json:"nodes"`
	Edges []Edge   `json:"edges"`
	Meta  Metadata `json:"dag_meta"`
}

// TopologicalOrder validates node and edge references and returns a stable,
// dependency-first order. Canvas positions are deliberately ignored.
func TopologicalOrder(content Content) ([]string, error) {
	if len(content.Nodes) == 0 {
		return nil, fmt.Errorf("DAG must contain at least one node")
	}
	if strings.TrimSpace(content.Meta.Version) == "" {
		return nil, fmt.Errorf("dag_meta.version must not be empty")
	}

	nodeOrder := make([]string, 0, len(content.Nodes))
	byID := make(map[string]Node, len(content.Nodes))
	for _, node := range content.Nodes {
		if strings.TrimSpace(node.ID) == "" {
			return nil, fmt.Errorf("node_id must not be empty")
		}
		if _, exists := byID[node.ID]; exists {
			return nil, fmt.Errorf("duplicate node_id %q", node.ID)
		}
		if strings.TrimSpace(node.Name) == "" {
			return nil, fmt.Errorf("node %q node_name must not be empty", node.ID)
		}
		if strings.TrimSpace(node.TemplateID) == "" {
			return nil, fmt.Errorf("node %q template_id must not be empty", node.ID)
		}
		if node.Config.BatchSize == 0 {
			return nil, fmt.Errorf("node %q batch_size must be positive", node.ID)
		}
		if node.Config.FailStrategy != "abort_dag" && node.Config.FailStrategy != "continue" {
			return nil, fmt.Errorf("node %q has invalid fail_strategy %q", node.ID, node.Config.FailStrategy)
		}
		byID[node.ID] = node
		nodeOrder = append(nodeOrder, node.ID)
	}

	edgeIDs := make(map[string]struct{}, len(content.Edges))
	edgePairs := make(map[string]struct{}, len(content.Edges))
	adjacency := make(map[string][]string, len(content.Nodes))
	for _, edge := range content.Edges {
		if strings.TrimSpace(edge.ID) == "" {
			return nil, fmt.Errorf("edge_id must not be empty")
		}
		if _, exists := edgeIDs[edge.ID]; exists {
			return nil, fmt.Errorf("duplicate edge_id %q", edge.ID)
		}
		edgeIDs[edge.ID] = struct{}{}
		if _, exists := byID[edge.SourceNodeID]; !exists {
			return nil, fmt.Errorf("edge %q references unknown source node %q", edge.ID, edge.SourceNodeID)
		}
		if _, exists := byID[edge.TargetNodeID]; !exists {
			return nil, fmt.Errorf("edge %q references unknown target node %q", edge.ID, edge.TargetNodeID)
		}
		if edge.SourceNodeID == edge.TargetNodeID {
			return nil, fmt.Errorf("edge %q cannot connect a node to itself", edge.ID)
		}
		pair := edge.SourceNodeID + "\x00" + edge.TargetNodeID
		if _, exists := edgePairs[pair]; exists {
			return nil, fmt.Errorf("duplicate edge from %q to %q", edge.SourceNodeID, edge.TargetNodeID)
		}
		edgePairs[pair] = struct{}{}
		adjacency[edge.TargetNodeID] = append(adjacency[edge.TargetNodeID], edge.SourceNodeID)
	}

	state := make(map[string]uint8, len(content.Nodes))
	order := make([]string, 0, len(content.Nodes))
	var visit func(string) error
	visit = func(nodeID string) error {
		switch state[nodeID] {
		case 1:
			return fmt.Errorf("cycle detected at node %q", nodeID)
		case 2:
			return nil
		}
		state[nodeID] = 1
		for _, dependency := range adjacency[nodeID] {
			if err := visit(dependency); err != nil {
				return err
			}
		}
		state[nodeID] = 2
		order = append(order, nodeID)
		return nil
	}
	for _, nodeID := range nodeOrder {
		if err := visit(nodeID); err != nil {
			return nil, err
		}
	}
	return order, nil
}
