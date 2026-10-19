package dag

import (
	"errors"
)

type DAGNode struct {
	NodeId     string
	TemplateId string
	Deps       []string
	VarMapping map[string]string
}

type DAG struct {
	Nodes []*DAGNode
}

// Topology 拓扑排序，返回执行顺序，检测循环依赖
func (d *DAG) Topology() ([]*DAGNode, error) {
	// 构建邻接表、入度表
	adj := make(map[string][]string)
	inDegree := make(map[string]int)
	nodeMap := make(map[string]*DAGNode)
	for _, n := range d.Nodes {
		nodeMap[n.NodeId] = n
		inDegree[n.NodeId] = 0
		adj[n.NodeId] = []string{}
	}
	for _, n := range d.Nodes {
		for _, depNodeId := range n.Deps {
			adj[depNodeId] = append(adj[depNodeId], n.NodeId)
			inDegree[n.NodeId]++
		}
	}
	// Kahn算法
	var queue []string
	for id, cnt := range inDegree {
		if cnt == 0 {
			queue = append(queue, id)
		}
	}
	var result []*DAGNode
	for len(queue) > 0 {
		u := queue[0]
		queue = queue[1:]
		result = append(result, nodeMap[u])
		for _, v := range adj[u] {
			inDegree[v]--
			if inDegree[v] == 0 {
				queue = append(queue, v)
			}
		}
	}
	if len(result) != len(d.Nodes) {
		return nil, errors.New("dag exist cycle dependency")
	}
	return result, nil
}
