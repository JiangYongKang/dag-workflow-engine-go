package dag

import (
	"fmt"
	"sort"
)

// Validate 对工作流定义做静态校验，按固定顺序检查并返回首个错误：
//  1. 重复节点 -> ErrDuplicateNode
//  2. 依赖指向不存在节点 -> ErrDependencyNotFound
//  3. 环形依赖 -> ErrCycleDetected
//
// 任何一类问题都不会被静默忽略。
func Validate(def *WorkflowDef) error {
	if def == nil {
		return &DefinitionError{Kind: ErrInvalidConfig, NodeID: "", Detail: "nil workflow definition"}
	}
	if def.ID == "" {
		return &DefinitionError{Kind: ErrInvalidConfig, NodeID: "", Detail: "empty workflow id"}
	}

	// 1. 重复节点（按出现顺序报告第一次重复）。
	seen := make(map[string]int)
	for i, n := range def.Nodes {
		if n.ID == "" {
			return &DefinitionError{Kind: ErrInvalidConfig, NodeID: "", Detail: fmt.Sprintf("node at index %d has empty id", i)}
		}
		if _, ok := seen[n.ID]; ok {
			return &DefinitionError{Kind: ErrDuplicateNode, NodeID: n.ID,
				Detail: fmt.Sprintf("declared %d times", seen[n.ID]+1)}
		}
		seen[n.ID] = i
	}

	// 2. 依赖缺失。
	for _, n := range def.Nodes {
		depSeen := make(map[string]struct{}, len(n.Deps))
		for _, dep := range n.Deps {
			if _, ok := seen[dep]; !ok {
				return &DefinitionError{Kind: ErrDependencyNotFound, NodeID: n.ID,
					Detail: fmt.Sprintf("depends on missing node %q", dep)}
			}
			depSeen[dep] = struct{}{}
		}
		_ = depSeen
	}

	// 3. 环形依赖（Kahn 入度剥离，剩余节点必在环上或依赖环）。
	if cyc := findCycle(def); len(cyc) > 0 {
		sort.Strings(cyc)
		return &DefinitionError{Kind: ErrCycleDetected, NodeID: cyc[0],
			Detail: fmt.Sprintf("nodes on cycle: %v", cyc)}
	}
	return nil
}

// findCycle 用 Kahn 算法返回无法被剥离的节点集合。
func findCycle(def *WorkflowDef) []string {
	indeg := make(map[string]int, len(def.Nodes))
	// children: dep -> 依赖它的节点
	children := make(map[string][]string)
	for _, n := range def.Nodes {
		indeg[n.ID] = 0
	}
	uniq := func(in []string) []string {
		m := make(map[string]struct{}, len(in))
		out := in[:0]
		for _, x := range in {
			if _, ok := m[x]; ok {
				continue
			}
			m[x] = struct{}{}
			out = append(out, x)
		}
		return out
	}
	for _, n := range def.Nodes {
		for _, dep := range uniq(append([]string{}, n.Deps...)) {
			indeg[n.ID]++
			children[dep] = append(children[dep], n.ID)
		}
	}

	// 确定性：按 ID 排序处理零入度节点。
	queue := make([]string, 0)
	for id, d := range indeg {
		if d == 0 {
			queue = append(queue, id)
		}
	}
	sort.Strings(queue)
	removed := 0
	for len(queue) > 0 {
		cur := queue[0]
		queue = queue[1:]
		removed++
		next := make([]string, 0)
		for _, ch := range children[cur] {
			indeg[ch]--
			if indeg[ch] == 0 {
				next = append(next, ch)
			}
		}
		sort.Strings(next)
		queue = append(queue, next...)
	}
	if removed == len(def.Nodes) {
		return nil
	}
	cyc := make([]string, 0)
	for id, d := range indeg {
		if d > 0 {
			cyc = append(cyc, id)
		}
	}
	return cyc
}

// TopoLevels 返回分层拓扑序：第 0 层无依赖，之后每层只依赖更早层。
// 同一层内节点互不依赖，可安全并发。结果按节点 ID 排序以保持确定性。
// 调用前应先通过 Validate。
func TopoLevels(def *WorkflowDef) ([][]string, error) {
	if err := Validate(def); err != nil {
		return nil, err
	}
	deps := make(map[string][]string)
	children := make(map[string][]string)
	indeg := make(map[string]int)
	for _, n := range def.Nodes {
		deps[n.ID] = append([]string{}, n.Deps...)
		indeg[n.ID] = len(n.Deps)
		for _, dep := range n.Deps {
			children[dep] = append(children[dep], n.ID)
		}
	}
	levels := make([][]string, 0)
	remaining := len(def.Nodes)
	for remaining > 0 {
		layer := make([]string, 0)
		for id, d := range indeg {
			if d == 0 {
				layer = append(layer, id)
			}
		}
		if len(layer) == 0 {
			return nil, &DefinitionError{Kind: ErrCycleDetected, Detail: "unexpected cycle during topo sort"}
		}
		sort.Strings(layer)
		levels = append(levels, layer)
		for _, id := range layer {
			delete(indeg, id)
			remaining--
			for _, ch := range children[id] {
				indeg[ch]--
			}
		}
	}
	return levels, nil
}
