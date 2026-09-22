package workflow

import "sort"

// Validate 校验工作流定义：
//   - 节点名不得为空；
//   - 节点不得重复；
//   - 依赖必须指向已存在的节点；
//   - 依赖图不得存在环。
//
// 校验通过后返回按名称排序访问的确定性拓扑序（依赖在前）。
func Validate(nodes []NodeDef) ([]string, error) {
	index := make(map[string]int, len(nodes))
	deps := make(map[string][]string, len(nodes))

	for i, n := range nodes {
		if n.Name == "" {
			return nil, &EmptyNameError{Index: i}
		}
		if _, dup := index[n.Name]; dup {
			return nil, &DuplicateNodeError{Node: n.Name}
		}
		index[n.Name] = i
		d := append([]string(nil), n.DependsOn...)
		sort.Strings(d)
		deps[n.Name] = d
	}

	// 依赖指向的节点必须存在。
	for _, n := range nodes {
		for _, d := range deps[n.Name] {
			if _, ok := index[d]; !ok {
				return nil, &MissingDependencyError{Node: n.Name, Dep: d}
			}
		}
	}

	// DFS 三色标记检测环，节点与依赖均按名称排序以保证结果确定。
	const (
		white = 0
		gray  = 1
		black = 2
	)
	color := make(map[string]int, len(nodes))
	names := make([]string, 0, len(nodes))
	for _, n := range nodes {
		names = append(names, n.Name)
	}
	sort.Strings(names)

	var order []string
	var stack []string

	var visit func(name string) error
	visit = func(name string) error {
		switch color[name] {
		case black:
			return nil
		case gray:
			// 从递归栈中抽取环路径。
			cycle := []string{name}
			for i := len(stack) - 1; i >= 0; i-- {
				cycle = append(cycle, stack[i])
				if stack[i] == name {
					break
				}
			}
			// cycle 当前为 name -> ... -> name，反转后更易读。
			for i, j := 1, len(cycle)-1; i < j; i, j = i+1, j-1 {
				cycle[i], cycle[j] = cycle[j], cycle[i]
			}
			return &CycleError{Cycle: cycle}
		}
		color[name] = gray
		stack = append(stack, name)
		for _, d := range deps[name] {
			if err := visit(d); err != nil {
				return err
			}
		}
		stack = stack[:len(stack)-1]
		color[name] = black
		order = append(order, name)
		return nil
	}

	for _, name := range names {
		if err := visit(name); err != nil {
			return nil, err
		}
	}
	return order, nil
}
