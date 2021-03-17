package paxoskv

import (
	"sort"
)

type ApplierSCC struct{}

func (a *ApplierSCC) getColumnToApply(d *applyData, column int64) (int64, error) {

	if !d.isCommitted(column) {
		return 0, &UncommittedErr{Column: column}
	}

	g, err := a.buildDepGraph(d, column)
	if err != nil {
		return 0, err
	}
	dd(d, "built dep graph: %v", g)

	sscs := findSCC(g, column)
	dd(d, "built execution sscs: %v", sscs)

	firstSCC := sscs[0]
	sort.Slice(firstSCC, func(i, j int) bool {
		return firstSCC[i] < firstSCC[j]
	})

	return firstSCC[0], nil
}

// buildDepGraph builds dependency graph of columns as vertexes.
// Column-a depends on Column-b if log[a][next_apply[a]].Seen[b] > log[b][next_apply[b]]
func (a *ApplierSCC) buildDepGraph(d *applyData, column int64) (graph, error) {

	dd(d, "start buildDepGraph: column: %d", column)
	g := graph{}

	q := []int64{column}
	i := 0
	for ; i < len(q); i++ {
		c := q[i]

		if !d.isCommitted(c) {
			return nil, &UncommittedErr{Column: c}
		}

		deps, err := d.getDeps(c)
		if err != nil {
			return nil, err
		}
		if len(deps) > 0 {
			g[c] = deps
		}
		for _, d := range deps {
			if g[d] == nil {
				q = append(q, d)
			}
		}
	}

	return g, nil
}
