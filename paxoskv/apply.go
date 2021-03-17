package paxoskv

import (
	"sort"
)

func (h *Handler) apply() (int, error) {
	nApplied := 0
	for {
		n := h.applyAnyColumn()
		nApplied += n
		if n == 0 {
			return nApplied, nil
		}
	}
}

func (h *Handler) applyAnyColumn() int {
	s := h.s
	nApplied := 0

	i := int64(0)
	// apply one of the columns
	for ; i < 3; i++ {
		col := s.columns[i]
		if col.nextApply >= int64(len(col.Log)) {
			dd(h, "apply: no log to apply, column: %d, nextApply: %d", i, col.nextApply)
			continue
		}

		n, err := h.applyOne(i)
		_ = err
		nApplied += n
	}
	return nApplied
}

func (h *Handler) applyOne(column int64) (int, error) {

	s := h.s
	col := s.columns[column]
	if col.nextApply >= int64(len(col.Log)) {
		panic("wtf")
	}

	g, err := h.buildDepGraph(column)
	if err != nil {
		return 0, err
	}
	seq := findSCC(g, column)
	for _, ss := range seq {
		sort.Slice(ss, func(i, j int) bool {
			return ss[i] < ss[j]
		})
		for _, c := range ss {
			h.doApply(c)
			return 1, nil
		}
	}
	panic("not apply?")
	return 0, nil
}

// buildDepGraph builds dependency graph of columns as vertexes.
// Column-a depends on Column-b if log[a][next_apply[a]].Seen[b] > log[b][next_apply[b]]
func (h *Handler) buildDepGraph(column int64) (graph, error) {
	s := h.s

	dd(h, "start buildDepGraph: column: %d", column)
	g := graph{}

	q := []int64{column}
	i := 0
	for ; i < len(q); i++ {
		c := q[i]
		col := s.columns[c]
		next := col.nextApply
		dd(h, "buildDepGraph: column: %d nextApply: %d", c, next)
		if next >= int64(len(col.Log)) {
			dd(h, "buildDepGraph: no such log yet: %d-%d", c, next)
		} else {
			dd(h, "buildDepGraph: column: %d nextApply: %d, log: %s", c, next, col.Log[next].str())
		}
		if next >= int64(len(col.Log)) || col.Log[next] == nil || !col.Log[next].Committed {
			dd(h, "buildDepGraph: %d-%d waiting for %d-%d to commit", column, s.columns[column].nextApply, c, next)
			return nil, &NeedCommitError{
				Column: c,
				Err:    NotCommitted,
			}
		}

		deps := s.getDep(c)
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

// getDep get all dependent columns
func (s *KVServer) getDep(column int64) []int64 {
	rst := []int64{}

	col := s.columns[column]
	lowestI := col.nextApply
	low := col.Log[lowestI]
	knows := low.Seen
	for i := 0; i < 3; i++ {
		if int64(i) == column {
			continue
		}
		if knows[i] > s.columns[i].nextApply {
			rst = append(rst, int64(i))
		}
	}

	return rst
}

func (h *Handler) doApply(column int64) {
	s := h.s

	col := s.columns[column]
	lsn := col.nextApply

	inst := col.Log[lsn]

	dd(h, "A: doApply: applied: col-lsn=%d-%d: %s", column, lsn, inst.str())

	s.storage[inst.Val.Key] = inst
	col.nextApply++
	s.applySeq = append(s.applySeq, column*1000000000+lsn)
}

func (s *KVServer) getSnapshot() map[string]int64 {
	s.Lock()
	defer s.Unlock()

	rst := make(map[string]int64)
	for k, acc := range s.storage {
		rst[k] = acc.Val.Vi64
	}

	return rst
}
