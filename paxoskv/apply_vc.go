package paxoskv

import (
	"math"
)

// TODO: use TailBitmap to trace isCommitted status

type ApplierVC struct{}

func (a *ApplierVC) getColumnToApply(d *applyData, column int64) (int64, error) {

	if !d.isCommitted(column) {
		return 0, &UncommittedErr{Column: column}
	}

	minOrd, err := a.findMinOrd(d, column)
	dd(d, "found minOrd: %v err:%v", minOrd, err)
	if err != nil {
		return 0, err
	}

	minColumn := minOrd % 100
	dd(d, "choose column %d to apply", minColumn)
	return minColumn, nil
}

// findMinOrd walks the dependency graph and calculates the ord of every instance walked through.
// It returns the minimal ord.
func (a *ApplierVC) findMinOrd(d *applyData, column int64) (int64, error) {
	minOrd := int64(math.MaxInt64)
	accessed := map[int64]bool{}
	q := []int64{column}

	for i := 0; i < len(q); i++ {

		column := q[i]
		if accessed[column] {
			continue
		}
		accessed[column] = true

		deps, err := d.getDeps(column)
		dd(d, "found deps: %v, err:%v", deps, err)
		if err != nil {
			return 0, err
		}
		minOrd = min(minOrd, int64(len(deps)*100)+column)
		q = append(q, deps...)
	}
	dd(d, "minOrd: %v by walking from %s", minOrd, column)
	return minOrd, nil
}
