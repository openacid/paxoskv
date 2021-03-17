package paxoskv

import "log"

// applyData contains all the info to decide which column is the next to apply:
// - the LSN of the next instance to apply on every column
// - the latest isCommitted LSN+1 on every column.
// - what other instance LSNs the instance to apply sees.
type applyData struct {
	nextCommits []int64
	nextApplies []int64
	seens       [][]int64
	lg          *log.Logger
}

// NewApplyData extracts info for applier to find out next instance to apply.
func NewApplyData(log *Log, st *StateMachine, lg *log.Logger) *applyData {
	a := &applyData{
		nextCommits: log.getNextCommits(),
		nextApplies: st.getNextApplies(),
		seens:       [][]int64{nil, nil, nil},
		lg:          lg,
	}

	for column := int64(0); column < 3; column++ {
		if a.isCommitted(column) {
			ins := log.refInstance(column, a.nextApplies[column])
			a.seens[column] = append(a.seens[column], ins.Seen...)
		}
	}

	return a
}

func (d *applyData) getLogger() *log.Logger {
	return d.lg
}

func (d *applyData) isCommitted(column int64) bool {
	return d.nextCommits[column] > d.nextApplies[column]
}

func (d *applyData) doesDependOn(column, depColumn int64) bool {
	return d.seens[column][depColumn] > d.nextApplies[depColumn]
}

// getDeps returns column index that is dependent by `column`.
// If the next instance to apply on a dependent column is not isCommitted, NeedCommitted error returns.
func (d *applyData) getDeps(column int64) ([]int64, error) {

	dd(d, "to find dep of %d", column)

	dependencies := make([]int64, 0, 3)
	for depColumn := int64(0); depColumn < 3; depColumn++ {
		if depColumn == column {
			continue
		}

		if d.doesDependOn(column, depColumn) {
			// column depends on depColumn
			if !d.isCommitted(depColumn) {
				return nil, &UncommittedErr{Column: depColumn}
			}
			dependencies = append(dependencies, depColumn)
		}
	}

	dd(d, "found deps of %d: %v", column, dependencies)
	return dependencies, nil
}
