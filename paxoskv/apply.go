package paxoskv

// applier defines the API of an apply algorithm impl
type applier interface {
	// getColumnToApply finds the column for next apply, by looking for an applicable instance starting from column `column`.
	// The returned column may not be  `column`, i.e., the instance at `nextApplies` on column `column` depends on an instance on another column.
	// If no instance can be applied, i.e., a dependent instance is not isCommitted, error UncommittedErr returned.
	getColumnToApply(d *applyData, column int64) (int64, error)
}

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
	nApplied := 0

	i := int64(0)
	// apply one of the columns
	for ; i < 3; i++ {
		n, err := h.applyNext(i)
		_ = err
		nApplied += n
	}
	return nApplied
}

// applyNext tries to apply the lowest non-applied instance on column `column`.
func (h *Handler) applyNext(column int64) (int, error) {

	s := h.kvServer
	d := NewApplyData(s.log, s.stateMachine, h.getLogger())

	applyColumn, err := h.kvServer.applier.getColumnToApply(d, column)
	dd(h, "applyNext from: %d, err: %v", applyColumn, err)
	if err != nil {
		return 0, err
	}

	lsn := s.stateMachine.nextApplies[applyColumn]

	inst := s.log.refInstance(applyColumn, lsn)

	s.stateMachine.applyInstance(h, inst)
	return 1, nil
}
