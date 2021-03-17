package paxoskv

func (inst *Instance) getColumn() int64 {
	return inst.Val.ValueId.Column
}

func (inst *Instance) getLSN() int64 {
	return inst.Val.ValueId.LSN
}

func (inst *Instance) getColLSN() (int64, int64) {
	return inst.getColumn(), inst.getLSN()
}

func (inst *Instance) updateSeen(u []int64) {
	for i := 0; i < 3; i++ {
		inst.Seen[i] = max(inst.Seen[i], u[i])
	}
}
