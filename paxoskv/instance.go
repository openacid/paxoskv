package paxoskv

import (
	"fmt"
)

func (inst *Ins) getColumn() int64 {
	return inst.InsId.Column
}

func (inst *Ins) getLSN() int64 {
	return inst.InsId.LSN
}

func (inst *Ins) getColLSN() (int64, int64) {
	return inst.getColumn(), inst.getLSN()
}

func (inst *Ins) updateDeps(u []int64) {
	for i := 0; i < 3; i++ {
		inst.Deps[i] = max(inst.Deps[i], u[i])
	}
}

func (inst *Ins) packedPosition() int64 {
	column, lsn := inst.getColLSN()
	m := int64(1000000000)
	return column*m + lsn
}
func (inst *Ins) colLSNIndentedStr() string {
	column, lsn := inst.getColLSN()
	return fmt.Sprintf("%[1]*d", column*5+3, lsn)
}
