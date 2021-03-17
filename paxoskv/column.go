package paxoskv

import "fmt"

// columnT is the colLogs and states of leader-i
type columnT struct {
	// non nil if it is a leader.
	Bal        *BallotNum
	LastBal    *BallotNum
	Log        []*Instance
	nextCommit int64
	nextApply  int64
}

func NewColumn(id int64) *columnT {
	return &columnT{LastBal: &BallotNum{N: 0, Id: id}}
}

func (col *columnT) getInstance(lsn int64) *Instance {
	if col.hasInstance(lsn) {
		return col.Log[lsn].Clone()
	}
	return nil
}

func (col *columnT) addInstance(inst *Instance) {

	column, lsn := inst.getColLSN()
	for int(lsn) >= len(col.Log) {
		col.Log = append(col.Log, nil)
	}

	if col.Log[lsn] != nil {
		panic("add to non-nil:" + fmt.Sprintf("%d-%d", column, lsn))
	}
	col.Log[lsn] = inst.Clone()
}

func (col *columnT) hasInstance(lsn int64) bool {
	if lsn >= int64(len(col.Log)) {
		return false
	}

	return col.Log[lsn] != nil
}

func packColLSN(column, lsn int64) int64 {
	m := int64(1000000000)
	return column*m + lsn
}
func parseColLSN(x int64) (int64, int64) {
	m := int64(1000000000)
	return x / m, x % m
}
func colLSNIndentedStr(x int64) string {
	column, lsn := parseColLSN(x)
	return fmt.Sprintf("%[1]*d", column*5+3, lsn)
}
