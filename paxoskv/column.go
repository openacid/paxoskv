package paxoskv

import "fmt"

// Column is the colLogs and states of leader-i
type Column struct {
	// non nil if it is a leader.
	column  int64
	Bal     *BallotNum
	LastBal *BallotNum
	Log     []*Ins
}

func NewColumn(id int64) *Column {
	return &Column{
		column:  id,
		LastBal: &BallotNum{N: 0, Id: id},
	}
}

func (col *Column) getInstance(lsn int64) *Ins {
	if col.hasInstance(lsn) {
		return col.Log[lsn].Clone()
	}
	return nil
}

func (col *Column) addInstance(inst *Ins) {

	column, lsn := inst.getColLSN()
	for int(lsn) >= len(col.Log) {
		col.Log = append(col.Log, nil)
	}

	if col.Log[lsn] != nil {
		panic("add to non-nil:" + fmt.Sprintf("%d-%d", column, lsn))
	}
	col.Log[lsn] = inst.Clone()
}

func (col *Column) hasInstance(lsn int64) bool {
	if lsn >= int64(len(col.Log)) {
		return false
	}

	return col.Log[lsn] != nil
}

func parseColLSN(x int64) (int64, int64) {
	m := int64(1000000000)
	return x / m, x % m
}
func colLSNIndentedStr(x int64) string {
	column, lsn := parseColLSN(x)
	return fmt.Sprintf("%[1]*d", column*5+3, lsn)
}
