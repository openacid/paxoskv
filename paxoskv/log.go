package paxoskv

type Log struct {
	nextCommits []int64
	columns     []*Column
}

func NewLog() *Log {
	return &Log{
		nextCommits: []int64{0, 0, 0},
		columns: []*Column{
			NewColumn(0),
			NewColumn(1),
			NewColumn(2),
		},
	}
}

func (l *Log) getNextCommits() []int64 {
	return dupI64s(l.nextCommits)
}

func (l *Log) refInstance(column, lsn int64) *Ins {
	return l.columns[column].Log[lsn]
}
