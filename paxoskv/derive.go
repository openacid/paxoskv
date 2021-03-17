package paxoskv

import (
	"github.com/gogo/protobuf/proto"
)

func (b *BallotNum) Clone() *BallotNum { return proto.Clone(b).(*BallotNum) }
func (b *Ins) Clone() *Ins             { return proto.Clone(b).(*Ins) }
func (b *Cmd) Clone() *Cmd             { return proto.Clone(b).(*Cmd) }

func (b *Cmd) isNoop() bool {
	return b.Key == "NOOP"
}

func NewBal(n, id int64) *BallotNum {
	return &BallotNum{
		N:  n,
		Id: id,
	}
}
func NewInsId(column, lsn, n int64) *InsId {
	return &InsId{
		Column:     column,
		LSN:        lsn,
		ProposerId: n,
	}
}

func NewCmd(expr string, lsn int64) *Cmd {
	c := &Cmd{
		Key:  expr,
		Vi64: lsn,
	}

	return c
}

func (a *BallotNum) Less(b *BallotNum) bool {
	// Vbal == nil is a fast-accepted state
	// a == b == nil return false: not comparable
	if b == nil {
		return false
	}

	if a == nil {
		return true
	}

	// a!= nil && b != nil

	if a.N != b.N {
		return a.N < b.N
	}

	return a.Id < b.Id
}

func (a *BallotNum) LessEqual(b *BallotNum) bool {
	// two nil are incomparable
	return a.Less(b) || (a != nil && a.Equal(b))
}
