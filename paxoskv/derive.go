package paxoskv

import (
	"fmt"
	"sort"
	"strings"

	"github.com/gogo/protobuf/proto"
)

func (b *BallotNum) Clone() *BallotNum { return proto.Clone(b).(*BallotNum) }
func (b *Instance) Clone() *Instance   { return proto.Clone(b).(*Instance) }
func (b *Cmd) Clone() *Cmd             { return proto.Clone(b).(*Cmd) }

func (b *Cmd) isNoop() bool {
	return b.Key == "NOOP"
}

func newBal(n, id int64) *BallotNum {
	return &BallotNum{
		N:  n,
		Id: id,
	}
}
func newVid(column, lsn, n int64) *ValueId {
	return &ValueId{
		Column:    column,
		LSN:       lsn,
		ProposerN: n,
	}
}
func NewCmd(expr string, lsn int64, other ...interface{}) *Cmd {
	c := &Cmd{
		ValueId: &ValueId{
			Column:    2,
			LSN:       lsn,
			ProposerN: 1,
		},
		Key:  expr,
		Vi64: lsn,
	}

	for _, o := range other {
		switch v := o.(type) {
		case *BallotNum:
			c.ValueId.ProposerN = v.N
		default:
			panic("???")
		}
	}
	return c
}

type strer interface {
	str() string
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

func (c *BallotNum) str() string {
	if c == nil {
		return "nil"
	}

	return fmt.Sprintf("%d,%d", c.N, c.Id)

}
func (c *Cmd) str() string {
	if c == nil {
		return "nil"
	}
	return fmt.Sprintf("<%s:%s=%d>",
		c.ValueId.str(),
		c.Key,
		c.Vi64,
	)
}

func (a *ValueId) str() string {
	if a == nil {
		return "nil"
	}
	return fmt.Sprintf("%d-%d-%d", a.Column, a.LSN, a.ProposerN)
}

func (a *Instance) str() string {
	if a == nil {
		return "nil"
	}

	v := a.Val.str()
	vb := a.VBal.str()
	c := fmt.Sprintf("%v", a.Committed)

	var seen string
	if a.Seen == nil {
		seen = "nil"
	} else {
		seen = fmt.Sprintf("%d,%d,%d", a.Seen[0], a.Seen[1], a.Seen[2])
	}

	return fmt.Sprintf("<v:%s vbal:%s c:%s seen:%s>", v, vb, c, seen)
}

func instsStr(as map[int64]*Instance) string {

	lsns := make([]int64, 0)

	for lsn := range as {
		lsns = append(lsns, lsn)
	}
	sort.Slice(lsns, func(i, j int) bool {
		return lsns[i] < lsns[j]
	})

	var ss []string
	for _, lsn := range lsns {
		inst := as[lsn]
		ss = append(ss, inst.str())
	}
	return strings.Join(ss, ",")
}

func logsStr(logs []*Instance) string {

	var ss []string

	for _, inst := range logs {
		ss = append(ss, inst.str())
	}
	return strings.Join(ss, ",")
}

func (a *Request) str() string {
	if a == nil {
		return "nil"
	}
	return fmt.Sprintf("<op:%s,bal:%s,col:%d,insts:%s>",
		a.Ops,
		a.Bal.str(),
		a.Column,
		instsStr(a.Instances),
	)
}

func (a *Reply) str() string {
	if a == nil {
		return "nil"
	}
	return fmt.Sprintf("<lastbal:%s,insts:%s>",
		a.LastBal.str(),
		instsStr(a.Instances),
	)
}
