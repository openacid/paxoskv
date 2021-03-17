package paxoskv

import (
	"fmt"
	"sort"
	"strings"
)

type strer interface {
	str() string
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
	return fmt.Sprintf("<%s=%d>",
		c.Key,
		c.Vi64,
	)
}

func (a *InsId) str() string {
	if a == nil {
		return "nil"
	}
	return fmt.Sprintf("%d-%d-%d", a.Column, a.LSN, a.ProposerId)
}

func (a *Ins) str() string {
	if a == nil {
		return "nil"
	}

	v := a.Val.str()
	vb := a.VBal.str()
	c := fmt.Sprintf("%v", a.Committed)

	var seen string
	if a.Deps == nil {
		seen = "nil"
	} else {
		seen = fmt.Sprintf("%d,%d,%d", a.Deps[0], a.Deps[1], a.Deps[2])
	}

	return fmt.Sprintf("<%s: %s vbal:%s c:%s seen:%s>", a.InsId.str(), v, vb, c, seen)
}

func instsStr(as map[int64]*Ins) string {

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

func logsStr(logs []*Ins) string {

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
		a.Op,
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
