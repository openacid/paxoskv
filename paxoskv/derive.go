package paxoskv

import (
	"fmt"
	"strings"

	"google.golang.org/protobuf/proto"
)

func (b *BallotNum) Clone() *BallotNum { return proto.Clone(b).(*BallotNum) }
func (b *Acceptor) Clone() *Acceptor   { return proto.Clone(b).(*Acceptor) }
func (b *Cmd) Clone() *Cmd             { return proto.Clone(b).(*Cmd) }

type strer interface {
	str() string
}

func (a *BallotNum) Cmp(b *BallotNum) int {
	if a.N < b.N {
		return -1
	}
	if a.N > b.N {
		return 1
	}
	if a.Id < b.Id {
		return -1
	}
	if a.Id > b.Id {
		return 1
	}
	return 0
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
	return fmt.Sprintf("<lsn:%d auth:%s %s=%d>", c.LSN, c.Author.str(), c.Key, c.Vi64)
}

func (a *Acceptor) str() string {
	if a == nil {
		return "nil"
	}

	v := a.Val.str()
	vb := a.VBal.str()
	c := fmt.Sprintf("%v", a.Committed)

	return fmt.Sprintf("<v:%s vbal:%s c:%s>", v, vb, c)
}

func (a *PrepareReq) str() string {
	if a == nil {
		return "nil"
	}
	return fmt.Sprintf("<from:%d Bal:%s>", a.FromLSN, a.Bal.str())
}

func (a *PrepareReply) str() string {
	if a == nil {
		return "nil"
	}
	var as []string
	for _, acc:= range a.Acceptors {
		as = append(as,acc.str())
	}
	return fmt.Sprintf("<lastbal:%s log:%s>", a.LastBal.str(), strings.Join(as, ","))
}

func (a *AcceptReq) str() string {
	if a == nil {
		return "nil"
	}
	var as []string
	for _, cmd:= range a.Cmds {
		as = append(as, cmd.str())
	}
	return fmt.Sprintf("<Bal:%s cmds:%s>", a.Bal.str(), strings.Join(as, ", "))
}

func (a *AcceptReply) str() string {
	if a == nil {
		return "nil"
	}
	return fmt.Sprintf("<lastbal:%s>", a.LastBal.str())
}

func (a *CommitReq) str() string {
	if a == nil {
		return "nil"
	}
	var as []string
	for _, cmd:= range a.Cmds {
		as = append(as, cmd.str())
	}
	return fmt.Sprintf("<cmds:%s>", strings.Join(as,", "))
}

func (a *CommitReply) str() string {
	if a == nil {
		return "nil"
	}
	return "<>"
}
