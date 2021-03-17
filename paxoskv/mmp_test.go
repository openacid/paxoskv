package paxoskv

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestKVServer_hdlPrepare(t *testing.T) {
	ta := require.New(t)

	s := NewKVServer(1)
	s.log.columns[0] = &Column{
		LastBal: NewBal(0, 0),
		Log:     []*Ins{nil, {InsId: NewInsId(0, 1, 0), Val: &Cmd{Key: "foo", Vi64: 10}, Deps: []int64{3, 0, 3}}},
	}

	{
		// prepare a nil instnace 0-0
		lsn := int64(0)
		inst := &Ins{InsId: NewInsId(0, lsn, 1), Val: &Cmd{}, Deps: []int64{1, 0, 5}}

		h := NewHandler(s, inst)
		gotInst, err := h.hdlPrepare(inst)
		ta.Equal("<0-0-1: <=0> vbal:nil c:false seen:2,0,5>", gotInst.str())
		ta.Equal("<0-0-1: <=0> vbal:nil c:false seen:2,0,5>", s.log.columns[0].Log[lsn].str())
		ta.Equal(nil, err)
	}

	{
		// prepare a non-nil instnace 0-1
		lsn := int64(1)
		inst := &Ins{InsId: NewInsId(0, lsn, 3), Val: &Cmd{}, Deps: []int64{1, 0, 5}}

		h := NewHandler(s, inst)
		gotInst, err := h.hdlPrepare(inst)
		ta.Equal("<0-1-0: <foo=10> vbal:nil c:false seen:3,0,3>", gotInst.str())
		ta.Equal("<0-1-0: <foo=10> vbal:nil c:false seen:3,0,3>", s.log.columns[0].Log[lsn].str())
		ta.Equal(AlreadyPrepared, err)
	}

	{
		// prepare a nil instnace 0-2
		lsn := int64(2)
		inst := &Ins{
			InsId: NewInsId(0, lsn, 1),
			Val:   &Cmd{},
			Deps:  []int64{1, 0, 5},
		}

		h := NewHandler(s, inst)
		gotInst, err := h.hdlPrepare(inst)
		ta.Equal("<0-2-1: <=0> vbal:nil c:false seen:2,0,5>", gotInst.str())
		ta.Equal("<0-2-1: <=0> vbal:nil c:false seen:2,0,5>", s.log.columns[0].Log[lsn].str())
		ta.Equal(nil, err)
	}

}
func TestKVServer_hdlOps(t *testing.T) {
	ta := require.New(t)

	column := int64(1)
	lsn := int64(0)

	cmdV := &Cmd{Key: "V"}
	cmdW := &Cmd{Key: "W"}

	cases := []struct {
		name string
		inst *Ins
		op   Op
		// server:
		logs     []*Ins
		wantInst string
		wantLogs string
	}{
		{
			name: "vnil->nil",
			inst: &Ins{InsId: NewInsId(column, lsn, 0), Val: cmdV.Clone(), VBal: nil, Deps: []int64{0, 1, 5}},
			logs: []*Ins{},

			wantInst: "<1-0-0: <V=0> vbal:10,2 c:false seen:1,1,5>",
			wantLogs: "<1-0-0: <V=0> vbal:10,2 c:false seen:1,1,5>",
		},
		{
			name: "v1->nil, no seen updated",
			inst: &Ins{InsId: NewInsId(column, lsn, 0), Val: cmdV.Clone(), VBal: NewBal(1, 0), Deps: []int64{0, 1, 5}},
			logs: []*Ins{},

			wantInst: "<1-0-0: <V=0> vbal:10,2 c:true seen:0,1,5>",
			wantLogs: "<1-0-0: <V=0> vbal:10,2 c:true seen:0,1,5>",
		},
		{
			name: "v0->w1",
			inst: &Ins{InsId: NewInsId(column, lsn, 0), Val: cmdV.Clone(), VBal: NewBal(0, 0), Deps: []int64{0, 1, 5}},
			logs: []*Ins{
				{InsId: NewInsId(column, lsn, 1), Val: cmdW.Clone(), VBal: NewBal(2, 1), Deps: []int64{3, 0, 3}},
			},
			wantInst: "<1-0-1: <W=0> vbal:10,2 c:false seen:3,0,3>",
			wantLogs: "<1-0-1: <W=0> vbal:10,2 c:false seen:3,0,3>",
		},
		{
			name: "v2->w1",
			inst: &Ins{InsId: NewInsId(column, lsn, 0), Val: cmdV.Clone(), VBal: NewBal(2, 0), Deps: []int64{0, 1, 5}},
			logs: []*Ins{
				{InsId: NewInsId(column, lsn, 1), Val: cmdW.Clone(), VBal: NewBal(1, 1), Deps: []int64{3, 0, 3}},
			},
			wantInst: "<1-0-0: <V=0> vbal:10,2 c:true seen:0,1,5>",
			wantLogs: "<1-0-0: <V=0> vbal:10,2 c:true seen:0,1,5>",
		},
		{
			name: "vnil->wnil, accept w, seens are merged",
			inst: &Ins{InsId: NewInsId(column, lsn, 0), Val: cmdV.Clone(), VBal: nil, Deps: []int64{0, 1, 5}},
			logs: []*Ins{
				{InsId: NewInsId(column, lsn, 1), Val: cmdW.Clone(), VBal: nil, Deps: []int64{3, 0, 3}},
			},
			wantInst: "<1-0-1: <W=0> vbal:10,2 c:false seen:3,1,5>",
			wantLogs: "<1-0-1: <W=0> vbal:10,2 c:false seen:3,1,5>",
		},
		{
			name: "v1->nil, just commit",
			inst: &Ins{InsId: NewInsId(column, lsn, 0), Val: cmdV.Clone(), VBal: NewBal(1, 0), Deps: []int64{0, 1, 5}},
			op:   Op_Commit,
			logs: []*Ins{},

			wantInst: "<1-0-0: <V=0> vbal:1,0 c:true seen:0,1,5>",
			wantLogs: "<1-0-0: <V=0> vbal:1,0 c:true seen:0,1,5>",
		},
		{
			name: "v0->w1, just commit",
			inst: &Ins{InsId: NewInsId(column, lsn, 0), Val: cmdV.Clone(), VBal: NewBal(0, 0), Deps: []int64{0, 1, 5}},
			op:   Op_Commit,
			logs: []*Ins{
				{InsId: NewInsId(column, lsn, 1), Val: cmdW.Clone(), VBal: NewBal(2, 1), Deps: []int64{3, 0, 3}},
			},
			wantInst: "<1-0-0: <V=0> vbal:0,0 c:true seen:0,1,5>",
			wantLogs: "<1-0-0: <V=0> vbal:0,0 c:true seen:0,1,5>",
		},
	}

	s := NewKVServer(1)
	for i, c := range cases {
		mes := fmt.Sprintf("%d-th: case: %+v", i+1, c)

		// a log to let prepared instance update its Deps
		s.log.columns[0].Log = []*Ins{
			{},
		}
		s.log.columns[column].Log = c.logs

		req := &Request{
			Instances: map[int64]*Ins{c.inst.getLSN(): c.inst},
			Bal:       NewBal(10, 2),
		}
		if c.op != Op_Noop {
			req.Op = c.op
		}
		inst := s.hdlOps(req, c.inst)

		ta.Equal(c.wantInst, inst.str(), mes)
		ta.Equal(c.wantLogs, logsStr(s.log.columns[column].Log), mes)
	}
}
