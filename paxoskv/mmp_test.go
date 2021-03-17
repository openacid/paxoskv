package paxoskv

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestKVServer_hdlPrepareInstance(t *testing.T) {
	ta := require.New(t)

	s := NewKVServer(1)
	s.columns[0] = &columnT{
		LastBal: &BallotNum{N: 0, Id: 0},
		Log: []*Instance{
			nil,
			{
				Val: &Cmd{
					ValueId: newVid(0, 1, 0),
					Key:     "foo",
					Vi64:    10,
				},
				Seen: []int64{3, 0, 3},
			},
		},
	}

	{
		// prepare a nil instnace 0-0
		lsn := int64(0)
		inst := &Instance{
			Val: &Cmd{
				ValueId: newVid(0, lsn, 1),
			},
			Seen: []int64{1, 0, 5},
		}

		h := NewHandler(s, inst)
		gotInst, err := h.hdlPrepareInstance(inst)
		ta.Equal("<v:<0-0-1:=0> vbal:nil c:false seen:2,0,5>", gotInst.str())
		ta.Equal("<v:<0-0-1:=0> vbal:nil c:false seen:2,0,5>", s.columns[0].Log[lsn].str())
		ta.Equal(nil, err)
	}

	{
		// prepare a non-nil instnace 0-1
		lsn := int64(1)
		inst := &Instance{
			Val: &Cmd{
				ValueId: newVid(0, lsn, 3),
			},
			Seen: []int64{1, 0, 5},
		}

		h := NewHandler(s, inst)
		gotInst, err := h.hdlPrepareInstance(inst)
		ta.Equal("<v:<0-1-0:foo=10> vbal:nil c:false seen:3,0,3>", gotInst.str())
		ta.Equal("<v:<0-1-0:foo=10> vbal:nil c:false seen:3,0,3>", s.columns[0].Log[lsn].str())
		ta.Equal(AlreadyPrepared, err)
	}

	{
		// prepare a nil instnace 0-2
		lsn := int64(2)
		inst := &Instance{
			Val: &Cmd{
				ValueId: newVid(0, lsn, 1),
			},
			Seen: []int64{1, 0, 5},
		}

		h := NewHandler(s, inst)
		gotInst, err := h.hdlPrepareInstance(inst)
		ta.Equal("<v:<0-2-1:=0> vbal:nil c:false seen:2,0,5>", gotInst.str())
		ta.Equal("<v:<0-2-1:=0> vbal:nil c:false seen:2,0,5>", s.columns[0].Log[lsn].str())
		ta.Equal(nil, err)
	}

}
func TestKVServer_hdlOps(t *testing.T) {
	ta := require.New(t)

	column := int64(1)
	lsn := int64(0)

	cmdV := &Cmd{
		ValueId: newVid(column, lsn, 0),
		Key:     "V",
	}
	cmdW := &Cmd{
		ValueId: newVid(column, lsn, 1),
		Key:     "W",
	}

	cases := []struct {
		name string
		inst *Instance
		op   Op
		// server:
		logs     []*Instance
		wantInst string
		wantLogs string
	}{
		{
			name: "vnil->nil",
			inst: &Instance{
				Val:  cmdV.Clone(),
				VBal: nil,
				Seen: []int64{0, 1, 5},
			},
			logs: []*Instance{},

			wantInst: "<v:<1-0-0:V=0> vbal:10,2 c:false seen:1,1,5>",
			wantLogs: "<v:<1-0-0:V=0> vbal:10,2 c:false seen:1,1,5>",
		},
		{
			name: "v1->nil, no seen updated",
			inst: &Instance{
				Val:  cmdV.Clone(),
				VBal: newBal(1, 0),
				Seen: []int64{0, 1, 5},
			},
			logs: []*Instance{},

			wantInst: "<v:<1-0-0:V=0> vbal:10,2 c:false seen:0,1,5>",
			wantLogs: "<v:<1-0-0:V=0> vbal:10,2 c:false seen:0,1,5>",
		},
		{
			name: "v0->w1",
			inst: &Instance{
				Val:  cmdV.Clone(),
				VBal: newBal(0, 0),
				Seen: []int64{0, 1, 5},
			},
			logs: []*Instance{
				{
					Val:  cmdW.Clone(),
					VBal: newBal(2, 1),
					Seen: []int64{3, 0, 3},
				},
			},
			wantInst: "<v:<1-0-1:W=0> vbal:10,2 c:false seen:3,0,3>",
			wantLogs: "<v:<1-0-1:W=0> vbal:10,2 c:false seen:3,0,3>",
		},
		{
			name: "v2->w1",
			inst: &Instance{
				Val:  cmdV.Clone(),
				VBal: newBal(2, 0),
				Seen: []int64{0, 1, 5},
			},
			logs: []*Instance{
				{
					Val:  cmdW.Clone(),
					VBal: newBal(1, 1),
					Seen: []int64{3, 0, 3},
				},
			},
			wantInst: "<v:<1-0-0:V=0> vbal:10,2 c:true seen:0,1,5>",
			wantLogs: "<v:<1-0-0:V=0> vbal:10,2 c:true seen:0,1,5>",
		},
		{
			name: "vnil->wnil, accept w, seens are merged",
			inst: &Instance{
				Val:  cmdV.Clone(),
				VBal: nil,
				Seen: []int64{0, 1, 5},
			},
			logs: []*Instance{
				{
					Val:  cmdW.Clone(),
					VBal: nil,
					Seen: []int64{3, 0, 3},
				},
			},
			wantInst: "<v:<1-0-1:W=0> vbal:10,2 c:false seen:3,1,5>",
			wantLogs: "<v:<1-0-1:W=0> vbal:10,2 c:false seen:3,1,5>",
		},
		{
			name: "v1->nil, just commit",
			inst: &Instance{
				Val:  cmdV.Clone(),
				VBal: newBal(1, 0),
				Seen: []int64{0, 1, 5},
			},
			op:   Op_Commit,
			logs: []*Instance{},

			wantInst: "<v:<1-0-0:V=0> vbal:1,0 c:true seen:0,1,5>",
			wantLogs: "<v:<1-0-0:V=0> vbal:1,0 c:true seen:0,1,5>",
		},
		{
			name: "v0->w1, just commit",
			inst: &Instance{
				Val:  cmdV.Clone(),
				VBal: newBal(0, 0),
				Seen: []int64{0, 1, 5},
			},
			op: Op_Commit,
			logs: []*Instance{
				{
					Val:  cmdW.Clone(),
					VBal: newBal(2, 1),
					Seen: []int64{3, 0, 3},
				},
			},
			wantInst: "<v:<1-0-0:V=0> vbal:0,0 c:true seen:0,1,5>",
			wantLogs: "<v:<1-0-0:V=0> vbal:0,0 c:true seen:0,1,5>",
		},
	}

	s := NewKVServer(1)
	for i, c := range cases {
		mes := fmt.Sprintf("%d-th: case: %+v", i+1, c)

		// a log to let prepared instance update its Seen
		s.columns[0].Log = []*Instance{
			{},
		}
		s.columns[column].Log = c.logs

		req := &Request{
			Instances: map[int64]*Instance{c.inst.getLSN(): c.inst},
			Bal:       newBal(10, 2),
		}
		if c.op != Op_Noop {
			req.Ops = []Op{c.op}
		}
		inst := s.handleOps(req, c.inst)

		ta.Equal(c.wantInst, inst.str(), mes)
		ta.Equal(c.wantLogs, logsStr(s.columns[column].Log), mes)
	}
}
