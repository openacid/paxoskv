package paxoskv

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestKVServer_allocNewInst(t *testing.T) {

	ta := require.New(t)

	cases := []struct {
		name     string
		column   int64
		cmd      *Cmd
		wantInst string
		wantLogs string
	}{
		{
			name:     "010",
			column:   1,
			cmd:      NewCmd("x", 1),
			wantInst: "<1-0-1: <x=1> vbal:nil c:false seen:0,1,0>",
			wantLogs: "<1-0-1: <x=1> vbal:nil c:false seen:0,1,0>",
		},
		{
			name:     "110",
			column:   0,
			cmd:      NewCmd("y", 1),
			wantInst: "<0-0-1: <y=1> vbal:nil c:false seen:1,1,0>",
			wantLogs: "<0-0-1: <y=1> vbal:nil c:false seen:1,1,0>",
		},
		{
			name:     "210",
			column:   0,
			cmd:      NewCmd("z", 1),
			wantInst: "<0-1-1: <z=1> vbal:nil c:false seen:2,1,0>",
			wantLogs: "<0-0-1: <y=1> vbal:nil c:false seen:1,1,0>,<0-1-1: <z=1> vbal:nil c:false seen:2,1,0>",
		},
		{
			name:     "211",
			column:   2,
			cmd:      NewCmd("u", 1),
			wantInst: "<2-0-1: <u=1> vbal:nil c:false seen:2,1,1>",
			wantLogs: "<2-0-1: <u=1> vbal:nil c:false seen:2,1,1>",
		},
	}

	s := NewKVServer(1)
	for i, c := range cases {
		mes := fmt.Sprintf("%d-th: case: %+v", i+1, c)

		inst := s.allocNewInst(c.column, c.cmd)

		ta.Equal(c.wantInst, inst.str(), mes)
		ta.Equal(c.wantLogs, logsStr(s.log.columns[c.column].Log), mes)
	}
}
