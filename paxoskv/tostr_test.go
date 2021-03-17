package paxoskv

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestInstance_str(t *testing.T) {

	ta := require.New(t)

	cases := []struct {
		acc  *Ins
		want string
	}{
		{
			acc: &Ins{
				InsId:     NewInsId(2, 3, 1),
				Val:       NewCmd("yy", 3),
				VBal:      &BallotNum{N: 3, Id: 4},
				Seen:      []int64{1, 2, 3},
				Committed: true,
			},
			want: "<2-3-1: <yy=3> vbal:3,4 c:true seen:1,2,3>",
		},
	}

	for i, c := range cases {
		got := c.acc.str()
		ta.Equal(c.want, got, "%d-th: case: %+v", i+1, c)
	}
}
