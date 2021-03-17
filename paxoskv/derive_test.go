package paxoskv

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestBallotNum_Less(t *testing.T) {
	ta := require.New(t)

	cases := []struct {
		a, b *BallotNum
		want bool
	}{
		{nil, nil, false},
		{&BallotNum{}, nil, false},
		{nil, &BallotNum{}, true},
		{&BallotNum{N: 1}, &BallotNum{N: 1}, false},
		{&BallotNum{N: 2}, &BallotNum{N: 1}, false},
		{&BallotNum{N: 1}, &BallotNum{N: 2}, true},
		{&BallotNum{N: 2, Id: 1}, &BallotNum{N: 2, Id: 1}, false},
		{&BallotNum{N: 2, Id: 2}, &BallotNum{N: 2, Id: 1}, false},
		{&BallotNum{N: 2, Id: 1}, &BallotNum{N: 2, Id: 2}, true},
	}

	for i, c := range cases {
		mes := fmt.Sprintf("%d-th: case: %+v %s %s", i+1, c, c.a.str(), c.b.str())
		ta.Equal(c.want, c.a.Less(c.b), mes)
	}

}

func TestAcceptor_str(t *testing.T) {

	ta := require.New(t)

	cases := []struct {
		acc  *Instance
		want string
	}{
		{
			acc: &Instance{
				Val:       NewCmd("yy", 3),
				VBal:      &BallotNum{N: 3, Id: 4},
				Seen:      []int64{1, 2, 3},
				Committed: true,
			},
			want: "<v:<2-3-1:yy=3> vbal:3,4 c:true seen:1,2,3>",
		},
	}

	for i, c := range cases {
		got := c.acc.str()
		ta.Equal(c.want, got, "%d-th: case: %+v", i+1, c)
	}
}
