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
