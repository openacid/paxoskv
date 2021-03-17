package paxoskv

import (
	"fmt"
	"github.com/stretchr/testify/require"
)

import "testing"

func TestTarjan(t*testing.T) {

	ta := require.New(t)

	cases := []struct {
		name string
		g graph
		want string

	}{
		{
			name: "1 node",
			g: graph{
				0: []int64{},
			},
			want:"[[0]]",
		},
		{
			name: "0->2",
			g: graph{
				0: []int64{2},
				2: []int64{},
			},
			want:"[[2] [0]]",
		},
		{
			name: "0->1<>2",
			g: graph{
				0: []int64{1},
				1: []int64{2},
				2: []int64{1},
			},
			want:"[[2 1] [0]]",
		},
		{
			name: "0<>1<>2",
			g: graph{
				0: []int64{1},
				1: []int64{0,2},
				2: []int64{1},
			},
			want:"[[2 1 0]]",
		},
		{
			name: "0->1->2->0",
			g: graph{
				0: []int64{1},
				1: []int64{2},
				2: []int64{0},
			},
			want:"[[2 1 0]]",
		},
	}

	for i, c := range cases {

		got := findSCC(c.g, 0)

		ta.Equal(c.want, fmt.Sprintf("%v", got), "%d-th: case: %+v", i+1, c)
	}
}
