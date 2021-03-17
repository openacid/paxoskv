package paxoskv

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestKVServer_apply(t *testing.T) {

	ta := require.New(t)

	cases := []struct {
		name         string
		fromColumn   []*Column
		nextCommits  []int64
		nextApply    []int64
		wantSnapshot map[string]int64
		wantN        int
	}{
		{
			name: "no deps",
			fromColumn: []*Column{
				{Log: []*Ins{nil, {InsId: NewInsId(0, 1, 0), Val: &Cmd{Key: "x", Vi64: 1}, Deps: []int64{1, 1, 1}, Committed: true}}},
				{},
				{},
			},
			nextCommits:  []int64{2, 0, 0},
			nextApply:    []int64{1, 1, 1},
			wantSnapshot: map[string]int64{"x": 1},
			wantN:        1,
		},
		{
			name: "depends on a nil log",
			fromColumn: []*Column{
				{Log: []*Ins{nil, {Deps: []int64{1, 0, 0}}}},
				{},
				{},
			},
			nextCommits:  []int64{0, 0, 0},
			nextApply:    []int64{0, 1, 1},
			wantSnapshot: map[string]int64{},
			wantN:        0,
		},
		{
			name: "none committed",
			fromColumn: []*Column{
				{Log: []*Ins{nil, {InsId: NewInsId(0, 1, 0), Deps: []int64{0, 2, 0}}}},
				{},
				{},
			},
			nextCommits:  []int64{2, 1, 1},
			nextApply:    []int64{1, 1, 1},
			wantSnapshot: map[string]int64{},
			wantN:        0,
		},
		{
			name: "committed, dep not committed",
			fromColumn: []*Column{
				{column: 0, Log: []*Ins{nil, {InsId: NewInsId(0, 1, 0), Deps: []int64{0, 2, 0}, Committed: true}}},
				{column: 1, Log: []*Ins{nil, {InsId: NewInsId(1, 1, 0), Deps: []int64{0, 1, 0}, Committed: false}}},
				{column: 2},
			},
			nextCommits:  []int64{2, 1, 0},
			nextApply:    []int64{1, 1, 1},
			wantSnapshot: map[string]int64{},
			wantN:        0,
		},
		{
			name: "not committed, dep committed, only dep applied",
			fromColumn: []*Column{
				{Log: []*Ins{nil, {InsId: NewInsId(0, 1, 0), Deps: []int64{0, 2, 0}}}},
				{Log: []*Ins{nil, {InsId: NewInsId(1, 1, 0), Val: &Cmd{Key: "y", Vi64: 2}, Deps: []int64{0, 1, 0}, Committed: true}}},
				{},
			},
			nextCommits:  []int64{1, 2, 0},
			nextApply:    []int64{1, 1, 1},
			wantSnapshot: map[string]int64{"y": 2},
			wantN:        1,
		},
		{
			name: "0->1 xx",
			fromColumn: []*Column{
				{column: 0, Log: []*Ins{nil, {InsId: NewInsId(0, 1, 0), Val: &Cmd{Key: "x", Vi64: 1}, Deps: []int64{0, 2, 0}, Committed: true}}},
				{column: 1, Log: []*Ins{nil, {InsId: NewInsId(1, 1, 0), Val: &Cmd{Key: "x", Vi64: 2}, Deps: []int64{0, 0, 0}, Committed: true}}},
				{},
			},
			nextCommits:  []int64{2, 2, 0},
			nextApply:    []int64{1, 1, 1},
			wantSnapshot: map[string]int64{"x": 1},
			wantN:        2,
		},
		{
			name: "0->1, xy",
			fromColumn: []*Column{
				{column: 0, Log: []*Ins{nil, {InsId: NewInsId(0, 1, 0), Val: &Cmd{Key: "x", Vi64: 1}, Deps: []int64{0, 2, 0}, Committed: true}}},
				{column: 1, Log: []*Ins{nil, {InsId: NewInsId(1, 1, 0), Val: &Cmd{Key: "y", Vi64: 2}, Deps: []int64{0, 0, 0}, Committed: true}}},
				{},
			},
			nextCommits:  []int64{2, 2, 0},
			nextApply:    []int64{1, 1, 1},
			wantSnapshot: map[string]int64{"x": 1, "y": 2},
			wantN:        2,
		},
		{
			name: "0->1<>2, 0->2, xyz",
			fromColumn: []*Column{
				{column: 0, Log: []*Ins{nil, {InsId: NewInsId(0, 1, 0), Val: &Cmd{Key: "x", Vi64: 1}, Deps: []int64{0, 2, 2}, Committed: true}}},
				{column: 1, Log: []*Ins{nil, {InsId: NewInsId(1, 1, 0), Val: &Cmd{Key: "y", Vi64: 2}, Deps: []int64{0, 0, 2}, Committed: true}}},
				{column: 2, Log: []*Ins{nil, {InsId: NewInsId(2, 1, 0), Val: &Cmd{Key: "z", Vi64: 3}, Deps: []int64{0, 2, 0}, Committed: true}}},
			},
			nextCommits:  []int64{2, 2, 2},
			nextApply:    []int64{1, 1, 1},
			wantSnapshot: map[string]int64{"x": 1, "y": 2, "z": 3},
			wantN:        3,
		},
		{
			name: "0->1<>2, 0->2, yyz",
			fromColumn: []*Column{
				{column: 0, Log: []*Ins{nil, {InsId: NewInsId(0, 1, 0), Val: &Cmd{Key: "y", Vi64: 1}, Deps: []int64{0, 2, 2}, Committed: true}}},
				{column: 1, Log: []*Ins{nil, {InsId: NewInsId(1, 1, 0), Val: &Cmd{Key: "y", Vi64: 2}, Deps: []int64{0, 0, 2}, Committed: true}}},
				{column: 2, Log: []*Ins{nil, {InsId: NewInsId(2, 1, 0), Val: &Cmd{Key: "z", Vi64: 3}, Deps: []int64{0, 2, 0}, Committed: true}}},
			},
			nextCommits:  []int64{2, 2, 2},
			nextApply:    []int64{1, 1, 1},
			wantSnapshot: map[string]int64{"y": 1, "z": 3},
			wantN:        3,
		},
		{
			name: "0->1<>2->0, xyz",
			fromColumn: []*Column{
				{column: 0, Log: []*Ins{nil, {InsId: NewInsId(0, 1, 0), Val: &Cmd{Key: "x", Vi64: 1}, Deps: []int64{0, 2, 0}, Committed: true}}},
				{column: 1, Log: []*Ins{nil, {InsId: NewInsId(1, 1, 0), Val: &Cmd{Key: "y", Vi64: 2}, Deps: []int64{0, 0, 2}, Committed: true}}},
				{column: 2, Log: []*Ins{nil, {InsId: NewInsId(2, 1, 0), Val: &Cmd{Key: "z", Vi64: 3}, Deps: []int64{2, 2, 0}, Committed: true}}},
			},
			nextCommits:  []int64{2, 2, 2},
			nextApply:    []int64{1, 1, 1},
			wantSnapshot: map[string]int64{"x": 1, "y": 2, "z": 3},
			wantN:        3,
		},
		{
			name: "0->1<>2, 0->2 zyz",
			fromColumn: []*Column{
				{column: 0, Log: []*Ins{nil, {InsId: NewInsId(0, 1, 0), Val: &Cmd{Key: "z", Vi64: 1}, Deps: []int64{0, 2, 2}, Committed: true}}},
				{column: 1, Log: []*Ins{nil, {InsId: NewInsId(1, 1, 0), Val: &Cmd{Key: "y", Vi64: 2}, Deps: []int64{0, 0, 2}, Committed: true}}},
				{column: 2, Log: []*Ins{nil, {InsId: NewInsId(2, 1, 0), Val: &Cmd{Key: "z", Vi64: 3}, Deps: []int64{0, 2, 0}, Committed: true}}},
			},
			nextCommits:  []int64{2, 2, 2},
			nextApply:    []int64{1, 1, 1},
			wantSnapshot: map[string]int64{"z": 1, "y": 2},
			wantN:        3,
		},
		{
			name: "0->1<>2->0 zyz",
			fromColumn: []*Column{
				{column: 0, Log: []*Ins{nil, {InsId: NewInsId(0, 1, 0), Val: &Cmd{Key: "z", Vi64: 1}, Deps: []int64{0, 2, 0}, Committed: true}}},
				{column: 1, Log: []*Ins{nil, {InsId: NewInsId(1, 1, 0), Val: &Cmd{Key: "y", Vi64: 2}, Deps: []int64{0, 0, 2}, Committed: true}}},
				{column: 2, Log: []*Ins{nil, {InsId: NewInsId(2, 1, 0), Val: &Cmd{Key: "z", Vi64: 3}, Deps: []int64{2, 2, 0}, Committed: true}}},
			},
			nextCommits:  []int64{2, 2, 2},
			nextApply:    []int64{1, 1, 1},
			wantSnapshot: map[string]int64{"z": 3, "y": 2},
			wantN:        3,
		},
		{
			name: "no log to apply",
			fromColumn: []*Column{
				{Log: []*Ins{nil, {Deps: []int64{0, 2, 0}, Committed: true}}},
				{Log: []*Ins{nil, {Deps: []int64{0, 0, 2}, Committed: true}}},
				{Log: []*Ins{nil, {Deps: []int64{0, 2, 0}, Committed: true}}},
			},
			nextCommits:  []int64{0, 0, 0},
			nextApply:    []int64{2, 2, 2},
			wantSnapshot: map[string]int64{},
			wantN:        0,
		},
	}

	for i, c := range cases {
		mes := fmt.Sprintf("%d-th: case: %+v", i+1, c)
		s := NewKVServer(1)
		s.log.columns = c.fromColumn
		s.log.nextCommits = c.nextCommits
		s.stateMachine.nextApplies = c.nextApply

		fmt.Println(c.name)

		h := NewHandler(s, nil)
		n, err := h.apply()
		_ = err

		st := s.stateMachine.getState()

		ta.Equal(c.wantN, n)
		ta.Equal(c.wantSnapshot, st, mes)
	}
}
