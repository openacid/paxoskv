package paxoskv

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestApplierSCC_buildDepGraph(t *testing.T) {

	ta := require.New(t)

	cases := []struct {
		name        string
		columns     []*Column
		nextCommits []int64
		nextApply   []int64
		want        graph
		wantErr     error
	}{
		{
			name: "none committed",
			columns: []*Column{
				{Log: []*Ins{nil, {Seen: []int64{0, 2, 0}}}},
				{},
				{},
			},
			nextCommits: []int64{0, 0, 0},
			nextApply:   []int64{1, 1, 1},
			want:        nil,
			wantErr:     &UncommittedErr{Column: 0},
		},
		{
			name: "committed, dep not committed",
			columns: []*Column{
				{Log: []*Ins{nil, {Seen: []int64{0, 2, 0}}}},
				{Log: []*Ins{nil, {Seen: []int64{0, 1, 0}}}},
				{},
			},
			nextCommits: []int64{2, 1, 0},
			nextApply:   []int64{1, 1, 0},
			want:        nil,
			wantErr:     &UncommittedErr{Column: 1},
		},
		{
			name: "not committed, dep committed",
			columns: []*Column{
				{Log: []*Ins{nil, {Seen: []int64{0, 2, 0}}}},
				{Log: []*Ins{nil, {Seen: []int64{0, 1, 0}}}},
				{},
			},
			nextCommits: []int64{1, 2, 0},
			nextApply:   []int64{1, 1, 1},
			want:        nil,
			wantErr:     &UncommittedErr{Column: 0},
		},
		{
			name: "0->1",
			columns: []*Column{
				{Log: []*Ins{nil, {Seen: []int64{0, 2, 0}}}},
				{Log: []*Ins{nil, {Seen: []int64{0, 0, 0}}}},
				{},
			},
			nextCommits: []int64{2, 2, 0},
			nextApply:   []int64{1, 1, 0},
			want:        graph{0: []int64{1}},
			wantErr:     nil,
		},
		{
			name: "0->1<>2",
			columns: []*Column{
				{Log: []*Ins{nil, {Seen: []int64{0, 2, 0}}}},
				{Log: []*Ins{nil, {Seen: []int64{0, 0, 2}}}},
				{Log: []*Ins{nil, {Seen: []int64{0, 2, 0}}}},
			},
			nextCommits: []int64{2, 2, 2},
			nextApply:   []int64{1, 1, 1},
			want:        graph{0: []int64{1}, 1: []int64{2}, 2: []int64{1}},
			wantErr:     nil,
		},
	}

	for i, c := range cases {
		s := NewKVServer(1)
		s.log.columns = c.columns
		s.log.nextCommits = c.nextCommits
		s.stateMachine.nextApplies = c.nextApply

		fmt.Println(c.name)

		column := int64(0)
		a := &ApplierSCC{}
		d := NewApplyData(s.log, s.stateMachine, s.getLogger())
		got, err := a.buildDepGraph(d, column)

		ta.Equal(c.wantErr, err, "%d-th: case: %+v", i+1, c)
		ta.Equal(c.want, got, "%d-th: case: %+v", i+1, c)
	}
}

func TestApplierSCC_getColumnToApply(t *testing.T) {

	ta := require.New(t)

	cases := []struct {
		name        string
		columns     []*Column
		column      int64
		nextCommits []int64
		nextApply   []int64
		wantFirst   int64
		wantErr     error
	}{
		{
			name: "no deps, not committed",
			columns: []*Column{
				{Log: []*Ins{nil, {Seen: []int64{1, 1, 1}}}},
				{},
				{},
			},
			nextCommits: []int64{1, 0, 0},
			nextApply:   []int64{1, 1, 1},
			column:      0,
			wantFirst:   0,
			wantErr:     &UncommittedErr{},
		},
		{
			name: "no deps, committed",
			columns: []*Column{
				{Log: []*Ins{nil, {Seen: []int64{1, 1, 1}}}},
				{},
				{},
			},
			nextCommits: []int64{2, 0, 0},
			nextApply:   []int64{1, 1, 1},
			column:      0,
			wantFirst:   0,
			wantErr:     nil,
		},
		{
			name: "none committed",
			columns: []*Column{
				{Log: []*Ins{nil, {Seen: []int64{0, 2, 0}}}},
				{},
				{},
			},
			nextCommits: []int64{1, 0, 0},
			nextApply:   []int64{1, 1, 1},
			column:      0,
			wantFirst:   0,
			wantErr:     &UncommittedErr{},
		},
		{
			name: "committed, dep not committed",
			columns: []*Column{
				{Log: []*Ins{nil, {Seen: []int64{0, 2, 0}}}},
				{Log: []*Ins{nil, {Seen: []int64{0, 1, 0}}}},
				{},
			},
			nextCommits: []int64{2, 1, 0},
			nextApply:   []int64{1, 1, 1},
			column:      0,
			wantFirst:   0,
			wantErr:     &UncommittedErr{},
		},
		{
			name: "not committed, dep committed, skip current and apply dep",
			columns: []*Column{
				{Log: []*Ins{nil, {Seen: []int64{0, 2, 0}}}},
				{Log: []*Ins{nil, {Seen: []int64{0, 1, 0}}}},
				{},
			},
			nextCommits: []int64{1, 2, 0},
			nextApply:   []int64{1, 1, 1},
			column:      0,
			wantFirst:   0,
			wantErr:     &UncommittedErr{},
		},
		{
			name: "not committed, dep committed, apply the dep",
			columns: []*Column{
				{Log: []*Ins{nil, {Seen: []int64{0, 2, 0}}}},
				{Log: []*Ins{nil, {Seen: []int64{0, 1, 0}}}},
				{},
			},
			nextCommits: []int64{1, 2, 0},
			nextApply:   []int64{1, 1, 1},
			column:      1,
			wantFirst:   1,
			wantErr:     nil,
		},
		{
			name: "0->1 from 0",
			columns: []*Column{
				{Log: []*Ins{nil, {Seen: []int64{0, 2, 0}}}},
				{Log: []*Ins{nil, {Seen: []int64{0, 0, 0}}}},
				{},
			},
			nextCommits: []int64{2, 2, 0},
			nextApply:   []int64{1, 1, 1},
			column:      0,
			wantFirst:   1,
			wantErr:     nil,
		},
		{
			name: "0->1 from 1",
			columns: []*Column{
				{Log: []*Ins{nil, {Seen: []int64{0, 2, 0}}}},
				{Log: []*Ins{nil, {Seen: []int64{0, 0, 0}}}},
				{},
			},
			nextCommits: []int64{2, 2, 0},
			nextApply:   []int64{1, 1, 1},
			column:      1,
			wantFirst:   1,
			wantErr:     nil,
		},
		{
			name: "0->1<>2, 0->2",
			columns: []*Column{
				{Log: []*Ins{nil, {Seen: []int64{0, 2, 2}}}},
				{Log: []*Ins{nil, {Seen: []int64{0, 0, 2}}}},
				{Log: []*Ins{nil, {Seen: []int64{0, 2, 0}}}},
			},
			nextCommits: []int64{2, 2, 2},
			nextApply:   []int64{1, 1, 1},
			column:      0,
			wantFirst:   1,
			wantErr:     nil,
		},
		{
			name: "0->1<>2->0, from 0",
			columns: []*Column{
				{Log: []*Ins{nil, {Seen: []int64{0, 2, 1}}}},
				{Log: []*Ins{nil, {Seen: []int64{0, 0, 2}}}},
				{Log: []*Ins{nil, {Seen: []int64{2, 2, 0}}}},
			},
			nextCommits: []int64{2, 2, 2},
			nextApply:   []int64{1, 1, 1},
			column:      0,
			wantFirst:   0,
			wantErr:     nil,
		},
		{
			name: "0->1<>2->0, from 1",
			columns: []*Column{
				{Log: []*Ins{nil, {Seen: []int64{0, 2, 1}}}},
				{Log: []*Ins{nil, {Seen: []int64{0, 0, 2}}}},
				{Log: []*Ins{nil, {Seen: []int64{2, 2, 0}}}},
			},
			nextCommits: []int64{2, 2, 2},
			nextApply:   []int64{1, 1, 1},
			column:      1,
			wantFirst:   0,
			wantErr:     nil,
		},
		{
			name: "0->1<>2->0, from 2",
			columns: []*Column{
				{Log: []*Ins{nil, {Seen: []int64{0, 2, 1}}}},
				{Log: []*Ins{nil, {Seen: []int64{0, 0, 2}}}},
				{Log: []*Ins{nil, {Seen: []int64{2, 2, 0}}}},
			},
			nextCommits: []int64{2, 2, 2},
			nextApply:   []int64{1, 1, 1},
			column:      2,
			wantFirst:   0,
			wantErr:     nil,
		},
		{
			name: "0->1->2->0, from 1",
			columns: []*Column{
				{Log: []*Ins{{Seen: []int64{0, 1, 0}}}},
				{Log: []*Ins{{Seen: []int64{0, 0, 1}}}},
				{Log: []*Ins{{Seen: []int64{1, 0, 0}}}},
			},
			nextCommits: []int64{1, 1, 1},
			nextApply:   []int64{0, 0, 0},
			column:      1,
			wantFirst:   0,
			wantErr:     nil,
		},
		{
			name: "0<-1<-2, 0 not commit",
			columns: []*Column{
				{Log: []*Ins{{Seen: []int64{0, 0, 0}}}},
				{Log: []*Ins{{Seen: []int64{1, 0, 0}}}},
				{Log: []*Ins{{Seen: []int64{0, 1, 0}}}},
			},
			nextCommits: []int64{0, 1, 1},
			nextApply:   []int64{0, 0, 0},
			column:      2,
			wantFirst:   0,
			wantErr:     &UncommittedErr{},
		},
	}

	for i, c := range cases {
		mes := fmt.Sprintf("%d-th: start from column: %d, case: %+v", i+1, c.column, c)
		s := NewKVServer(1)
		s.log.columns = c.columns
		s.log.nextCommits = c.nextCommits
		s.stateMachine.nextApplies = c.nextApply

		fmt.Println(c.name)

		a := &ApplierSCC{}
		d := NewApplyData(s.log, s.stateMachine, nil)
		n, err := a.getColumnToApply(d, c.column)
		if c.wantErr != nil {
			ta.NotNil(err, mes)
		} else {
			ta.Nil(err, mes)
		}
		ta.Equal(c.wantFirst, n, mes)
	}
}
