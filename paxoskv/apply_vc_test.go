package paxoskv

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestApplierVC_getColumnToApply(t *testing.T) {

	ta := require.New(t)

	cases := []struct {
		name        string
		columns     []*Column
		nextCommits []int64
		nextApply   []int64
		column      int64
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
			name: "depends on a nil log",
			columns: []*Column{
				{Log: []*Ins{nil, {Seen: []int64{1, 0, 0}}}},
				{},
				{},
			},
			nextCommits: []int64{0, 0, 0},
			nextApply:   []int64{0, 1, 1},
			column:      0,
			wantFirst:   0,
			wantErr:     &UncommittedErr{},
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
			name: "not committed, dep committed",
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
			name: "not committed, dep committed apply the dep",
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
			name: "0->1->2, 0->2, from 1, 1 has high vec",
			columns: []*Column{
				{Log: []*Ins{{Seen: []int64{0, 1, 0}}}},
				{Log: []*Ins{{Seen: []int64{0, 9, 1}}}},
				{Log: []*Ins{{Seen: []int64{0, 0, 0}}}},
			},
			nextCommits: []int64{1, 1, 1},
			nextApply:   []int64{0, 0, 0},
			column:      1,
			wantFirst:   2,
			wantErr:     nil,
		},
		{
			name: "0->1, from 0, 1 has high vec",
			columns: []*Column{
				{Log: []*Ins{{Seen: []int64{0, 1, 0}}}},
				{Log: []*Ins{{Seen: []int64{0, 9, 1}}}},
				{Log: []*Ins{{Seen: []int64{0, 0, 0}}}},
			},
			nextCommits: []int64{1, 1, 1},
			nextApply:   []int64{0, 0, 1},
			column:      0,
			wantFirst:   1,
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
			column:      0,
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

		a := &ApplierVC{}
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
