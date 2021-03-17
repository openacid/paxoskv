package paxoskv

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestKVServer_getDep(t *testing.T) {

	ta := require.New(t)

	cases := []struct {
		name    string
		columns []*columnT
		column  int64
		want    []int64
	}{
		{
			name: "no dep",
			columns: []*columnT{
				{nextApply: 1, Log: []*Instance{nil, {Seen: []int64{0, 0, 0}}}},
				{nextApply: 1},
				{nextApply: 1},
			},
			column: 0,
			want:   []int64{},
		},
		{
			name: "one dep",
			columns: []*columnT{
				{nextApply: 1, Log: []*Instance{nil, {Seen: []int64{0, 2, 0}}}},
				{nextApply: 1},
				{nextApply: 1},
			},
			column: 0,
			want:   []int64{1},
		},
		{
			name: "2 dep",
			columns: []*columnT{
				{nextApply: 1, Log: []*Instance{nil, {Seen: []int64{0, 2, 2}}}},
				{nextApply: 1},
				{nextApply: 1},
			},
			column: 0,
			want:   []int64{1, 2},
		},
		{
			name: "do not dep on itself",
			columns: []*columnT{
				{nextApply: 1, Log: []*Instance{nil, {Seen: []int64{2, 2, 2}}}},
				{nextApply: 1},
				{nextApply: 1},
			},
			column: 0,
			want:   []int64{1, 2},
		},
	}

	for i, c := range cases {
		s := NewKVServer(1)
		s.columns = c.columns

		fmt.Println(c.name)

		got := s.getDep(c.column)

		ta.Equal(c.want, got, "%d-th: case: %+v", i+1, c)
	}

}

func TestKVServer_buildDepGraph(t *testing.T) {

	ta := require.New(t)

	cases := []struct {
		name    string
		columns []*columnT
		want    graph
		wantErr error
	}{
		{
			name: "none committed",
			columns: []*columnT{
				{nextApply: 1, Log: []*Instance{nil, {Seen: []int64{0, 2, 0}}}},
				{nextApply: 1},
				{nextApply: 1},
			},
			want:    nil,
			wantErr: &NeedCommitError{Column: 0, Err: NotCommitted},
		},
		{
			name: "committed, dep not committed",
			columns: []*columnT{
				{nextApply: 1, Log: []*Instance{nil, {Seen: []int64{0, 2, 0}, Committed: true}}},
				{nextApply: 1, Log: []*Instance{nil, {Seen: []int64{0, 1, 0}, Committed: false}}},
				{nextApply: 1},
			},
			want:    nil,
			wantErr: &NeedCommitError{Column: 1, Err: NotCommitted},
		},
		{
			name: "not committed, dep committed",
			columns: []*columnT{
				{nextApply: 1, Log: []*Instance{nil, {Seen: []int64{0, 2, 0}}}},
				{nextApply: 1, Log: []*Instance{nil, {Seen: []int64{0, 1, 0}, Committed: true}}},
				{nextApply: 1},
			},
			want:    nil,
			wantErr: &NeedCommitError{Column: 0, Err: NotCommitted},
		},
		{
			name: "0->1",
			columns: []*columnT{
				{nextApply: 1, Log: []*Instance{nil, {Seen: []int64{0, 2, 0}, Committed: true}}},
				{nextApply: 1, Log: []*Instance{nil, {Seen: []int64{0, 0, 0}, Committed: true}}},
				{nextApply: 1},
			},
			want:    graph{0: []int64{1}},
			wantErr: nil,
		},
		{
			name: "0->1<>2",
			columns: []*columnT{
				{nextApply: 1, Log: []*Instance{nil, {Seen: []int64{0, 2, 0}, Committed: true}}},
				{nextApply: 1, Log: []*Instance{nil, {Seen: []int64{0, 0, 2}, Committed: true}}},
				{nextApply: 1, Log: []*Instance{nil, {Seen: []int64{0, 2, 0}, Committed: true}}},
			},
			want:    graph{0: []int64{1}, 1: []int64{2}, 2: []int64{1}},
			wantErr: nil,
		},
	}

	for i, c := range cases {
		s := NewKVServer(1)
		s.columns = c.columns

		fmt.Println(c.name)

		h := NewHandler(s, nil)

		column := int64(0)
		got, err := h.buildDepGraph(column)

		ta.Equal(c.wantErr, err, "%d-th: case: %+v", i+1, c)
		ta.Equal(c.want, got, "%d-th: case: %+v", i+1, c)
	}
}

func TestKVServer_apply(t *testing.T) {

	ta := require.New(t)

	cases := []struct {
		name    string
		columns []*columnT
		want    map[string]int64
		wantN   int
	}{
		{
			name: "no deps",
			columns: []*columnT{
				{nextApply: 1, Log: []*Instance{nil, {
					Val:  &Cmd{Key: "x", Vi64: 1},
					Seen: []int64{1, 1, 1}, Committed: true,
				}}},
				{nextApply: 1},
				{nextApply: 1},
			},
			want:  map[string]int64{"x": 1},
			wantN: 1,
		},
		{
			name: "depends on a nil log",
			columns: []*columnT{
				{nextApply: 0, Log: []*Instance{nil, {Seen: []int64{1, 0, 0}}}},
				{nextApply: 1},
				{nextApply: 1},
			},
			want:  map[string]int64{},
			wantN: 0,
		},
		{
			name: "none committed",
			columns: []*columnT{
				{nextApply: 1, Log: []*Instance{nil, {Seen: []int64{0, 2, 0}}}},
				{nextApply: 1},
				{nextApply: 1},
			},
			want:  map[string]int64{},
			wantN: 0,
		},
		{
			name: "committed, dep not committed",
			columns: []*columnT{
				{nextApply: 1, Log: []*Instance{nil, {Seen: []int64{0, 2, 0}, Committed: true}}},
				{nextApply: 1, Log: []*Instance{nil, {Seen: []int64{0, 1, 0}, Committed: false}}},
				{nextApply: 1},
			},
			want:  map[string]int64{},
			wantN: 0,
		},
		{
			name: "not committed, dep committed, only dep applied",
			columns: []*columnT{
				{nextApply: 1, Log: []*Instance{nil, {Seen: []int64{0, 2, 0}}}},
				{nextApply: 1, Log: []*Instance{nil, {
					Val:  &Cmd{Key: "y", Vi64: 2},
					Seen: []int64{0, 1, 0}, Committed: true,
				}}},
				{nextApply: 1},
			},
			want:  map[string]int64{"y": 2},
			wantN: 1,
		},
		{
			name: "0->1 xx",
			columns: []*columnT{
				{nextApply: 1, Log: []*Instance{nil, {
					Val:  &Cmd{Key: "x", Vi64: 1},
					Seen: []int64{0, 2, 0}, Committed: true,
				}}},
				{nextApply: 1, Log: []*Instance{nil, {
					Val:  &Cmd{Key: "x", Vi64: 2},
					Seen: []int64{0, 0, 0}, Committed: true,
				}}},
				{nextApply: 1},
			},
			want:  map[string]int64{"x": 1},
			wantN: 2,
		},
		{
			name: "0->1, xy",
			columns: []*columnT{
				{nextApply: 1, Log: []*Instance{nil, {
					Val:  &Cmd{Key: "x", Vi64: 1},
					Seen: []int64{0, 2, 0}, Committed: true,
				}}},
				{nextApply: 1, Log: []*Instance{nil, {
					Val:  &Cmd{Key: "y", Vi64: 2},
					Seen: []int64{0, 0, 0}, Committed: true,
				}}},
				{nextApply: 1},
			},
			want:  map[string]int64{"x": 1, "y": 2},
			wantN: 2,
		},
		{
			name: "0->1<>2, xyz",
			columns: []*columnT{
				{nextApply: 1, Log: []*Instance{nil, {
					Val:  &Cmd{Key: "x", Vi64: 1},
					Seen: []int64{0, 2, 0}, Committed: true,
				}}},
				{nextApply: 1, Log: []*Instance{nil, {
					Val:  &Cmd{Key: "y", Vi64: 2},
					Seen: []int64{0, 0, 2}, Committed: true,
				}}},
				{nextApply: 1, Log: []*Instance{nil, {
					Val:  &Cmd{Key: "z", Vi64: 3},
					Seen: []int64{0, 2, 0}, Committed: true,
				}}},
			},
			want:  map[string]int64{"x": 1, "y": 2, "z": 3},
			wantN: 3,
		},
		{
			name: "0->1<>2, yyz",
			columns: []*columnT{
				{nextApply: 1, Log: []*Instance{nil, {
					Val:  &Cmd{Key: "y", Vi64: 1},
					Seen: []int64{0, 2, 0}, Committed: true,
				}}},
				{nextApply: 1, Log: []*Instance{nil, {
					Val:  &Cmd{Key: "y", Vi64: 2},
					Seen: []int64{0, 0, 2}, Committed: true,
				}}},
				{nextApply: 1, Log: []*Instance{nil, {
					Val:  &Cmd{Key: "z", Vi64: 3},
					Seen: []int64{0, 2, 0}, Committed: true,
				}}},
			},
			want:  map[string]int64{"y": 1, "z": 3},
			wantN: 3,
		},
		{
			name: "0->1<>2, zyz",
			columns: []*columnT{
				{nextApply: 1, Log: []*Instance{nil, {
					Val:  &Cmd{Key: "z", Vi64: 1},
					Seen: []int64{0, 2, 0}, Committed: true,
				}}},
				{nextApply: 1, Log: []*Instance{nil, {
					Val:  &Cmd{Key: "y", Vi64: 2},
					Seen: []int64{0, 0, 2}, Committed: true,
				}}},
				{nextApply: 1, Log: []*Instance{nil, {
					Val:  &Cmd{Key: "z", Vi64: 3},
					Seen: []int64{0, 2, 0}, Committed: true,
				}}},
			},
			want:  map[string]int64{"z": 1, "y": 2},
			wantN: 3,
		},
		{
			name: "no log to apply",
			columns: []*columnT{
				{nextApply: 2, Log: []*Instance{nil, {
					Val:  &Cmd{Key: "z", Vi64: 1},
					Seen: []int64{0, 2, 0}, Committed: true,
				}}},
				{nextApply: 2, Log: []*Instance{nil, {
					Val:  &Cmd{Key: "y", Vi64: 2},
					Seen: []int64{0, 0, 2}, Committed: true,
				}}},
				{nextApply: 2, Log: []*Instance{nil, {
					Val:  &Cmd{Key: "z", Vi64: 3},
					Seen: []int64{0, 2, 0}, Committed: true,
				}}},
			},
			want:  map[string]int64{},
			wantN: 0,
		},
	}

	for i, c := range cases {
		mes := fmt.Sprintf("%d-th: case: %+v", i+1, c)
		s := NewKVServer(1)
		s.columns = c.columns

		fmt.Println(c.name)

		h := NewHandler(s, nil)
		n, err := h.apply()
		_ = err

		snap := s.getSnapshot()

		ta.Equal(c.wantN, n)
		ta.Equal(c.want, snap, mes)
	}
}
