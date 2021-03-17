package paxoskv

import (
	"fmt"
	"strings"
)

type Log struct {
	nextCommits []int64
	columns     []*Column
}

func NewLog() *Log {
	return &Log{
		nextCommits: []int64{0, 0, 0},
		columns: []*Column{
			NewColumn(0),
			NewColumn(1),
			NewColumn(2),
		},
	}
}

func (l *Log) getNextCommits() []int64 {
	return dupI64s(l.nextCommits)
}

func (l *Log) refInstance(column, lsn int64) *Ins {
	return l.columns[column].Log[lsn]
}

func (s *KVServer) graphviz() string {
	l := s.log
	as := s.stateMachine.applySeq
	lines := []string{
		"digraph x",
		"{",
		"node [shape=plaintext]",
		"rankdir=LR",
	}

	letters := "abc"

	for column := 0; column < 3; column++ {
		col := l.columns[column]

		allSymbols := []string{}
		for lsn, ins := range col.Log {
			label := fmt.Sprintf("%c%d", letters[column], lsn)
			symbol := fmt.Sprintf("X%dX%d", column, lsn)
			// Ins node
			lines = append(lines, fmt.Sprintf("%s [ label=\"%s\"]", symbol, label))

			// deps
			for j := 0; j < 3; j++ {
				var seen int64
				if column == j {
					seen = int64(lsn) - 1
				} else {
					seen = ins.Deps[j] - 1
				}

				if seen >= 0 {
					s0 := fmt.Sprintf("X%dX%d", j, seen)
					lines = append(lines, fmt.Sprintf("%s -> %s [ color=\"#aaaadd\"]", symbol, s0))

				}
			}

			allSymbols = append(allSymbols, symbol)
		}

		// keeps logs on one column
		lines = append(lines, fmt.Sprintf("{ rank=same %s }", strings.Join(allSymbols, " ")))
	}

	prev := ""
	for _, pp := range as {
		column, lsn := parseColLSN(pp)
		symbol := fmt.Sprintf("X%dX%d", column, lsn)
		if prev != "" {
			lines = append(lines, fmt.Sprintf("%s -> %s [ color=\"#444444\", penwidth=3]", symbol, prev))
		}
		prev = symbol
	}

	lines = append(lines, "}")
	return strings.Join(lines, "\n")

}
