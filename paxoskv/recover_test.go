package paxoskv

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestNewNOOP(t *testing.T) {
	ta := require.New(t)
	s := NewKVServer(2)
	s.log.columns = []*Column{
		{Log: []*Ins{nil, nil}},
		{Log: []*Ins{nil}},
		{Log: []*Ins{nil, nil, nil}},
	}
	inst := s.NewNoop(1, 5)
	ta.Equal("<1-5-2: <NOOP=0> vbal:nil c:false seen:2,1,3>", inst.str())
}
