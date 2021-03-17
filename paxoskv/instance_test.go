package paxoskv

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestInstance_getColumn_LSN(t *testing.T) {
	ta := require.New(t)
	inst := &Instance{Val: &Cmd{ValueId: newVid(10, 5, 1)}}
	ta.Equal(int64(10), inst.getColumn())

	ta.Equal(int64(5), inst.getLSN())

	c, l := inst.getColLSN()
	ta.Equal(int64(10), c)
	ta.Equal(int64(5), l)

}
