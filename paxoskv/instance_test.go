package paxoskv

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestInstance_getColumn_LSN(t *testing.T) {
	ta := require.New(t)
	inst := &Ins{
		InsId: NewInsId(10, 5, 1),
		Val:   &Cmd{}}
	ta.Equal(int64(10), inst.getColumn())

	ta.Equal(int64(5), inst.getLSN())

	c, l := inst.getColLSN()
	ta.Equal(int64(10), c)
	ta.Equal(int64(5), l)

}
func TestInstance_packedPosition(t *testing.T) {
	ta := require.New(t)

	inst := &Ins{
		InsId: NewInsId(2, 5, 0),
		Val:   &Cmd{}}

	packed := inst.packedPosition()
	ta.Equal(int64(2000000005), packed, "packed")

	s := inst.colLSNIndentedStr()
	ta.Equal("            5", s)
}
