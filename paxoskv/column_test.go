package paxoskv

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestColumnT_hasInstance(t *testing.T) {

	ta := require.New(t)

	col := &Column{
		Log: []*Ins{nil, {}, nil},
	}

	ta.False(col.hasInstance(0))
	ta.True(col.hasInstance(1))
	ta.False(col.hasInstance(2))
	ta.False(col.hasInstance(3))
}

func TestColLSN(t *testing.T) {
	ta := require.New(t)

	column, lsn := int64(2), int64(5)

	packed := int64(2000000005)

	c, l := parseColLSN(packed)
	ta.Equal(column, c)
	ta.Equal(lsn, l)

	s := colLSNIndentedStr(packed)
	ta.Equal("            5", s)
}
