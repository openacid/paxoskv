package paxoskv

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestAcceptor_Accept_deref_LastBal(t *testing.T) {

	ta := require.New(t)

	kvs := KVServer{
		Storage: map[string]Versions{},
	}
	p := &Proposer{
		Id: &PaxosInstanceId{
			Key: "x",
			Ver: 0,
		},
		// smaller than any bal
		Bal: &BallotNum{N: -1},
	}

	reply, err := kvs.Accept(nil, p)
	_ = err

	v := kvs.Storage["x"][0]

	// change storage, the reply should not be affected
	v.acceptor.LastBal.N = 100
	ta.Equal(int64(0), reply.LastBal.N)

}
