package paxoskv

import (
	"testing"

	"github.com/openacid/paxoskv"
	"github.com/stretchr/testify/require"
)

func TestAcceptor_Accept_deref_LastBal(t *testing.T) {

	ta := require.New(t)

	kvs := paxoskv.KVServer{
		Storage: map[string]paxoskv.Versions{},
	}
	p := &paxoskv.Proposer{
		Id: &paxoskv.PaxosInstanceId{
			Key: "x",
			Ver: 0,
		},
		// smaller than any bal
		Bal: &paxoskv.BallotNum{N: -1},
	}

	reply, err := kvs.Accept(nil, p)
	_ = err

	//v := kvs.Storage["x"][0]

	// change storage, the reply should not be affected
	//v.acceptor.LastBal.N = 100
	ta.Equal(int64(0), reply.LastBal.N)

}
