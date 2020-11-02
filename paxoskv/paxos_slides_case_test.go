package paxoskv

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/golang/protobuf/proto"
)

func TestCase1SingleProposer(t *testing.T) {

	// slide-32: 1 Proposer, 3 Acceptor, only two of them are involved.
	// The Proposer finishes a paxos without conflict.

	ta := require.New(t)

	acceptorIds := []int64{0, 1, 2}
	quorum := 2

	servers := ServeAcceptors(acceptorIds)
	defer func() {
		for _, s := range servers {
			s.Stop()
		}
	}()

	// The proposer try to set i₀ = 10
	var val int64 = 10
	paxosId := &PaxosInstanceId{
		Key: "i",
		Ver: 0,
	}

	// proposer X
	var pidx int64 = 10
	px := Proposer{
		Id:  paxosId,
		Bal: &BallotNum{N: 0, ProposerId: pidx},
	}

	// Phase 1 will be done without seeing other ballot, nor other voted value.
	latestVal, higherBal, err := px.Phase1([]int64{0, 1}, quorum)
	ta.Nil(err, "constituted a quorum")
	ta.Nil(higherBal, "no other proposer is seen")
	ta.Nil(latestVal, "no voted value")

	// Thus the Proposer choose a new value to propose.
	px.Val = &Value{Vi64: val}

	// Phase 2
	higherBal, err = px.Phase2([]int64{0, 1}, quorum)
	ta.Nil(err, "constituted a quorum")
	ta.Nil(higherBal, "no other proposer is seen")
}

func TestCase2DoubleProposer(t *testing.T) {

	// slide-33: 2 Proposer X and Y, 3 Acceptor.
	// Y overrides X then successfully decided a value.
	// Then X re-run paxos with a higher ballot and then proposed the value Y
	// chose.

	ta := require.New(t)

	acceptorIds := []int64{0, 1, 2}
	quorum := 2

	servers := ServeAcceptors(acceptorIds)
	defer func() {
		for _, s := range servers {
			s.Stop()
		}
	}()

	// two proposer
	var pidx int64 = 10
	var pidy int64 = 11

	paxosId := &PaxosInstanceId{
		Key: "i",
		Ver: 0,
	}

	// Proposer X prepared on Acceptor 0, 1 with ballot (1, pidx) and succeed.

	px := Proposer{
		Id:  paxosId,
		Bal: &BallotNum{N: 1, ProposerId: pidx},
	}
	latestVal, higherBal, err := px.Phase1([]int64{0, 1}, quorum)
	ta.True(err == nil && higherBal == nil && latestVal == nil, "succeess")

	// Proposer Y prepared on Acceptor 1, 2 with a higher ballot(2, pidy) and
	// succeed too, by overriding ballot for X.

	py := Proposer{
		Id:  paxosId,
		Bal: &BallotNum{N: 2, ProposerId: pidy},
	}
	latestVal, higherBal, err = py.Phase1([]int64{1, 2}, quorum)
	ta.True(err == nil && higherBal == nil && latestVal == nil, "succeess")

	// Proposer X does not know of Y, it chooses the value it wants to
	// write and proceed phase-2 on Acceptor 0, 1.
	// Then X found a higher ballot thus it failed to finish the paxos algo.

	px.Val = &Value{Vi64: 100}
	higherBal, err = px.Phase2([]int64{0, 1}, quorum)
	ta.Equalf(err, NotEnoughQuorum, "Proposer X should fail in phase-2")
	ta.True(proto.Equal(higherBal, py.Bal),
		"X should seen a higher bal, which is written by Y")

	// Proposer Y does not know of X.
	// But it has a higher ballot thus it would succeed running phase-2

	py.Val = &Value{Vi64: 200}
	higherBal, err = py.Phase2([]int64{1, 2}, quorum)
	ta.Nil(err, "Proposer Y succeeds in phase-2")
	ta.Nil(higherBal, "Y would not see a higher bal")

	// Proposer X retry with a higher ballot (3, pidx).
	// It will see a voted value by Y then choose it to propose.
	// Finally X finished the paxos but it did not propose the value it wants
	// to.

	px.Val = nil
	px.Bal = &BallotNum{N: 3, ProposerId: pidx}
	latestVal, higherBal, err = px.Phase1([]int64{0, 1}, quorum)
	ta.Nil(err, "constituted a quorum")
	ta.Nil(higherBal, "X should not see other bal")
	ta.True(proto.Equal(latestVal, py.Val),
		"X should see the value Acceptor voted for Y")

	// Proposer X then propose the seen value and finish phase-2

	px.Val = latestVal
	higherBal, err = px.Phase2([]int64{0, 1}, quorum)
	ta.Nil(err, "Proposer X should succeed in phase-2")
	ta.Nil(higherBal, "X should succeed")

}
