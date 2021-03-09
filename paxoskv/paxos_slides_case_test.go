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
			s.srv.Stop()
		}
	}()

	// The proposer try to set i₀ = 10
	var val int64 = 10
	paxosId := int64(0)

	// proposer X
	var pidx int64 = 10
	px := &BallotNum{N: 0, Id: pidx}

	// Phase 1 will be done without seeing other ballot, nor other voted value.
	latestVal, higherBal, err := Phase1(px, []int64{0, 1}, paxosId, quorum)
	ta.Nil(err, "constituted a Quorum")
	ta.Nil(higherBal, "no other proposer is seen")
	ta.Equal(0, len(latestVal), "no voted value")

	// Thus the Proposer choose a new value to propose.
	// Phase 2
	higherBal, err = Phase2(px, []int64{0, 1}, map[int64]*Cmd{paxosId: {Key: "i", Vi64: val}}, quorum)
	ta.Nil(err, "constituted a Quorum")
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
			s.srv.Stop()
		}
	}()

	// two proposer
	var pidx int64 = 10
	var pidy int64 = 11

	paxosId := int64(0)

	// Proposer X prepared on Acceptor 0, 1 with ballot (1, pidx) and succeed.

	px :=      &BallotNum{N: 1, Id: pidx}
	latestVal, higherBal, err := Phase1(px, []int64{0, 1}, paxosId, quorum)
	ta.True(err == nil && higherBal == nil && len(latestVal) == 0, "succeess")

	// Proposer Y prepared on Acceptor 1, 2 with a higher ballot(2, pidy) and
	// succeed too, by overriding ballot for X.

	py :=      &BallotNum{N: 2, Id: pidy}
	latestVal, higherBal, err = Phase1(py, []int64{1, 2}, paxosId, quorum)
	ta.True(err == nil && higherBal == nil && len(latestVal) == 0, "succeess")

	// Proposer X does not know of Y, it chooses the value it wants to
	// write and proceed phase-2 on Acceptor 0, 1.
	// Then X found a higher ballot thus it failed to finish the paxos algo.

	higherBal, err = Phase2(px, []int64{0, 1}, map[int64]*Cmd {paxosId: {Key: "i", Vi64: 100}}, quorum)
	ta.Equalf(err, NotEnoughQuorum, "Proposer X should fail in phase-2")
	ta.True(proto.Equal(higherBal, py),
		"X should seen a higher Bal, which is written by Y")

	// Proposer Y does not know of X.
	// But it has a higher ballot thus it would succeed running phase-2

	higherBal, err = Phase2(py, []int64{1, 2}, map[int64]*Cmd {paxosId: {Key: "i", Vi64: 200}}, quorum)
	ta.Nil(err, "Proposer Y succeeds in phase-2")
	ta.Nil(higherBal, "Y would not see a higher Bal")

	// Proposer X retry with a higher ballot (3, pidx).
	// It will see a voted value by Y then choose it to propose.
	// Finally X finished the paxos but it did not propose the value it wants
	// to.

	px = &BallotNum{N: 3, Id: pidx}
	latestVal, higherBal, err = Phase1(px, []int64{0, 1}, paxosId, quorum)
	ta.Nil(err, "constituted a Quorum")
	ta.Nil(higherBal, "X should not see other Bal")
	ta.True(proto.Equal(latestVal[paxosId], &Cmd{Key: "i", Vi64: 200}),
		"X should see the value Acceptor voted for Y")

	// Proposer X then propose the seen value and finish phase-2

	higherBal, err = Phase2(px, []int64{0, 1}, latestVal, quorum)
	ta.Nil(err, "Proposer X should succeed in phase-2")
	ta.Nil(higherBal, "X should succeed")

}
