package paxoskv

import (
	"fmt"
)

func Example_setAndGetByKeyVer() {

	// In this example it set or get a key_ver by running a paxos instance.

	acceptorIds := []int64{0, 1, 2}

	servers := ServeAcceptors(acceptorIds)
	defer func() {
		for _, s := range servers {
			s.Stop()
		}
	}()

	// set foo₀ = 5
	{
		prop := Proposer{
			Id: &PaxosInstanceId{
				Key: "foo",
				Ver: 0,
			},
			Bal: &BallotNum{N: 0, ProposerId: 2},
		}
		v := prop.RunPaxos(acceptorIds, &Value{Vi64: 5})
		fmt.Printf("written: %v;\n", v.Vi64)
	}

	// get foo₀
	{
		prop := Proposer{
			Id: &PaxosInstanceId{
				Key: "foo",
				Ver: 0,
			},
			Bal: &BallotNum{N: 0, ProposerId: 2},
		}
		v := prop.RunPaxos(acceptorIds, nil)
		fmt.Printf("read:    %v;\n", v.Vi64)
	}

	// set foo₁ = 6
	{
		prop := Proposer{
			Id: &PaxosInstanceId{
				Key: "foo",
				Ver: 1,
			},
			Bal: &BallotNum{N: 0, ProposerId: 2},
		}
		v := prop.RunPaxos(acceptorIds, &Value{Vi64: 6})
		fmt.Printf("written: %v;\n", v.Vi64)
	}

	// get foo₁
	{
		prop := Proposer{
			Id: &PaxosInstanceId{
				Key: "foo",
				Ver: 1,
			},
			Bal: &BallotNum{N: 0, ProposerId: 2},
		}
		v := prop.RunPaxos(acceptorIds, nil)
		fmt.Printf("read:    %v;\n", v.Vi64)
	}

	// Output:
	// written: 5;
	// read:    5;
	// written: 6;
	// read:    6;

}
