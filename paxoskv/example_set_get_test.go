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
		fmt.Printf("written: %v;\n", v)
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
		fmt.Printf("read:    %v;\n", v)
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
		fmt.Printf("written: %v;\n", v)
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
		fmt.Printf("read:    %v;\n", v)
	}

	// Output:
	// written: Vi64:5 ;
	// read:    Vi64:5 ;
	// written: Vi64:6 ;
	// read:    Vi64:6 ;

}
