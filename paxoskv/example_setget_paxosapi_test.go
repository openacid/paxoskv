package paxoskv

import (
	"fmt"
)

func Example_setAndGet_PaxosAPI() {

	// In this example it set or get a key_ver by running a paxos instance.

	acceptorIds := []int64{0, 1, 2}

	servers := ServeAcceptors(acceptorIds)
	defer func() {
		for _, s := range servers {
			s.srv.Stop()
		}
	}()

	// set foo₀ = 5
	{
		bal := &BallotNum{N: 0, Id: 2}
		v := RunPaxos(bal, acceptorIds, 0, map[int64]*Cmd{0: {LSN: 0, Key: "foo", Vi64: 5}})
		fmt.Printf("written: %v;\n", v[0].Vi64)
	}

	// get foo₀
	{
		bal := &BallotNum{N: 0, Id: 2}
		v := RunPaxos(bal, acceptorIds, 0, map[int64]*Cmd{})
		fmt.Printf("read:    %v;\n", v[0].Vi64)
	}

	// set foo₁ = 6
	{
		bal := &BallotNum{N: 0, Id: 2}
		v := RunPaxos(bal, acceptorIds, 1, map[int64]*Cmd{1: {LSN: 1, Key: "foo", Vi64: 6}})
		fmt.Printf("written: %v;\n", v[1].Vi64)
	}

	// get foo₁
	{
		bal := &BallotNum{N: 0, Id: 2}
		v := RunPaxos(bal, acceptorIds, 1, map[int64]*Cmd{})
		fmt.Printf("read:    %v;\n", v[1].Vi64)
	}

	// Output:
	// written: 5;
	// read:    5;
	// written: 6;
	// read:    6;

}
