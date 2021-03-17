package paxoskv

import (
	"fmt"
	"math/rand"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
)

func Test_set_get(t *testing.T) {

	ta := require.New(t)
	_ = ta

	acceptorIds := []int64{0, 1, 2}
	alive := []bool{
		true,
		true,
		true,
	}

	sendErrorRate = 0.2
	recvErrorRate = 0.2

	servers := serveKVServers(acceptorIds)
	defer func() {
		for _, s := range servers {
			s.grpcSrv.Stop()
		}
	}()
	for i := range servers {
		if !alive[i] {
			servers[i].grpcSrv.Stop()
		}
	}

	letters := "abcdefghijklmnopqrst012"[:3]
	r := rand.New(rand.NewSource(555))

	l := len(letters)
	ks := make([]string, 0)
	for i := 0; i < 100; i++ {
		key := string([]byte{letters[r.Int()%l], letters[r.Int()%l]})
		ks = append(ks, key)
	}

	nworker := 5
	nreq := 200

	var wg sync.WaitGroup
	ch := make(chan *Cmd, 1000)
	type outT struct {
		method string
		err    error
		cmd    *Cmd
	}
	outputCh := make(chan *outT, 1000)
	for i := 0; i < nworker; i++ {
		wg.Add(1)
		go func(aid int64) {

			r := rand.New(rand.NewSource(aid))
			defer wg.Done()
			for {

				req := <-ch
				if req == nil {
					break
				}
				var err error
				var reply *Cmd
				for {
					aid = int64(r.Int() % 3)
					reply = new(Cmd)
					err = rpcTo2(aid, "Set", req, reply)
					if err == nil {
						break
					}
				}
				outputCh <- &outT{
					method: "Set",
					err:    err,
					cmd:    reply,
				}

				reply = new(Cmd)
				err = rpcTo2(aid, "Get", req, reply)
				outputCh <- &outT{
					method: "Get",
					err:    err,
					cmd:    reply,
				}
				// ta.Nil(err)
				// ta.Equal(req.Key, reply.Key)
				// ta.LessOrEqual(req.Vi64, reply.Vi64)
			}
		}(acceptorIds[i%len(acceptorIds)])
	}

	for i := 0; i < nreq; i++ {
		req := &Cmd{
			Key:  ks[i%len(ks)],
			Vi64: 10000 + int64(i),
		}
		ch <- req
	}
	close(ch)

	for i := 0; i < nreq*2; i++ {
		out := <-outputCh
		fmt.Printf("output: %s %v %v\n", out.method, out.err, out.cmd)
	}

	wg.Wait()

	for _, s := range servers {
		s.waitForApplyAll()
	}

	for _, s := range servers {
		s.Stop()
	}
	for sIdx, s := range servers {
		fmt.Println("---")
		fmt.Println(s.stateMachine.getState())
		for column := 0; column < 3; column++ {
			col := s.log.columns[column]
			fmt.Println("column:", column, "next:c/a", s.log.nextCommits[column], s.stateMachine.nextApplies[column])
			for i := 0; i < len(col.Log); i++ {
				fmt.Printf("%d %d %s\n", sIdx, column, col.Log[i].str())
			}
		}
	}

	s := servers[0]

	{
		fmt.Println("check every cmd present on s0")

		total := 0
		cnt := map[string]int{}
		for column, col := range s.log.columns {
			_ = column
			for i, inst := range col.Log {
				ta.EqualValues(i, inst.getLSN(), "lsn is identical to log index")
				if !inst.Val.isNoop() {
					total += 1

					k := fmt.Sprintf("%s=%d", inst.Val.Key, inst.Val.Vi64)
					cnt[k] = cnt[k] + 1

				}
			}
		}

		for k, v := range cnt {
			ta.Equal(1, v, "one log for key:%s", k)
		}

		ta.Equal(nreq, total, "one log entry for every Set operation")

		vals := make([]int, nreq)
		for column, col := range s.log.columns {
			_ = column
			for _, v := range col.Log {
				if !v.Val.isNoop() {
					vals[v.Val.Vi64-10000]++
				}
			}
		}
		for i, v := range vals {
			ta.EqualValues(1, v, "every Set has a log entry: %d", i)
		}
	}

	{
		fmt.Println("check depedency: two instance has at least one relation")
		for a := 0; a < 3; a++ {
			for b := a + 1; b < 3; b++ {
				ca := s.log.columns[a].Log
				cb := s.log.columns[b].Log
				for i := 0; i < len(ca); i++ {
					for j := 0; j < len(cb); j++ {
						insta := ca[i]
						instb := cb[j]
						if insta.Seen[b] > int64(j) || instb.Seen[a] > int64(i) {
							// there is a relation
						} else {
							ta.Fail("no relation:", "%d-%d and %d-%d : seen: %v %v", a, i, b, j, insta.Seen, instb.Seen)
						}
					}
				}
			}
		}
	}

	{
		fmt.Println("check logs consistent")
		for column := 0; column < 3; column++ {
			for lsn := 0; lsn < len(s.log.columns[column].Log); lsn++ {
				i0 := servers[0].log.columns[column].Log[lsn]
				i1 := servers[1].log.columns[column].Log[lsn]
				i2 := servers[2].log.columns[column].Log[lsn]

				v0 := i0.Val.str() + " " + fmt.Sprintf("%d,%d,%d", i0.Seen[0], i0.Seen[1], i0.Seen[2])
				v1 := i1.Val.str() + " " + fmt.Sprintf("%d,%d,%d", i1.Seen[0], i1.Seen[1], i1.Seen[2])
				v2 := i2.Val.str() + " " + fmt.Sprintf("%d,%d,%d", i2.Seen[0], i2.Seen[1], i2.Seen[2])
				fmt.Printf("s0 %d-%d: %s\n", column, lsn, v0)
				fmt.Printf("s1 %d-%d: %s\n", column, lsn, v1)
				fmt.Printf("s2 %d-%d: %s\n", column, lsn, v2)

				ta.Equal(v0, v1, "server 0:1, col-lsn:%d-%d", column, lsn)
				ta.Equal(v0, v2, "server 0:2, col-lsn:%d-%d", column, lsn)
			}
		}
	}

	{
		fmt.Println("check applySeq")
		for _, v := range s.stateMachine.applySeq {
			fmt.Println(colLSNIndentedStr(v))
		}

		for i := range servers {
			if alive[i] {
				ta.Equal(s.stateMachine.applySeq, servers[i].stateMachine.applySeq, "server: %d", i)
			}
		}
	}

	{
		fmt.Println("check snapshot")
		shot := s.stateMachine.getState()
		fmt.Println(shot)

		for i := range servers {
			if alive[i] {
				ta.Equal(shot, servers[i].stateMachine.getState(), "server %d", i)
			}
		}

	}
}
