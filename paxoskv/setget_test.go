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

	acceptorIds := []int64{0, 1, 2}

	servers := ServeAcceptors(acceptorIds)
	defer func() {
		for _, s := range servers {
			s.srv.Stop()
		}
	}()

	letters := "abcdefghijklmnopqrst012"[:3]
	r := rand.New(rand.NewSource(555))

	l := len(letters)
	ks := make([]string, 0)
	for i := 0; i < 100; i++ {
		key := string([]byte{letters[r.Int()%l], letters[r.Int()%l]})
		ks = append(ks, key)
	}

	nworker := 2
	nreq := 5

	var wg sync.WaitGroup
	ch := make(chan *Cmd, 1000)
	type outT struct {
		method string
		err error
		cmd *Cmd
	}
	outputCh:= make(chan *outT,1000)
	for i := 0; i < nworker; i++ {
		wg.Add(1)
		go func(aid int64) {
			defer wg.Done()
			for {
				req := <-ch
				if req == nil {
					break
				}
				reply := new(Cmd)
				err := rpcTo(9, aid, "Set", req, reply)
				outputCh <- &outT{
					method: "Set",
					err:err,
					cmd: reply,
				}

				reply = new(Cmd)
				err = rpcTo(9, aid, "Get", req, reply)
				outputCh <- &outT{
					method: "Get",
					err:err,
					cmd: reply,
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
		out := <- outputCh
		fmt.Printf("output: %s %v %v\n", out.method, out.err, out.cmd)
	}

	wg.Wait()

	for _, s := range servers {
		s.srv.Stop()
	}

	s := servers[0]

	{
		fmt.Println("check logs")

		for i, v := range s.Log {
			fmt.Println(i, v.str())
		}

		for i, v := range s.Log {
			ta.EqualValues(i, v.Val.LSN, "lsn is identical to log index")
		}

		ta.Equal(nreq, len(s.Log), "a log entry for every Set")

		for i, v := range s.Log {
			fmt.Println(v)
			ta.EqualValues(i, v.Val.LSN, "lsn is identical to log index")
		}

		vals := make([]int, nreq)
		for _, v := range s.Log {
			vals[v.Val.Vi64-10000]++
		}
		for i, v := range vals {
			ta.EqualValues(1, v, "every Set has a log entry: %d", i)
		}
	}

	{
		fmt.Println("check snapshot")

		shot := make(map[string]int64)
		for _, v := range s.Log {
			shot[v.Val.Key] = v.Val.Vi64
		}
		ta.Equal(len(shot), len(s.Storage))
		for k, v := range shot {
			ta.Equal(v, s.Storage[k].Val.Vi64, "k:%s", k)
		}
	}

	{
		fmt.Println("check consistency")

		for i := 0; i < nreq; i++ {
			cmd := servers[0].Log[i].Val
			fmt.Printf("%d: 0-1: %s=%d\n", cmd.LSN, cmd.Key, cmd.Vi64)
			fmt.Println(servers[0].Log[i].Val)
			fmt.Println(servers[1].Log[i].Val)
			fmt.Println(servers[2].Log[i].Val)
			ta.Equal(servers[0].Log[i].Val, servers[1].Log[i].Val)
			fmt.Printf("%d: 0-2: %s=%d\n", cmd.LSN, cmd.Key, cmd.Vi64)
			ta.Equal(servers[0].Log[i].Val, servers[2].Log[i].Val)
		}

		for i := 0; i < 3; i++ {
			for j := 0; j < 3; j++ {
				if i == j {
					continue
				}
				for k, v := range servers[i].Storage {
					ta.Equal(v.Val, servers[j].Storage[k].Val, "k:%s i:%d, j:%d", k, i, j)
				}
			}
		}
	}

	for _, s := range servers {
		ta.EqualValues(nreq, s.nonCommitted)
	}

}
