package paxoskv

import (
	"fmt"
	"sync"
	"time"

	"golang.org/x/net/context"
	"google.golang.org/grpc"
)

func Example_setAndGet_KVAPI() {

	serverIds := []int64{0, 1, 2}

	servers := serveKVServers(serverIds)
	defer func() {
		for _, s := range servers {
			s.grpcSrv.Stop()
		}
	}()

	var wg sync.WaitGroup
	input := make(chan *Cmd, 1000)

	// 5 concurrent proposers

	for i := 0; i < 5; i++ {
		wg.Add(1)
		go func(aid int64) {
			defer wg.Done()
			for req := range input {

				ctx, cancel := context.WithTimeout(context.Background(), time.Second)
				defer cancel()

				address := fmt.Sprintf("127.0.0.1:%d", 3333+int64(aid))
				conn, err := grpc.Dial(address, grpc.WithInsecure())
				if err != nil {
					panic(err.Error())
				}
				defer conn.Close()

				c := NewPaxosKVClient(conn)

				_, err = c.Set(ctx, req)
				if err != nil {
					panic(err.Error())
				}

				reply, err := c.Get(ctx, &Cmd{Key: req.Key})
				if err != nil {
					panic(err.Error())
				}

				// Get may got a different value since Set happens concurrently
				fmt.Println("Set:", req.Key, req.Vi64, "Get:", reply.Key, reply.Vi64)

			}
		}(serverIds[i%len(serverIds)])
	}

	nreq := 10
	for i := 0; i < nreq; i++ {
		input <- &Cmd{
			Key:  string("abcd"[i%2]),
			Vi64: int64(i),
		}
	}
	close(input)
	wg.Wait()
}
