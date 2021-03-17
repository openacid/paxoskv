package paxoskv

type graph map[int64][]int64

func findSCC(g graph, n int64) [][]int64 {

	rst := make([][]int64, 0)

	stack := make([]int64, 0)
	inStack := make(map[int64]bool, 0)

	idx := make(map[int64]int64, 0)
	low := make(map[int64]int64, 0)

	var scc func(n, i int64)
	scc = func(n, i int64) {
		stack = append(stack, n)
		inStack[n] = true
		idx[n] = i
		low[n] = i

		for _, nxt := range g[n] {
			_, found := idx[nxt]
			if !found {
				scc(nxt, i+1)
				low[n] = min(low[n], low[nxt])
				continue
			}

			if inStack[nxt] {
				low[n] = min(low[n], low[nxt])
			}
		}
		// fmt.Println(stack)

		if low[n] == idx[n] {
			// fmt.Println("root:", stack)
			res := []int64{}
			for {
				l := len(stack)
				last := stack[l-1]

				res = append(res, last)

				stack = stack[:l-1]
				delete(inStack, last)

				if n == last {
					break
				}
			}
			// fmt.Printf("one scc: %v\n", res)

			rst = append(rst, res)
		}

	}

	scc(n, 0)

	return rst

}
