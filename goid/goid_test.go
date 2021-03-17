// Copyright 2018 Huan Du. All rights reserved.
// Licensed under the MIT license that can be found in the LICENSE file.

package goid

import (
	"sync"
	"testing"
	"time"
)

func TestID(t *testing.T) {
	id1 := ID()

	if id1 == defID {
		t.Fatalf("fail to get goroutine id.")
	}

	t.Run("G in another goroutine", func(t *testing.T) {
		id2 := ID()

		if id2 == defID {
			t.Fatalf("fail to get goroutine id.")
		}

		if id2 == id1 {
			t.Fatalf("every living goroutine id must be different. [id1:%s] [id2:%s]", id1, id2)
		}
	})
}

func TestSetID(t *testing.T) {
	SetID()
	id1 := ID()

	SetID()
	id2 := ID()

	if id1 == id2 {
		t.Fatalf("goroutine id must be different. [id1:%s] [id2:%s]", id1, id2)
	}
}

func TestExpireID(t *testing.T) {
	SetID()
	_ = ID()
	ExpireID()

	_, et, ok := idCache.GetWithExpiration(gP())
	if !ok {
		t.Fatalf("failed get goroutine id")
	}

	if et.Sub(time.Now()) > cleanExpTime {
		t.Fatalf("expire id error")
	}
}

func TestConcurrentGetID(t *testing.T) {
	cntG := 100
	var wg sync.WaitGroup
	wg.Add(cntG)

	for i := 0; i < cntG; i++ {
		go func() {
			SetID()

			id1 := ID()

			for j := 0; j < 100000; j++ {
				id2 := ID()
				if id1 != id2 {
					t.Fatalf("goroutine id must be equal. [id1:%s] [id2:%s]", id1, id2)
				}
			}
			wg.Done()
		}()
	}
	wg.Wait()
}

func BenchmarkID(b *testing.B) {
	SetID()
	_ = ID()
}
