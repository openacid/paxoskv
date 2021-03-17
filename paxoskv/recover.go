package paxoskv

import (
	"math/rand"
	"time"
)

// find out uncommitted instances and try to commit them, in case the leader crashes.
func (s *KVServer) recoveryLoop(column int64) {
	s.wg.Add(1)

	prev := int64(0)
	next := int64(0)
	seen := int64(-1)
	for {

		select {
		case <-s.running:
			// closed
			s.wg.Done()
			return
		default:
		}

		prev = next

		time.Sleep(time.Millisecond * time.Duration(rand.Int()%10+10))

		next, seen = s.getCommitState(column)
		dd(s, "recover: column: %d next:%d, prev:%d, seen: %d", column, next, prev, seen)
		if next == seen || next > prev {
			continue
		}

		s.recover(column, next)
	}
}

func (s *KVServer) recover(column, lsn int64) {
	if column == s.Id {
		panic("wtf")
	}

	dd(s, "start recover %d-%d", column, lsn)

	col := s.log.columns[column]

	var inst *Ins
	var h *Handler

	s.Lock()
	if col.getInstance(lsn) == nil {
		inst = s.NewNoop(column, lsn)
		h = NewHandler(s, inst)
		h.setInstance(inst)
	} else {
		inst = col.getInstance(lsn)
		h = NewHandler(s, inst)
	}
	s.Unlock()

	// repair with the other non leader column
	to := 3 - s.Id - column
	dd(h, "start recover: %s", inst.str())
	recovered := h.runPaxosLoop(column, lsn, []int64{to})
	dd(h, "recover-ed: column:%d, lsn:%d, %s", column, lsn, recovered.str())
}

func (s *KVServer) NewNoop(column, lsn int64) *Ins {
	inst := &Ins{
		InsId: &InsId{
			Column:     column,
			LSN:        lsn,
			ProposerId: s.Id,
		},
		Val: &Cmd{
			Key:  "NOOP",
			Vi64: 0,
		},
		VBal:      nil,
		Deps:      s.getLogLens(),
		Committed: false,
	}

	return inst
}
