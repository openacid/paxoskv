package paxoskv

import (
	"math/rand"
	"time"
)

func (s *KVServer) recoveryLoop(column int64) {
	s.wg.Add(1)

	prev := int64(0)
	next := int64(0)
	unseen := int64(-1)
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

		next, unseen = s.getCommitState(column)
		dd(s, "recover: column: %d next:%d, prev:%d, unseen: %d", column, next, prev, unseen)
		if next == unseen || next > prev {
			continue
		}

		s.recover(column, next)
	}
}

func (s *KVServer) getCommitState(column int64) (int64, int64) {
	col := s.columns[column]

	s.Lock()
	defer s.Unlock()

	return col.nextCommit, int64(len(col.Log))
}

func (s *KVServer) getAppliedState(column int64) (int64, int64) {
	col := s.columns[column]

	s.Lock()
	defer s.Unlock()

	return col.nextApply, int64(len(col.Log))
}

func (s *KVServer) recover(column, lsn int64) {
	if column == s.Id {
		panic("wtf")
	}

	dd(s, "A: start recover %d-%d", column, lsn)

	col := s.columns[column]

	var inst *Instance
	var h *Handler

	s.Lock()
	if col.getInstance(lsn) == nil {
		inst = s.NewNoop(column, lsn)
		h = NewHandler(s, inst)
		h.setLog(inst)
	} else {
		inst = col.getInstance(lsn)
		h = NewHandler(s, inst)
	}
	s.Unlock()

	// repair with the other non author column
	to := 3 - s.Id - column
	dd(h, "start recover: %s", inst.str())
	recovered := h.runPaxosLoop(column, lsn, []int64{to})
	dd(h, "recover-ed: column:%d, lsn:%d, %s", column, lsn, recovered.str())
}

func (s *KVServer) NewNoop(column, lsn int64) *Instance {
	inst := &Instance{
		Val: &Cmd{
			ValueId: &ValueId{
				Column:    column,
				LSN:       lsn,
				ProposerN: s.Id,
			},
			Key:  "NOOP",
			Vi64: 0,
		},
		VBal:      nil,
		Seen:      s.getLogLens(),
		Committed: false,
	}

	return inst
}
