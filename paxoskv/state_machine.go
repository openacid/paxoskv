package paxoskv

import (
	"sync"
)

type StateMachine struct {
	sync.Mutex
	nextApplies []int64
	state       map[string]*Ins
	applySeq    []int64
}

func NewStateMachine() *StateMachine {
	return &StateMachine{
		nextApplies: []int64{0, 0, 0},
		state:       make(map[string]*Ins),
	}
}

func (sm *StateMachine) applyInstance(h *Handler, inst *Ins) {
	dd(h, "curr nextApplies: %v, to apply: %s", sm.nextApplies, inst.str())
	column, lsn := inst.getColLSN()
	sm.Lock()
	defer sm.Unlock()

	if sm.nextApplies[column] > lsn {
		// already applied
		return
	}

	if sm.nextApplies[column] != lsn {
		bug(h, "non continuous apply: nextApplies: %d-%d, inst:%s", column, sm.nextApplies[column], inst.str())
	}

	dd(h, "applyInstance: applied: col-lsn=%d-%d: %s", column, lsn, inst.str())
	sm.state[inst.Val.Key] = inst.Clone()
	sm.nextApplies[column]++
	sm.applySeq = append(sm.applySeq, inst.packedPosition())

}

func (sm *StateMachine) getNextApplies() []int64 {
	sm.Lock()
	defer sm.Unlock()

	return []int64{
		sm.nextApplies[0],
		sm.nextApplies[1],
		sm.nextApplies[2],
	}
}

func (sm *StateMachine) getState() map[string]int64 {
	sm.Lock()
	defer sm.Unlock()

	rst := make(map[string]int64)

	for k, inst := range sm.state {
		rst[k] = inst.Val.Vi64
	}

	return rst
}

func (sm *StateMachine) get(key string) (*Cmd, error) {
	sm.Lock()
	defer sm.Unlock()

	inst, found := sm.state[key]
	dd(nil, "G: v: %s", inst.str())
	if found {
		v := inst.Val.Clone()
		return v, nil
	}
	return &Cmd{}, nil
}
