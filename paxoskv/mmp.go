package paxoskv

func (s *KVServer) hdlOps(req *Request, remote *Ins) *Ins {
	h := NewHandler(s, remote)

	if req.Op == Op_Commit {
		latest := h.hdlCommit(remote)
		return latest
	}

	// Op_Accept: Accept implies Prepare

	local, _ := h.hdlPrepare(remote)
	dd(h, "after prepare: local: %s", local.str())

	if local.VBal == nil && remote.VBal == nil {
		// In case another value with VBal=nil is initialized.
		// hdlPrepare only update seen if local was nil
		// no accepted value, create one with merged `Deps`
		local.updateDeps(remote.Deps)
		dd(h, "merged seen: %v from local: %s remote: %s", local.Deps, local.str(), remote.str())
	}

	localLERemote := local.VBal.LessEqual(remote.VBal)
	dd(h, "after prepare: local: %s <= remote: %s  : %t", local.str(), remote.str(), localLERemote)

	latest := local
	if localLERemote {
		latest = remote
	}

	accepted := h.hdlAccept(req.Bal, latest)

	if localLERemote {
		// there is a quorum(the sender and this replica) that has the latest value.
		return h.hdlCommit(accepted)
	}

	return accepted
}

func (h *Handler) hdlPrepare(inst *Ins) (*Ins, error) {
	s := h.kvServer
	column, lsn := inst.getColLSN()
	col := s.log.columns[column]

	curr := col.getInstance(lsn)
	if curr != nil {
		return curr, AlreadyPrepared
	}

	inst = inst.Clone()

	// value with vbal=nil is a prepared value
	inst.VBal = nil
	inst.updateDeps(s.getLogLens())

	col.addInstance(inst)
	return inst, nil
}

func (h *Handler) hdlAccept(bal *BallotNum, inst *Ins) *Ins {
	inst.VBal = bal.Clone()
	h.setInstance(inst)
	dd(h, "after accept: latest: %s", inst.str())
	return inst
}

func (h *Handler) hdlCommit(inst *Ins) *Ins {

	s := h.kvServer
	dd(h, "hdlCommit: %s", inst.str())

	column := inst.getColumn()
	col := s.log.columns[column]

	inst.Committed = true
	h.setInstance(inst)

	nc := s.log.nextCommits[column]
	for int(nc) < len(col.Log) && col.Log[nc] != nil && col.Log[nc].Committed {
		nc++
	}
	s.log.nextCommits[column] = nc

	dd(s, "hdlCommit Committed: %s, nextCommit: %d", inst.str(), nc)

	_, err := h.apply()
	_ = err
	return inst
}
