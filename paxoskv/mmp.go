package paxoskv

func (s *KVServer) handleOps(req *Request, remote *Instance) *Instance {
	h := NewHandler(s, remote)

	if len(req.Ops) > 0 && req.Ops[0] == Op_Commit {
		latest := h.hdlCommitInstance(remote)
		dd(h, "after commit: %s", latest.str())
		return latest
	}

	local, err := h.hdlPrepareInstance(remote)
	var latest *Instance

	if err == nil {
		latest = local
		if local.VBal.Less(remote.VBal) {
			dd(h, "after prepare: local: %s < remote: %s", local.str(), remote.str())
			latest = remote
		}

		dd(h, "after prepare on nil: latest: %s", latest.str())

		latest = h.hdlAcceptInstance(req.Bal, latest)
		dd(h, "after accept: latest: %s", latest.str())

		return latest

	} else {
		if err != AlreadyPrepared {
			bug(h, "unknown error: %s", err.Error())
		}

		if local.VBal.Less(remote.VBal) || (local.VBal != nil && local.VBal.Equal(remote.VBal)) {
			dd(h, "after prepare: remote: %s >= local: %s", remote.str(), local.str())
			latest = remote
			latest = h.hdlAcceptInstance(req.Bal, latest)
			dd(h, "after accept: latest: %s", latest.str())

			// this is valid because leader has only two state:
			// Vbal=0, or committed.
			latest = h.hdlCommitInstance(latest)
			dd(h, "after commit: %s", latest.str())
			return latest
		}

		dd(h, "after prepare: remote < local or all nil: %s  %s", remote.str(), local.str())

		if local.VBal == nil && remote.VBal == nil {
			// NOTE: seens must be merged, or the accepted value may not contain committed instances.
			local.updateSeen(remote.Seen)
			dd(h, "merged seen: %v from local: %s remote: %s", local.Seen, local.str(), remote.str())
		}

		latest = local
		latest = h.hdlAcceptInstance(req.Bal, latest)
		dd(h, "after accept: latest: %s", latest.str())
		return latest
	}

}

func (h *Handler) hdlPrepareInstance(inst *Instance) (*Instance, error) {
	s := h.s
	column, lsn := inst.getColLSN()
	col := s.columns[column]

	curr := col.getInstance(lsn)
	if curr != nil {
		return curr, AlreadyPrepared
	}

	inst = inst.Clone()

	// vbal=nil indicate a fast-accepted value
	inst.VBal = nil
	inst.updateSeen(s.getLogLens())

	col.addInstance(inst)
	return inst, nil
}

func (h *Handler) hdlAcceptInstance(bal *BallotNum, inst *Instance) *Instance {
	inst.VBal = bal.Clone()
	h.setLog(inst)
	return inst
}

func (h *Handler) hdlCommitInstance(inst *Instance) *Instance {

	s := h.s
	dd(h, "hdlCommitInstance: %s", inst.str())

	column := inst.getColumn()
	col := s.columns[column]

	inst.Committed = true
	h.setLog(inst)

	nc := col.nextCommit
	for int(nc) < len(col.Log) && col.Log[nc] != nil && col.Log[nc].Committed {
		nc++
	}
	col.nextCommit = nc

	dd(s, "hdlCommitInstance Committed: %s, nextCommit: %d", inst.str(), col.nextCommit)

	_, err := h.apply()
	_ = err
	return inst
}

func seenEq(a, b []int64) bool {
	for i := 0; i < 3; i++ {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}
