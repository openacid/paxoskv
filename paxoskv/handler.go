package paxoskv

import (
	"fmt"
	"log"
	"math/rand"
	"os"
	"time"

	"github.com/gogo/protobuf/proto"
	"golang.org/x/net/context"
	"google.golang.org/grpc"
)

// Handler for one instance
type Handler struct {
	kvServer *KVServer
	inst     *Ins
	lg       *log.Logger
	txId     string
}

func NewHandler(s *KVServer, inst *Ins) *Handler {

	txId := fmt.Sprintf("R%d:tx:?-?-?", s.Id)
	if inst != nil {
		txId = fmt.Sprintf("R%d:tx:%s", s.Id, inst.InsId.str())
	}

	h := &Handler{
		kvServer: s,
		inst:     inst,
		txId:     txId,
		lg:       log.New(os.Stderr, txId+" ", log.Ltime|log.Lmicroseconds|log.Lshortfile|log.Lmsgprefix),
	}
	return h
}

func (h *Handler) getLogger() *log.Logger {
	return h.lg
}

func (h *Handler) sendCommit(column int64, instances map[int64]*Ins) {
	s := h.kvServer

	req := &Request{
		Op:        Op_Commit,
		Column:    column,
		Instances: instances,
	}
	for _, to := range s.other {
		go func(to int64) {
			for i := 0; i < 3; i++ {
				err := h.rpcTo(to, "HandlePaxos", req, &Reply{})
				if err == nil {
					dd(h, "Commit done to %d column:%d %s", to, column, instsStr(instances))
					return
				}
				dd(h, "Commit err: %s, column:%d %s", err.Error(), column, instsStr(instances))
				time.Sleep(time.Millisecond * 10)
			}
			dd(h, "Commit fail to %d column:%d %s", to, column, instsStr(instances))
		}(to)
	}
	dd(h, "Commit sent: column:%d %s", column, instsStr(instances))
}

func (h *Handler) setInstance(inst *Ins) {
	s := h.kvServer
	column, lsn := inst.getColLSN()
	col := s.log.columns[column]

	for int(lsn) >= len(col.Log) {
		col.Log = append(col.Log, nil)
	}

	cc := col.Log[lsn]
	if cc != nil && cc.Committed {
		if !cc.Val.Equal(inst.Val) || !vecEq(cc.Seen, inst.Seen) {
			bug(h, "trying to set a committed instance with different value: %s %s", cc.str(), inst.str())
		}
	}
	col.Log[lsn] = inst.Clone()
	dd(h, "setInstance: column:%d, inst:%s", column, inst.str())
}

// prepare the instance on local server
func (h *Handler) localPrepare(column, lsn int64) (*BallotNum, *Ins) {

	s := h.kvServer

	col := s.log.columns[column]

	s.Lock()
	defer s.Unlock()

	if col.Bal == nil {
		dd(h, "localPrepare: column: %d, incr lastBal: %s", column, col.LastBal.str())

		// next ballot and prepare myself
		col.LastBal.N++
		col.LastBal.Id = s.Id
		col.Bal = col.LastBal.Clone()
	} else {
		dd(h, "localPrepare: column: %d, use current bal: %s", column, col.Bal.str())
	}

	// NOTE: after prepare, it must re-fetch the local latest value.
	return col.Bal.Clone(), col.Log[lsn].Clone()
}

func (h *Handler) runPaxosLoop(column, lsn int64, dsts []int64) *Ins {
	// this is the only goroutine to have a bal.
	// No two proposers are allowed to have the same bal
	for i := 0; ; i++ {
		bal, inst := h.localPrepare(column, lsn)
		// every time retry running paxos, choose a different candidate
		dst := dsts[i%len(dsts)]
		committed, err := h.runPaxos(bal, column, dst, inst)
		if err == nil {
			return committed
		}
	}
}

func (h *Handler) runPaxos(bal *BallotNum, column, to int64, inst *Ins) (*Ins, error) {

	s := h.kvServer
	dd(h, "runPaxos: %s", inst.str())

	col := s.log.columns[column]
	lsn := inst.getLSN()
	req := &Request{
		Op:        Op_Accept,
		Bal:       bal.Clone(),
		Column:    column,
		Instances: map[int64]*Ins{lsn: inst},
	}

	reply := new(Reply)
	err := h.rpcTo(to, "HandlePaxos", req, reply)
	if err != nil {
		s.invalidateLastBal(column, bal)
		return nil, HigherBalErr
	}

	if bal.Less(reply.LastBal) {
		dd(h, "bal < reply.LastBal: %s < %s", bal.str(), reply.LastBal.str())
		s.invalidateLastBal(column, reply.LastBal)
		return nil, HigherBalErr
	}

	remote := reply.Instances[lsn]

	s.Lock()
	defer s.Unlock()

	if bal.Less(col.LastBal) {
		dd(h, "bal < col.LastBal: %s < %s", bal.str(), col.LastBal.str())
		return nil, HigherBalErr
	}

	local := col.Log[lsn]
	if local.VBal.Less(remote.VBal) {
		h.setInstance(remote)
	} else {
		// local >= remote

		// replied instance.Vbal is the req.Bal;
		// thus there is no chance local >= remote.
		bug(h, "impossible: local >= remote: %s %s", local.str(), remote.str())
	}

	local = col.Log[lsn]
	h.hdlCommit(local)
	h.sendCommit(column,
		map[int64]*Ins{
			local.getLSN(): local,
		})

	return local, nil
}

func (h *Handler) rpcTo(to int64, method string, req, reply proto.Message) error {

	// With heavy competition an operation takes longer time to finish.
	// There is still chance no leader is established before timeout.

	dd(h, "send %s-req to R%d %s", method, to, req.(strer).str())

	if rand.Float64() < sendErrorRate {
		dd(h, "fake sendError to R%d, req: %s", to, req.(strer).str())
		return FakeErr
	}

	ctx, cancel := context.WithTimeout(context.Background(), time.Second*3)
	defer cancel()

	address := fmt.Sprintf("127.0.0.1:%d", AcceptorBasePort+int64(to))
	conn, err := grpc.Dial(address, grpc.WithInsecure())
	if err != nil {
		dd(h, "did not connect: %s", err.Error())
		return err
	}
	defer conn.Close()

	fakeRecvErr := rand.Float64() < recvErrorRate
	if fakeRecvErr {
		// do not let caller to receive the reply
		reply = proto.Clone(reply)
	}
	err = conn.Invoke(ctx, "/paxoskv.PaxosKV/"+method, req, reply)
	if err != nil {
		dd(h, "recv %s-reply from R%d err: %s", method, to, err)
	} else {
		dd(h, "recv %s-reply from R%d %s", method, to, reply.(strer).str())
	}

	if fakeRecvErr {
		dd(h, "fake recvError from R%d, req: %s", to, req.(strer).str())
		return FakeErr
	}
	return err
}
