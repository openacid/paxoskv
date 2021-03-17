package paxoskv

import (
	"fmt"
	"log"
	"math/rand"
	"net"
	"os"
	"sync"
	"time"

	"github.com/gogo/protobuf/proto"
	"github.com/kr/pretty"
	"github.com/openacid/paxoskv/goid"
	"github.com/pkg/errors"
	"golang.org/x/net/context"
	"google.golang.org/grpc"
	"google.golang.org/grpc/reflection"
)

var (
	HigherBalErr    = errors.New("seen a higher ballot")
	AlreadyPrepared = errors.New("already prepared")
	NotCommitted    = errors.New("not committed")
	FakeErr         = errors.New("fake error")

	AcceptorBasePort = int64(3333)

	sendErrorRate = float64(0)
	recvErrorRate = float64(0)
)

func dd(gg loggerGetter, f string, args ...interface{}) {
	p := fmt.Sprintf("%s ", goid.ID())
	gg.getLogger().Output(2, p+pretty.Sprintf(f, args...))
}
func bug(gg loggerGetter, f string, args ...interface{}) {
	p := fmt.Sprintf("%s ", goid.ID())
	gg.getLogger().Output(2, p+pretty.Sprintf(f, args...))
	panic("bug")
}

type NeedCommitError struct {
	Column int64
	Err    error
}

func (e *NeedCommitError) Cause() error {
	return e.Err
}

func (e *NeedCommitError) Error() string {
	return fmt.Sprintf("%s: %d", e.Err.Error(), e.Column)
}

type loggerGetter interface {
	getLogger() *log.Logger
}

type Handler struct {
	s    *KVServer
	inst *Instance
	lg   *log.Logger
	txid string
}

func NewHandler(s *KVServer, inst *Instance) *Handler {

	txid := fmt.Sprintf("R%d:tx:?-?-?", s.Id)
	if inst != nil {
		txid = fmt.Sprintf("R%d:tx:%s", s.Id, inst.Val.ValueId.str())
	}

	h := &Handler{
		s:    s,
		inst: inst,
		txid: txid,
		lg:   log.New(os.Stderr, txid+" ", log.Ltime|log.Lmicroseconds|log.Lshortfile|log.Lmsgprefix),
	}
	return h
}

func (h *Handler) getLogger() *log.Logger {
	return h.lg
}

// KVServer provides: a single Proposer with field Bal, multiple Instances with Log.
type KVServer struct {
	sync.Mutex

	addr string

	Id      int64
	cluster []int64
	other   []int64

	columns  []*columnT
	applySeq []int64
	storage  map[string]*Instance

	running chan bool
	wg      sync.WaitGroup
	srv     *grpc.Server

	lg *log.Logger
}

func (s *KVServer) getLogger() *log.Logger {
	return s.lg
}

func NewKVServer(id int64) *KVServer {
	pkv := &KVServer{
		Id:      id,
		addr:    fmt.Sprintf(":%d", AcceptorBasePort+int64(id)),
		cluster: []int64{0, 1, 2},
		other:   []int64{},

		columns: []*columnT{
			NewColumn(0),
			NewColumn(1),
			NewColumn(2),
		},
		storage: map[string]*Instance{},

		running: make(chan bool),
		srv:     grpc.NewServer(),
		lg:      log.New(os.Stderr, fmt.Sprintf("R%d: ", id), log.Ltime|log.Lmicroseconds|log.Lshortfile|log.Lmsgprefix),
	}

	for _, rid := range pkv.cluster {
		if id == rid {
			continue
		}
		pkv.other = append(pkv.other, rid)
	}

	RegisterPaxosKVServer(pkv.srv, pkv)
	reflection.Register(pkv.srv)

	return pkv
}

func (s *KVServer) Start() {

	lis, err := net.Listen("tcp", s.addr)
	if err != nil {
		panic(pretty.Sprintf("listen: %s %s", s.addr, err.Error()))
	}

	dd(s, "serving on %s ...", s.addr)
	go s.srv.Serve(lis)

	for _, column := range s.other {
		go s.recoveryLoop(column)
	}
}

func (s *KVServer) Stop() {
	close(s.running)
	s.srv.GracefulStop()
	s.wg.Wait()
}

func (s *KVServer) waitForApplyAll() {
	for _, column := range s.cluster {
		dd(s, "start waitForApplyAll: %d", column)
		for {
			next, unseen := s.getAppliedState(column)
			dd(s, "waitForApplyAll: column %d: next,unseen: %d,%d", column, next, unseen)
			if next == unseen {
				break
			}
			for cc := 0; cc < 3; cc++ {
				s.Lock()
				for _, inst := range s.columns[cc].Log {
					dd(s, "log: %s", inst.str())
				}
				s.Unlock()
			}
			time.Sleep(time.Millisecond * 500)
		}
		dd(s, "done waitForApplyAll: %d", column)
	}
}

func (h *Handler) sendCommit(column int64, instances map[int64]*Instance) {
	s := h.s

	req := &Request{
		Ops:       []Op{Op_Commit},
		Column:    column,
		Instances: instances,
	}
	for _, to := range s.other {
		go func(to int64) {
			for i := 0; i < 3; i++ {
				err := h.rpcTo(to, "HandlePaxos", req, &Reply{})
				if err == nil {
					dd(h, "P: Commit done to %d column:%d %s", to, column, instsStr(instances))
					return
				}
				dd(h, "P: Commit err: %s, column:%d %s", err.Error(), column, instsStr(instances))
				time.Sleep(time.Millisecond * 10)
			}
			dd(h, "P: Commit fail to %d column:%d %s", to, column, instsStr(instances))
		}(to)
	}
	dd(h, "Commit sent: column:%d %s", column, instsStr(instances))
}

func (s *KVServer) getLogLens() []int64 {
	return []int64{
		int64(len(s.columns[0].Log)),
		int64(len(s.columns[1].Log)),
		int64(len(s.columns[2].Log)),
	}
}

// allocNewInst allocates a log-sequence-number in local cmds.
func (s *KVServer) allocNewInst(column int64, cmd *Cmd) *Instance {
	col := s.columns[column]

	lsn := int64(len(col.Log))

	// VBal nil: a FastAccept state
	inst := &Instance{
		Val:  cmd,
		VBal: nil,
	}
	col.Log = append(col.Log, inst)

	inst.Seen = s.getLogLens()

	cmd.ValueId = &ValueId{
		Column:    column,
		LSN:       lsn,
		ProposerN: s.Id,
	}

	dd(s, "allocated: %s", inst.str())
	return inst.Clone()
}

func (h *Handler) setLog(inst *Instance) {
	s := h.s
	column, lsn := inst.getColLSN()
	col := s.columns[column]

	for int(lsn) >= len(col.Log) {
		col.Log = append(col.Log, nil)
	}

	// TODO bad.
	cc := col.Log[lsn]
	if cc != nil && cc.Committed {
		if !cc.Val.Equal(inst.Val) || !seenEq(cc.Seen, inst.Seen) {
			bug(h, "accept to a committed but different: %s %s", cc.str(), inst.str())
		}
	}
	col.Log[lsn] = inst.Clone()
	dd(h, "setLog: column:%d, inst:%s", column, inst.str())
}

func (h *Handler) localPrepare(column, lsn int64) (*BallotNum, *Instance) {

	s := h.s

	col := s.columns[column]

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

func (h *Handler) runPaxosLoop(column, lsn int64, dsts []int64) *Instance {
	// this is the only goroutine to have a bal.
	// No two proposer is allowed to have the same bal
	for i := 0; ; i++ {
		bal, inst := h.localPrepare(column, lsn)
		dst := dsts[i%len(dsts)]
		committed, err := h.runPaxos(bal, column, dst, inst)
		if err == nil {
			return committed
		}
	}
}

func (h *Handler) runPaxos(bal *BallotNum, column, to int64, inst *Instance) (*Instance, error) {

	s := h.s
	dd(h, "runPaxos: %s", inst.str())

	col := s.columns[column]
	lsn := inst.getLSN()
	req := &Request{
		Ops:       []Op{Op_Prepare, Op_Accept},
		Bal:       bal.Clone(),
		Column:    column,
		Instances: map[int64]*Instance{lsn: inst},
	}

	reply := new(Reply)
	err := h.rpcTo(to, "HandlePaxos", req, reply)
	if err != nil {
		s.setNeedElect(column, bal)
		return nil, HigherBalErr
	}

	if bal.Less(reply.LastBal) {
		dd(h, "bal < reply.LastBal: %s < %s", bal.str(), reply.LastBal.str())
		s.setNeedElect(column, reply.LastBal)
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
	if local.VBal.Compare(remote.VBal) < 0 {
		h.setLog(remote)
	} else {
		// local >= remote

		// replied instance.Vbal is the req.Bal;
		// thus there is no chance local >= remote.
		bug(h, "impossible: local >= remote: %s %s", local.str(), remote.str())
	}

	local = col.Log[lsn]
	h.hdlCommitInstance(local)
	h.sendCommit(column,
		map[int64]*Instance{
			local.getLSN(): local,
		})

	return local, nil
}

func (s *KVServer) setNeedElect(column int64, bal *BallotNum) {
	col := s.columns[column]
	s.Lock()
	defer s.Unlock()

	col.Bal = nil

	if col.LastBal.Less(bal) {
		col.LastBal = bal.Clone()
	}
}

// ServeAcceptors starts a grpc server for every acceptor.
func ServeAcceptors(ids []int64) []*KVServer {

	var servers []*KVServer

	for _, aid := range ids {

		pkv := NewKVServer(aid)
		servers = append(servers, pkv)
		pkv.Start()
	}

	return servers
}

// rpcTo send grpc request to all acceptors
func (h *Handler) rpcTo(to int64, method string, req, reply proto.Message) error {

	// With heavy competition an operation takes longer time to finish.
	// There is still chance no leader is established before timeout.

	dd(h, "P: send %s-req to R%d %s", method, to, req.(strer).str())

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
		dd(h, "P: recv %s-reply from R%d err: %s", method, to, err)
	} else {
		dd(h, "P: recv %s-reply from R%d %s", method, to, reply.(strer).str())
	}

	if fakeRecvErr {
		dd(h, "fake recvError from R%d, req: %s", to, req.(strer).str())
		return FakeErr
	}
	return err
}

func rpcTo2(to int64, method string, req, reply proto.Message) error {

	ctx, cancel := context.WithTimeout(context.Background(), time.Second*3)
	defer cancel()

	address := fmt.Sprintf("127.0.0.1:%d", AcceptorBasePort+int64(to))
	conn, err := grpc.Dial(address, grpc.WithInsecure())
	if err != nil {
		return err
	}
	defer conn.Close()

	err = conn.Invoke(ctx, "/paxoskv.PaxosKV/"+method, req, reply)
	return err
}
