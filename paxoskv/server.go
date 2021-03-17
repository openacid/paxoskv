package paxoskv

import (
	"fmt"
	"log"
	"net"
	"os"
	"sync"
	"time"

	"github.com/gogo/protobuf/proto"
	"github.com/kr/pretty"
	"golang.org/x/net/context"
	"google.golang.org/grpc"
	"google.golang.org/grpc/reflection"
)

// KVServer provides: a single Proposer with field Bal, multiple Instances with Log.
type KVServer struct {
	sync.Mutex

	addr string

	Id      int64
	cluster []int64
	other   []int64

	log *Log

	applier applier

	stateMachine *StateMachine

	running chan bool
	wg      sync.WaitGroup
	grpcSrv *grpc.Server

	lg *log.Logger
}

func NewKVServer(id int64) *KVServer {
	pkv := &KVServer{
		Id:      id,
		addr:    fmt.Sprintf(":%d", AcceptorBasePort+int64(id)),
		cluster: []int64{0, 1, 2},
		other:   []int64{},

		log: NewLog(),

		// A traditional apply algo is by finding the SCC and then apply the smallest in an SCC.
		// a simplified apply algo just apply the instance with least number of dependency.
		// SCC and VC may choose different instance to apply. Both guarantees consistency and linearizability.
		//
		// Leave one of them uncommented and go test to see the difference.
		// applier: &ApplierSCC{},
		applier: &ApplierVC{},

		stateMachine: NewStateMachine(),

		running: make(chan bool),
		grpcSrv: grpc.NewServer(),
		lg:      log.New(os.Stderr, fmt.Sprintf("R%d: ", id), log.Ltime|log.Lmicroseconds|log.Lshortfile|log.Lmsgprefix),
	}

	for _, rid := range pkv.cluster {
		if id == rid {
			continue
		}
		pkv.other = append(pkv.other, rid)
	}

	RegisterPaxosKVServer(pkv.grpcSrv, pkv)
	reflection.Register(pkv.grpcSrv)

	return pkv
}

func (s *KVServer) Start() {

	lis, err := net.Listen("tcp", s.addr)
	if err != nil {
		panic(pretty.Sprintf("listen: %s %s", s.addr, err.Error()))
	}

	dd(s, "serving on %s ...", s.addr)
	go s.grpcSrv.Serve(lis)

	for _, column := range s.other {
		go s.recoveryLoop(column)
	}
}

func (s *KVServer) Stop() {
	close(s.running)
	s.grpcSrv.GracefulStop()
	s.wg.Wait()
}

func (s *KVServer) getLogger() *log.Logger {
	return s.lg
}

func (s *KVServer) waitForApplyAll() {
	for _, column := range s.cluster {
		dd(s, "start waitForApplyAll: %d", column)
		for {
			next, seen := s.getAppliedState(column)
			dd(s, "waitForApplyAll: column %d: next,seen: %d,%d", column, next, seen)
			if next == seen {
				break
			}
			time.Sleep(time.Millisecond * 500)
		}
		dd(s, "done waitForApplyAll: %d", column)
	}
}
func (s *KVServer) getCommitState(column int64) (int64, int64) {
	col := s.log.columns[column]

	s.Lock()
	defer s.Unlock()

	return s.log.getNextCommits()[column], int64(len(col.Log))
}

func (s *KVServer) getAppliedState(column int64) (int64, int64) {
	col := s.log.columns[column]

	s.Lock()
	defer s.Unlock()

	return s.stateMachine.getNextApplies()[column], int64(len(col.Log))
}

func (s *KVServer) getLogLens() []int64 {
	return []int64{
		int64(len(s.log.columns[0].Log)),
		int64(len(s.log.columns[1].Log)),
		int64(len(s.log.columns[2].Log)),
	}
}

// allocNewInst allocates a log-sequence-number in local cmds.
func (s *KVServer) allocNewInst(column int64, cmd *Cmd) *Ins {
	col := s.log.columns[column]

	lsn := int64(len(col.Log))

	// VBal nil: state: prepared
	inst := &Ins{
		Val:   cmd,
		VBal:  nil,
		InsId: NewInsId(column, lsn, s.Id),
	}
	col.Log = append(col.Log, inst)

	inst.Deps = s.getLogLens()

	dd(s, "allocated: %s", inst.str())
	return inst.Clone()
}
func (s *KVServer) invalidateLastBal(column int64, bal *BallotNum) {
	col := s.log.columns[column]
	s.Lock()
	defer s.Unlock()

	col.Bal = nil

	if col.LastBal.Less(bal) {
		col.LastBal = bal.Clone()
	}
}

func serveKVServers(ids []int64) []*KVServer {

	var servers []*KVServer

	for _, aid := range ids {

		pkv := NewKVServer(aid)
		servers = append(servers, pkv)
		pkv.Start()
	}

	return servers
}
func rpcTo2(to int64, method string, req, reply proto.Message) error {

	ctx, cancel := context.WithTimeout(context.Background(), time.Second*30)
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
