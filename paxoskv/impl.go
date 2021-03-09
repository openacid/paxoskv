package paxoskv

import (
	"errors"
	"fmt"
	"net"
	"sync"
	"time"

	"github.com/kr/pretty"
	"golang.org/x/net/context"
	"google.golang.org/grpc"
	"google.golang.org/grpc/reflection"
	"google.golang.org/protobuf/proto"
)

var (
	NotEnoughQuorum  = errors.New("not enough Quorum")
	AcceptorBasePort = int64(3333)
)

// RunPaxos execute the paxos phase-1 and phase-2 to establish multi values.
// The values to establish are log entries of multi paxos.
// Every log entry is a paxos instance.
//
// cmds contains values the caller wants to propose.
// It returns the established values, which may be not the proposed
// value.
//
// If an entry in cmds is not nil, this func tries to establish it with two phase paxos.
//
// If an entry is nil, it act as a reading operation:
// it reads the state of the paxos instance by running only Phase1.
func RunPaxos(bal *BallotNum, acceptorIds []int64, fromLSN int64, vals map[int64]*Cmd) map[int64]*Cmd {

	maxLSN := int64(-1)
	for lsn := range vals {
		if lsn > maxLSN {
			maxLSN = lsn
		}
	}
	quorum := len(acceptorIds)/2 + 1

	for {
		maxVotedVal, higherBal, err := Phase1(bal, acceptorIds, fromLSN, quorum)
		if err != nil {
			pretty.Logf("R%d: P: fail to run phase-1: highest ballot: %s, increment ballot and retry", bal.Id, higherBal.str())
			bal.N = higherBal.N + 1
			continue
		}
		pretty.Logf("R%d: P: %v", bal.Id, maxVotedVal)

		for lsn := range maxVotedVal {
			if lsn > maxLSN {
				maxLSN = lsn
			}
		}
		pretty.Logf("R%d: P: maxLSN: %d", bal.Id, maxLSN)

		// A mutli paxos runs Phase2 for a continuous range of instances.
		// Holes are filled with NOOP commands.
		// An instance not voted by a quorum can be used to propose caller's value.

		for lsn := fromLSN; lsn <= maxLSN; lsn++ {
			if maxVotedVal[lsn] == nil {
				cmd, found := vals[lsn]
				if found {
					if cmd == nil {
						pretty.Logf("R%d: P: no value to propose in phase-2: lsn: %d cmd: %s", bal.Id, lsn, cmd.str())
					} else {
						maxVotedVal[lsn] = cmd
						pretty.Logf("R%d: P: no voted value seen lsn: %d, propose my value: %s", bal.Id, lsn, cmd.str())
					}
				} else {
					maxVotedVal[lsn] = &Cmd{
						LSN:    lsn,
						Author: bal.Clone(),
						Key:    "NOOP",
						Vi64:   0,
					}
					pretty.Logf("R%d: P: fill hole lsn: %d", bal.Id, lsn)
				}
			} else {
				pretty.Logf("R%d: P: ChooseOtherValue lsn: %d, %s", bal.Id, lsn, maxVotedVal[lsn].str())
			}
		}

		for lsn, cmd := range maxVotedVal {
			pretty.Logf("R%d: P: value for Phase2: lsn: %d cmd: %s", bal.Id, lsn, cmd.str())
		}

		higherBal, err = Phase2(bal, acceptorIds, maxVotedVal, quorum)
		if err != nil {
			pretty.Logf("R%d: P: fail to run phase-2: highest ballot: %s, increment ballot and retry", bal.Id, higherBal.str())
			bal.N = higherBal.N + 1
			continue
		}

		for lsn, cmd := range vals {
			pretty.Logf("R%d: P: values are voted by a Quorum and has been safe: lsn: %d cmd: %s", bal.Id, lsn, cmd.str())
		}

		return maxVotedVal
	}
}

// Phase1 run paxos phase-1 on the instances in range [fromLSN, +oo).
// If a higher ballot number is seen and phase-1 failed to constitute a Quorum,
// the highest ballot number that is seen and a NotEnoughQuorum is returned.
func Phase1(bal *BallotNum, acceptorIds []int64, fromLSN int64, quorum int) (map[int64]*Cmd, *BallotNum, error) {

	req := &PrepareReq{
		FromLSN: fromLSN,
		Bal:     bal.Clone(),
	}

	higherBal := *bal

	replies := make([]*PrepareReply, 0)
	for _, aid := range acceptorIds {
		reply := new(PrepareReply)
		err := rpcTo(bal.Id, aid, "Prepare", req, reply)
		if err != nil {
			continue
		}

		if bal.Cmp(reply.LastBal) < 0 {
			higherBal = *reply.LastBal
			continue
		}

		replies = append(replies, reply)
	}

	if len(replies) < quorum {
		return nil, &higherBal, NotEnoughQuorum
	}

	// find the voted value with highest vbal
	maxVoted := make(map[int64]*Acceptor)
	for _, r := range replies {
		for _, acc := range r.Acceptors {
			lsn := acc.Val.LSN
			cur := maxVoted[lsn]
			if cur == nil || acc.VBal.Cmp(cur.VBal) >= 0 {
				maxVoted[lsn] = acc
			}
		}
	}

	pretty.Logf("R%d: P: Phase1: maxVoted: %#v", bal.Id, maxVoted)

	m := make(map[int64]*Cmd)
	for lsn, acc := range maxVoted {
		m[lsn] = acc.Val
	}
	return m, nil, nil
}

// Phase2 run paxos phase-2 on multiple instances.
// If a higher ballot number is seen and phase-2 failed to constitute a Quorum,
// the highest ballot number and a NotEnoughQuorum is returned.
func Phase2(bal *BallotNum, acceptorIds []int64, vals map[int64]*Cmd, quorum int) (*BallotNum, error) {

	req := &AcceptReq{
		Bal:  bal.Clone(),
		Cmds: vals,
	}

	higherBal := *bal

	ok := 0
	for _, aid := range acceptorIds {
		reply := new(AcceptReply)
		err := rpcTo(bal.Id, aid, "Accept", req, reply)
		if err != nil {
			continue
		}

		pretty.Logf("R%d: P: hdl Accept reply: %s", bal.Id, reply)
		if bal.Cmp(reply.LastBal) < 0 {
			higherBal = *reply.LastBal
			continue
		}

		ok++
		if ok == quorum {
			return nil, nil
		}
	}

	return &higherBal, NotEnoughQuorum
}

// rpcTo send grpc request to all acceptors
func rpcTo(id int64, acceptorId int64, method string, req, reply proto.Message) error {

	// With heavy competition an operation takes longer time to finish.
	// There is still chance no leader is established before timeout.

	ctx, cancel := context.WithTimeout(context.Background(), time.Second*3)
	defer cancel()

	address := fmt.Sprintf("127.0.0.1:%d", AcceptorBasePort+int64(acceptorId))
	conn, err := grpc.Dial(address, grpc.WithInsecure())
	if err != nil {
		pretty.Logf("did not connect: %s", err.Error())
		return err
	}
	defer conn.Close()

	pretty.Logf("R%d: P: send %s-req to: R%d: %s", id, method, acceptorId, req.(strer).str())

	err = conn.Invoke(ctx, "/paxoskv.PaxosKV/"+method, req, reply)
	if err != nil {
		pretty.Logf("R%d: P: recv %s-reply from: R%d: err: %s", id, method, acceptorId, err)
	} else {
		pretty.Logf("R%d: P: recv %s-reply from: R%d: %s", id, method, acceptorId, reply.(strer).str())
	}
	return err
}

// KVServer provides: a single Proposer with field Bal, multiple Acceptors with Log.
type KVServer struct {
	sync.Mutex

	Id          int64
	AcceptorIds []int64
	Quorum      int

	// Proposer ballot
	Bal     *BallotNum
	LastBal *BallotNum
	// A semaphore for election.
	// A multi-paxos impl only need one proposer.
	electSem chan int

	Log     []*Acceptor
	Storage map[string]*Acceptor

	// The first log that is not committed
	nonCommitted int64

	srv *grpc.Server
}

func NewKVServer(acceptorIds []int64, id int64) *KVServer {
	pkv := &KVServer{
		Id:          id,
		LastBal:     &BallotNum{N: 0, Id: id},
		AcceptorIds: append([]int64{}, acceptorIds...),
		Quorum:      len(acceptorIds)/2 + 1,

		Storage: map[string]*Acceptor{},

		electSem: make(chan int, 1),
	}

	pkv.electSem <- 1
	return pkv
}

func (s *KVServer) getAcceptor(lsn int64) *Acceptor {

	for int(lsn) >= len(s.Log) {
		s.Log = append(s.Log, nil)
	}

	if s.Log[lsn] == nil {
		return &Acceptor{VBal: &BallotNum{}}
	}
	return s.Log[lsn]
}

// Prepare handles Prepare request.
// Handling Prepare needs only the `Bal` field.
// The reply contains all Acceptor the proposer wants to establish a value on.
func (s *KVServer) Prepare(c context.Context, req *PrepareReq) (*PrepareReply, error) {

	pretty.Logf("R%d: A: recv Prepare-req: %s", s.Id, req.str())

	s.Lock()
	defer s.Unlock()

	reply := &PrepareReply{
		LastBal:   s.LastBal.Clone(),
		Acceptors: make([]*Acceptor, 0),
	}

	if req.Bal.Cmp(s.LastBal) >= 0 {
		*s.LastBal = *req.Bal
		for i := req.FromLSN; i < int64(len(s.Log)); i++ {
			if s.Log[i] != nil {
				reply.Acceptors = append(reply.Acceptors, s.Log[i].Clone())
			}
		}
	}

	pretty.Logf("R%d: A: send Prepare-reply: %s", s.Id, reply.str())

	return reply, nil
}

// Accept handles Accept request.
func (s *KVServer) Accept(c context.Context, req *AcceptReq) (*AcceptReply, error) {

	pretty.Logf("R%d: A: recv Accept-req: %s", s.Id, req.str())

	s.Lock()
	defer s.Unlock()

	reply := &AcceptReply{
		LastBal: s.LastBal.Clone(),
	}

	accepted := req.Bal.Cmp(s.LastBal) >= 0

	if accepted {
		for lsn, cmd := range req.Cmds {
			acc := s.getAcceptor(lsn)
			acc.Val = cmd
			acc.VBal = req.Bal

			s.Log[lsn] = acc
		}

		// This is a common optimization of paxos:
		// In the original impl an Accept-req can only be sent to prepared acceptors.
		// It could be sent to a non-prepared acceptor, if the LastBal on the acceptor <= req.Bal.
		// But it requires to update the LastBal, just like it has granted a Prepare-req from the proposer.
		//
		// See detailed discussion:  https://stackoverflow.com/questions/29880949/contradiction-in-lamports-paxos-made-simple-paper
		s.LastBal = req.Bal
	}

	pretty.Logf("R%d: A: accepted: %t", s.Id, accepted)

	return reply, nil
}

// Commit handles Commit request.
// It is possible an acceptor votes a new value for a committed instance,
// e.g. update the VBal to a newer value after it is committed.
// This results in inconsistent Log[i].VBal but the Log[i].Val itself is consistent on all servers.
func (s *KVServer) Commit(c context.Context, req *CommitReq) (*CommitReply, error) {

	pretty.Logf("R%d: A: hdl Commit-req: %s", s.Id, req.str())

	s.Lock()
	defer s.Unlock()

	for lsn, cmd := range req.Cmds {
		acc := s.getAcceptor(lsn)

		acc.Val = cmd
		acc.Committed = true

		s.Log[lsn] = acc

		for int(s.nonCommitted) < len(s.Log) &&
			s.Log[s.nonCommitted] != nil &&
			s.Log[s.nonCommitted].Committed {
			s.nonCommitted++
		}

		s.apply(lsn)
	}

	return &CommitReply{}, nil
}

func (s *KVServer) apply(lsn int64) {

	acc := s.Log[lsn]

	pretty.Logf("R%d: A: apply: lsn=%d: %s", s.Id, lsn, acc.str())

	cur := s.Storage[acc.Val.Key]
	if cur != nil && cur.Val.LSN >= lsn {
		pretty.Logf("R%d: A: AlreadyApplied: lsn=%d: overridden by:%d", s.Id, lsn, cur.Val.LSN)
		return
	}
	s.Storage[acc.Val.Key] = acc
}

// Set impl the KV API and handles a Set request from client.
// Only the Key and Vi64 should be set in req.
func (s *KVServer) Set(c context.Context, req *Cmd) (*Cmd, error) {

	pretty.Logf("R%d: P: hdl Set", s.Id)

	// This demo is an aggressive impl of multipaxos:
	// Unlike raft a follower forwards a write-op to the leader,
	// in this demo it just seize the leadership.
	//
	// This is not a practical strategy but only meant to introduce more competition thus to expose problems quickly.
	// It repeats electing itself as leader until timed out.
	for {
		select {
		case <-c.Done():
			return nil, c.Err()
		default:
		}
		chosen := s.set(c, req)
		if chosen != nil {
			return chosen, nil
		}
	}
}

func (s *KVServer) set(c context.Context, cmd *Cmd) *Cmd {

	pretty.Logf("R%d: hdl set", s.Id)

	// electMe has 3 steps:
	// - Run Phase1 to establish leadership
	// - Re-run Phase2 for all seen logs(paxos instances) that are not committed.
	// - Commit them.
	// Then new instance is allowed to propose.
	// This is a simplified strategy. With a practical multi-paxos impl, proposing new instances and rebuilding logs are safe to run concurrently.

	bal := s.electMe()

	// After leadership established, all safe instances are rebuilt on this
	// proposer.

	// The election process may repeat several times before a leader is established.
	// Since election itself rebuild logs(paxos instances),
	// a previously proposed(but not finished) instance may have been committed by other leader.
	// Thus we'd find it in local logs first before allocating a new log entry.
	if cmd.Author != nil {
		lsn := cmd.LSN
		s.Lock()
		if int(lsn) < len(s.Log) && proto.Equal(cmd, s.Log[lsn].Val) {
			pretty.Logf("R%d: P: cmd is already committed when set: %s", s.Id, s.Log[lsn].str())
			s.Unlock()
			return cmd
		}
		s.Unlock()
	}

	// cmd is not in local log, or it is overridden by other cmd.
	// Try to re-propose it in a new log entry.

	lsn := s.allocLSN()

	cmd.LSN = lsn
	cmd.Author = bal.Clone()

	// A multi-paxos only need to run Phase2 for a proposal.
	// Phase1 is required only when Phase2 fails.

	higherBal, err := Phase2(bal, s.AcceptorIds, map[int64]*Cmd{lsn: cmd}, s.Quorum)
	if err != nil {
		pretty.Logf("R%d: P: fail to run phase-2: highest ballot: %s, increment ballot and retry", bal.Id, higherBal.str())

		// Leadership is taken by another server,
		// clear local proposer.
		s.Lock()
		if higherBal.Cmp(s.LastBal) > 0 {
			s.LastBal = higherBal.Clone()
		}
		if s.Bal != nil && s.Bal.Cmp(bal) == 0 {
			s.Bal = nil
		}
		s.Unlock()
		return nil
	}

	pretty.Logf("R%d: P: value is voted by a Quorum and has been safe: %s", bal.Id, cmd.str())

	creq := &CommitReq{Cmds: map[int64]*Cmd{lsn: cmd.Clone()}}
	for _, aid := range s.AcceptorIds {
		_ = rpcTo(s.Id, aid, "Commit", creq, &CommitReply{})
	}
	pretty.Logf("R%d: P: Commit done: %s", bal.Id, cmd.str())
	return cmd
}

// allocLSN allocates a log-sequence-number in local logs.
func (s *KVServer) allocLSN() int64 {
	s.Lock()
	defer s.Unlock()
	s.Log = append(s.Log, nil)
	lsn := int64(len(s.Log)) - 1
	pretty.Logf("R%d: P: alloc: %d", s.Id, lsn)
	return lsn
}

// electMe run paxos Phase1 to become leader, and re-run Phase2 and Commit for uncommitted instances.
func (s *KVServer) electMe() *BallotNum {

	<-s.electSem
	defer func() { s.electSem <- 1 }()

	s.Lock()
	bal := s.Bal
	nonCommitted := s.nonCommitted
	lastN := s.LastBal.N
	s.Unlock()

	if bal != nil {
		return bal
	}

	// run paxos to establish leadership and commit uncommitted logs

	bal = &BallotNum{N: lastN + 1, Id: s.Id}

	pretty.Logf("R%d: P: electMe: %s", s.Id, bal.str())

	voted := RunPaxos(bal, s.AcceptorIds, nonCommitted, map[int64]*Cmd{})

	creq := &CommitReq{Cmds: voted}
	for _, aid := range s.AcceptorIds {
		_ = rpcTo(s.Id, aid, "Commit", creq, &CommitReply{})
	}

	s.Lock()
	defer s.Unlock()

	s.Bal = bal

	if bal.Cmp(s.LastBal) > 0 {
		s.LastBal = bal.Clone()
	}

	// clean up logs thus no annoying holes in log:
	for len(s.Log) > 0 && s.Log[len(s.Log)-1] == nil {
		pretty.Logf("R%d: clean nil log: %d", s.Id, len(s.Log)-1)
		s.Log = s.Log[:len(s.Log)-1]
	}

	for lsn, l := range s.Log {
		pretty.Logf("R%d: repaired log: %d %s", s.Id, lsn, l.str())
	}
	return bal
}

// Get impl the KV-API get method.
// Only req.Key should be specified.
func (s *KVServer) Get(c context.Context, req *Cmd) (*Cmd, error) {
	s.Lock()
	defer s.Unlock()

	a, found := s.Storage[req.Key]
	pretty.Logf("R%d: G: v: %s", s.Id, a.str())
	if found {
		v := proto.Clone(a.Val).(*Cmd)
		return v, nil
	}
	return nil, nil
}

// ServeAcceptors starts a grpc server for every acceptor.
func ServeAcceptors(acceptorIds []int64) []*KVServer {

	var servers []*KVServer

	for _, aid := range acceptorIds {
		addr := fmt.Sprintf(":%d", AcceptorBasePort+int64(aid))

		lis, err := net.Listen("tcp", addr)
		if err != nil {
			panic(pretty.Sprintf("listen: %s %s", addr, err.Error()))
		}

		s := grpc.NewServer()
		pkv := NewKVServer(acceptorIds, aid)
		pkv.srv = s

		RegisterPaxosKVServer(s, pkv)
		reflection.Register(s)
		pretty.Logf("R%d: serving on %s ...", aid, addr)
		servers = append(servers, pkv)
		go s.Serve(lis)
	}

	return servers
}
