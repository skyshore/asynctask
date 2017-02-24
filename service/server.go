package service

import (
	"errors"
	"net"
	"net/rpc"
	"os"
	"reflect"
	"sync"
	"time"

	"github.com/skyshore/asynctask/config"
	"github.com/skyshore/asynctask/proto"
	"github.com/hashicorp/raft"
	"gopkg.in/inconshreveable/log15.v2"
)

type Server struct {
	cfg *config.Config

	rpcListener net.Listener
	// raft
	raftLayer *RaftLayer
	//raftPeers     raft.PeerStore
	raftTransport *raft.NetworkTransport
	raft          *raft.Raft

	// Endpoints holds our RPC endpoints
	endpoints endpoints

	rpcServer *rpc.Server

	fsm *FSM

	mutex  sync.Mutex // protect conns
	conns  map[string]*rpc.Client
	logger log15.Logger
}

// Holds the RPC endpoints
type endpoints struct {
	Status *Status
	KVS    *KVS
}

func NewServer(cfg *config.Config) (*Server, error) {
	s := &Server{
		cfg:       cfg,
		conns:     make(map[string]*rpc.Client),
		rpcServer: rpc.NewServer(),
		logger:    log15.Root(),
	}
	err := s.setupRPC()
	if err != nil {
		return nil, err
	}
	err = s.setupRaft(cfg)
	if err != nil {
		log15.Error("setup raft err", "err", err)
		return nil, err
	}
	log15.Debug("setup raft done")

	go s.listen()
	return s, nil
}

func (s *Server) setupRaft(cfg *config.Config) error {
	// setup raft fsm
	fsm, err := NewFSM(&cfg.DB)
	if err != nil {
		return err
	}

	// setup transporter
	_, err = net.ResolveTCPAddr("tcp", cfg.Raft.Advertise)
	if err != nil {
		return err
	}
	//layer := NewRaftLayer(advertise, raftl)
	trans := raft.NewNetworkTransport(
		s.raftLayer,
		5,
		time.Second,
		os.Stderr,
	)

	// setup raft
	raft, err := NewRaft(&cfg.Raft, fsm, trans)
	if err != nil {
		return err
	}
	//s.raftLayer = layer
	s.raftTransport = trans
	s.raft = raft
	s.fsm = fsm

	return nil
}

func (s *Server) setupRPC() error {
	// setup rpc server
	s.endpoints.Status = &Status{s}
	kvs := NewKVS(s)
	s.endpoints.KVS = kvs
	err := s.rpcServer.RegisterName("KVS", kvs)
	if err != nil {
		return err
	}
	err = s.rpcServer.Register(s.endpoints.Status)
	if err != nil {
		return err
	}

	list, err := net.Listen("tcp", s.cfg.Server.Listen)
	if err != nil {
		return err
	}
	s.rpcListener = list

	// setup raft transporter
	advertise, err := net.ResolveTCPAddr("tcp", s.cfg.Raft.Advertise)
	if err != nil {
		return err
	}
	s.raftLayer = NewRaftLayer(advertise)
	return nil
}

// inmemCodec is used to do an RPC call without going over a network
type inmemCodec struct {
	method string
	args   interface{}
	reply  interface{}
	err    error
}

func (i *inmemCodec) ReadRequestHeader(req *rpc.Request) error {
	req.ServiceMethod = i.method
	return nil
}

func (i *inmemCodec) ReadRequestBody(args interface{}) error {
	sourceValue := reflect.Indirect(reflect.Indirect(reflect.ValueOf(i.args)))
	dst := reflect.Indirect(reflect.Indirect(reflect.ValueOf(args)))
	dst.Set(sourceValue)
	return nil
}

func (i *inmemCodec) WriteResponse(resp *rpc.Response, reply interface{}) error {
	if resp.Error != "" {
		i.err = errors.New(resp.Error)
		return nil
	}
	sourceValue := reflect.Indirect(reflect.Indirect(reflect.ValueOf(reply)))
	dst := reflect.Indirect(reflect.Indirect(reflect.ValueOf(i.reply)))
	dst.Set(sourceValue)
	return nil
}

func (i *inmemCodec) Close() error {
	return nil
}

// RPC is used to make a local RPC call
func (s *Server) RPC(method string, args interface{}, reply interface{}) error {
	codec := &inmemCodec{
		method: method,
		args:   args,
		reply:  reply,
	}
	if err := s.rpcServer.ServeRequest(codec); err != nil {
		return err
	}
	return codec.err
}

func (s *Server) IsLeader() bool {
	return s.raft.State() == raft.Leader
}

func (s *Server) LeaderCh() <-chan bool {
	return s.raft.LeaderCh()
}

func (s *Server) GenTid() int64 {
	return s.endpoints.KVS.genTid()
}

func (s *Server) SetTid() {
	s.endpoints.KVS.setTid()
}

func (s *Server) Leader() string {
	return s.raft.Leader()
}

func (s *Server) forward(method string, req, rep interface{}) (done bool, err error) {
	if s.raft.State() == raft.Leader {
		return false, nil
	}

	done = true

	leader := s.raft.Leader()
	if leader == "" {
		err = proto.ErrNoLeader
		return
	}

	s.mutex.Lock()
	cli, ok := s.conns[leader]
	s.mutex.Unlock()
	if !ok {
		// FIXME connection timeout hard code
		cli, err = proto.DialMsgpack(leader, time.Second*3)
		if err != nil {
			return
		}
		// cache connection
		s.mutex.Lock()
		s.conns[leader] = cli
		s.mutex.Unlock()
	}

	err = cli.Call(method, req, rep)
	if err != nil {
		// if is ServerError, do not close, otherwise close connection
		if _, ok := err.(rpc.ServerError); ok {
			return
		}
		cli.Close()
		s.mutex.Lock()
		delete(s.conns, leader)
		s.mutex.Unlock()
		return
	}

	return
}

func (s *Server) Close() error {
	if s.raft != nil {
		s.raftLayer.Close()
		s.raftTransport.Close()
		ret := s.raft.Shutdown()
		// wait raft shutdown
		ret.Error()
	}
	return s.fsm.Close()
}
