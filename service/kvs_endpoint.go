package service

import (
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"gopkg.in/inconshreveable/log15.v2"

	"github.com/skyshore/asynctask/proto"
)

type KVS struct {
	tid  int64
	serv *Server
}

func NewKVS(serv *Server) *KVS {
	kvs := &KVS{
		serv: serv,
	}
	kvs.initTid()
	return kvs
}

func (s *KVS) initTid() {
	//todo myid
	var myid int64
	now := time.Now().Unix()
	tid := now / 1000
	s.tid = (tid << 24 >> 8) | myid<<56
}

func (s *KVS) genTid() int64 {
	return atomic.AddInt64(&s.tid, 1)
}

func (s *KVS) setTid() error {
	//tid=id_95678169089
	r := proto.Request{proto.OpRead, []byte("tid"), nil}
	rep := new(proto.Reply)
	err := s.Read(&r, rep)
	if err != nil {
		return err
	}
	log15.Info("tid", "tid", string(rep.Data))
	sid := strings.TrimPrefix(string(rep.Data), "id_")
	tid, err := strconv.ParseInt(sid, 10, 64)
	if err != nil {
		return err
	}
	atomic.StoreInt64(&s.tid, tid)
	return nil
}

func (s *KVS) Read(req *proto.Request, rep *proto.Reply) error {
	if done, err := s.serv.forward("KVS.Read", req, rep); done {
		return err
	}
	//read
	v, err := s.serv.fsm.Get(req.Key, nil)
	if err != nil {
		return err
	}
	rep.Data = v
	return nil
}

func (s *KVS) List(req []byte, rep *[]proto.Entry) (err error) {
	if done, err := s.serv.forward("KVS.List", req, rep); done {
		return err
	}
	*rep, err = s.serv.fsm.List(req)
	return
}

func (s *KVS) Apply(req *proto.Request, rep *proto.Reply) (err error) {
	var done bool
	if done, err = s.serv.forward("KVS.Apply", req, rep); done {
		return
	}
	//leader
	var buf []byte
	buf, err = proto.Marshal(req)
	if err != nil {
		return
	}
	ret := s.serv.raft.Apply(buf, time.Second)
	err = ret.Error()
	if err != nil {
		return
	}

	if ret.Response() != nil {
		return ret.Response().(error)
	}
	rep.Data, err = proto.Marshal(s.tid)
	return
}
