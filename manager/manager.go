package manager

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/skyshore/asynctask/config"
	"github.com/skyshore/asynctask/proto"
	"github.com/skyshore/asynctask/service"
	"github.com/pquerna/ffjson/ffjson"
	"gopkg.in/inconshreveable/log15.v2"
)

var (
	ErrEnqueueTimeout = errors.New("timed out enqueuing operation")
	ErrNotFound       = errors.New("taskid not found")
)

const (
	jsonPeerPath = "https.json"
	enqueueLimit = 30 * time.Second
	newFormat    = "http://%s/asynctask/new"
	stopFormat   = "http://%s/asynctask/stop?id=%d"

	urlNew    = "/asynctask/new"
	urlQuery  = "/asynctask/query"
	urlStop   = "/asynctask/stop"
	urlStatus = "/hc/status.html"
)

type formatData struct {
	Code int         `json:"code"`
	Msg  string      `json:"msg"`
	Data interface{} `json:"data"`
}

type Manager struct {
	cfg     *config.Config
	applyCh chan *newFuture

	mu         sync.Mutex
	router     map[int64]*Task
	serv       *service.Server
	taskStopCh chan int64
	httpPeers  map[string]string
}

func NewManager(cfg *config.Config, s *service.Server) (*Manager, error) {
	//set rpc peers
	httpPeers, err := peers(cfg.Raft.DataDir)
	if err != nil {
		return nil, err
	}
	return &Manager{
		cfg:        cfg,
		applyCh:    make(chan *newFuture),
		router:     make(map[int64]*Task),
		serv:       s,
		taskStopCh: make(chan int64, 1),
		httpPeers:  httpPeers,
	}, nil
}

// RPC is used to make an RPC call to the Consul servers
// This allows the agent to implement the Consul.Interface
func (m *Manager) RPC(method string, args interface{}, reply interface{}) error {
	log15.Debug("rpc method", "method", method)
	if m.serv != nil {
		return m.serv.RPC(method, args, reply)
	}
	return nil
}

func (m *Manager) apply(req *http.Request, timeout time.Duration) ApplyFuture {
	var timer <-chan time.Time
	if timeout > 0 {
		timer = time.After(timeout)
	}
	newFuture := &newFuture{
		req: req,
	}
	newFuture.init()

	select {
	case <-timer:
		return errorFuture{ErrEnqueueTimeout}
	case m.applyCh <- newFuture:
		return newFuture
	}
}

func (m *Manager) Run() {
	log15.Info("manager start...")
	go func() {
		leaderCh := m.serv.LeaderCh()
		var stopCh chan struct{}
		for {
			select {
			case isLeader := <-leaderCh:
				if isLeader {
					stopCh = make(chan struct{})
					go m.recoverAll(stopCh)
					log15.Info("hor: cluster leadership acquired")
				} else if stopCh != nil {
					close(stopCh)
					stopCh = nil
					log15.Info("hor: cluster leadership lost")
				}
			}
		}
	}()
	for {
		select {
		case a := <-m.applyCh:
			err := m.execute(a)
			a.respond(err)
		case tid := <-m.taskStopCh:
			log15.Info(fmt.Sprintf("task %d normal finished", tid))
			delete(m.router, tid)
		}
	}
}

func (m *Manager) execute(future *newFuture) error {
	path := future.req.URL.Path
	switch {
	case strings.HasPrefix(path, urlNew):
		return m.startNew(future)
	case strings.HasPrefix(path, urlStop):
		return m.rm(future)
	}
	return nil
}

func (m *Manager) startNew(future *newFuture) error {
	if !m.serv.IsLeader() {
		var leader string
		err := m.RPC("Status.Leader", struct{}{}, &leader)
		if err != nil {
			return err
		}
		log15.Info("start new task, me is not leader", "leader", leader)
		leader = m.httpPeers[leader]
		contentType := future.req.Header.Get("Content-Type")
		resp, err := http.Post(fmt.Sprintf(newFormat, leader), contentType, future.req.Body)
		if err != nil {
			return err
		}
		defer resp.Body.Close()
		dec := json.NewDecoder(resp.Body)
		var data formatData
		err = dec.Decode(&data)
		if err != nil {
			return err
		}
		/*respData, err := ioutil.ReadAll(resp.Body)
		if err != nil {
			return err
		}*/
		if data.Code != 200 {
			return fmt.Errorf("request code not 200, msg:%s", data.Msg)
		}
		log15.Info("in startnew", "resp data", data.Data)
		//id, err := strconv.ParseInt(string(data.Data), 10, 64)
		id, err := AtoN(data.Data)
		if err != nil {
			return err
		}
		future.rep = id
		return nil
	}
	//self is leader
	decoder := json.NewDecoder(future.req.Body)
	var meta Meta
	err := decoder.Decode(&meta)
	if err != nil {
		log15.Error("decode packet error", "err", err)
		return err
	}
	log15.Info("receive new task", "task info", meta)
	tid := m.serv.GenTid()
	task := newTask3(tid, &meta, m)
	task.Info.Tid = tid
	log15.Info("gen new taskid", "id", tid, "leader", m.serv.Leader())
	m.router[tid] = task
	future.rep = tid
	go task.start(m.taskStopCh)
	return nil
}

func (m *Manager) rm(future *newFuture) error {
	r := future.req
	tid := tid(r.Form)
	if tid == nil {
		return errors.New("invalid, no task id")
	}
	id, err := strconv.Atoi(string(tid))
	if err != nil {
		return err
	}
	log15.Info("receive stop", "tid", id)

	if !m.serv.IsLeader() {
		var leader string
		err := m.RPC("Status.Leader", struct{}{}, &leader)
		if err != nil {
			return err
		}
		log15.Info("stop task, me is not leader", "leader", leader)
		leader = m.httpPeers[leader]
		resp, err := http.Get(fmt.Sprintf(stopFormat, leader, id))
		if err != nil {
			return err
		}
		defer resp.Body.Close()
		repData, err := ioutil.ReadAll(resp.Body)
		if err != nil {
			return err
		}
		future.rep = string(repData)
		log15.Debug("in rm", "resp data", future.rep)
		return nil
	}
	return m.remove(int64(id))
}

func (m *Manager) stopAll() {
	log15.Info("not leader stop tasks...")
	m.mu.Lock()
	defer m.mu.Unlock()
	for _, task := range m.router {
		task.stop()

	}
}

func (m *Manager) remove(tid int64) error {
	t, ok := m.router[tid]
	if !ok || t == nil {
		return ErrNotFound
	}
	if t != nil {
		t.remove()
		delete(m.router, tid)
	}
	return nil
}

//when get leadership
func (m *Manager) recoverAll(stopCh chan struct{}) {
	//load tid
	m.serv.SetTid()

	var entries []proto.Entry
	err := m.RPC("KVS.List", []byte(id_prefix), &entries)
	if err != nil {
		log15.Error("list error", "err", err)
	}
	length := len(entries)
	var keys []string
	for i := 0; i < length; i++ {
		entry := entries[i]
		keys = append(keys, string(entry.Key))
		var ret Info
		err = proto.Unmarshal(entry.Value, &ret)
		log15.Info("recover task", "task info ", &ret, "err", err)
		if ret.Result.State == StateRunning {
			tid := ret.Tid
			task := newTask2(&ret, m)
			log15.Info("recover taskid", "id", tid)
			m.router[tid] = task
			go func() {
				task.start(m.taskStopCh)
			}()
		}
	}
	log15.Info("recover all tasks done", "entries", keys)

	//wait for stop
	select {
	case <-stopCh:
		m.stopAll()
	}
}

// TODO thread safe
func (m *Manager) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	r.ParseForm()
	path := r.URL.Path
	switch {
	case strings.HasPrefix(path, urlStop):
		fallthrough

	case strings.HasPrefix(path, urlNew):
		future := m.apply(r, enqueueLimit)
		if err := future.Error(); err != nil {
			errorReply(w, 400, err)
		} else {
			rep := future.Response()
			reply(w, 200, "", rep)
		}

	case strings.HasPrefix(path, urlQuery):
		_, ok := r.Form["id"]
		if !ok {
			errorReply(w, 400, errors.New("invalid, no task id"))
			return
		}
		tid := r.Form["id"][0]
		log15.Info("receive query", "tid", tid)
		sid := fmt.Sprintf("%s%s", id_prefix, tid)
		r := proto.Request{proto.OpRead, []byte(sid), nil}
		rep := new(proto.Reply)
		err := m.RPC("KVS.Read", &r, rep)
		if err != nil {
			errorReply(w, 400, err)
			return
		}
		var ret Info
		err = proto.Unmarshal(rep.Data, &ret)
		log15.Info("get reply data", "task info ", &ret, "err", err)
		reply(w, 200, "", &ret)

	case strings.HasPrefix(path, urlStatus):
		http.ServeFile(w, r, "."+r.URL.Path)

	case strings.HasPrefix(path, "/asynctask/tasks"):
		stat := managerStat{
			TaskCount: len(m.router),
			Tasks:     make([]int64, 0),
		}
		for id, _ := range m.router {
			stat.Tasks = append(stat.Tasks, id)
		}
		buf, err := json.MarshalIndent(&stat, "", "  ")
		if err != nil {
			log15.Error("marshal json err", "error", err)
		}
		w.Write(buf)

	case strings.HasPrefix(path, "/leader"):
		var leader string
		err := m.RPC("Status.Leader", struct{}{}, &leader)
		if err != nil {
			errorReply(w, 400, err)
			return
		}
		reply(w, 200, "", leader)
	}
}

func tid(form url.Values) []byte {
	_, ok := form["id"]
	if !ok {
		return nil
	}
	tid := form["id"][0]
	return []byte(tid)
}

type managerStat struct {
	TaskCount int
	Tasks     []int64
}

func errorReply(w http.ResponseWriter, code int, err error) {
	w.WriteHeader(code)
	fmt.Fprintf(w, "%s", err)
}

func reply(w io.Writer, code int, msg string, data interface{}) error {
	newData := formatData{
		Code: code,
		Msg:  msg,
		Data: data,
	}
	r, err := ffjson.Marshal(newData)
	if err != nil {
		log15.Error("encode json error", "err", err)
		return err
	}
	fmt.Fprintf(w, "%s", r)
	return nil
}

func peers(base string) (map[string]string, error) {
	path := filepath.Join(base, jsonPeerPath)
	// Read the file
	buf, err := ioutil.ReadFile(path)
	if err != nil && !os.IsNotExist(err) {
		return nil, err
	}
	fmt.Printf("buf:%s", buf)
	// Check for no peers
	if len(buf) == 0 {
		return nil, nil
	}

	// Decode the peers
	var peerSet map[string]string
	dec := json.NewDecoder(bytes.NewReader(buf))
	if err := dec.Decode(&peerSet); err != nil {
		return nil, err
	}
	return peerSet, nil
}

func AtoN(s interface{}) (int64, error) {
	var err error
	switch v := s.(type) {
	case string:
		i, err := strconv.ParseInt(v, 10, 64)
		if err != nil {
			return -1, nil
		}
		return i, nil
	case int:
		return int64(v), nil
	case float32:
		return int64(v), nil
	case float64:
		return int64(v), nil
	default:
		err = errors.New("invalid type")
	}
	return -1, err
}
