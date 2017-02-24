package manager

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"net"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/skyshore/asynctask/proto"
	"golang.org/x/net/context"
	"golang.org/x/net/context/ctxhttp"
	"gopkg.in/inconshreveable/log15.v2"
)

type State string

const (
	StateNew     State = "NEW"
	StateRunning       = "RUNNING"
	StateDestory       = "DESTORY"
	StateOK            = "OK"
	StateERR           = "ERR"
)

const id_prefix = "id_"

type Meta struct {
	Url      string
	Method   string
	Typ      string
	Args     *json.RawMessage
	Delay    int64
	Time     int64
	Retry    int
	Callback string
}

type Info struct {
	Tid    int64
	Meta   *Meta
	Result *Result
}

type Task struct {
	ctx      context.Context
	cancel   context.CancelFunc
	m        *Manager
	Info     *Info
	shutdown bool
	stopCh   chan struct{}
	removeCh chan struct{}
}

type Result struct {
	State     State
	StartTime int64
	EndTime   int64

	Code int    `json:"code"`
	Msg  string `json:"msg"`
	Data []byte `json:"data"`
	//Data string `json:"data"`
}

func (r *Info) String() string {
	return fmt.Sprintf("taskId:%d, state:%s, starttime:%s, endtime:%s", r.Tid, r.Result.State, unix2str(r.Result.StartTime), unix2str(r.Result.EndTime))
}

func newTask2(info *Info, m *Manager) *Task {
	ctx, cancel := context.WithCancel(context.Background())
	return &Task{
		ctx:      ctx,
		cancel:   cancel,
		m:        m,
		Info:     info,
		stopCh:   make(chan struct{}, 1),
		removeCh: make(chan struct{}),
	}

}

func newTask3(id int64, meta *Meta, m *Manager) *Task {
	r := new(Result)
	r.State = StateNew
	info := &Info{
		Tid:    id,
		Meta:   meta,
		Result: r,
	}
	ctx, cancel := context.WithCancel(context.Background())

	return &Task{
		ctx:      ctx,
		cancel:   cancel,
		m:        m,
		Info:     info,
		stopCh:   make(chan struct{}, 1),
		removeCh: make(chan struct{}),
	}
}

func (t *Task) start(stopCh chan int64) {
	tid := t.Info.Tid
	r := t.Info.Result
	for !t.shutdown {
		switch r.State {
		case StateNew:
			t.doNew()
		case StateRunning:
			t.doRunning(stopCh)
		case StateOK:
			fallthrough
		case StateERR:
			t.doEnd()
			stopCh <- tid
			return
		}
	}
}

func (t *Task) doNew() {
	r := t.Info.Result
	now := time.Now().Unix()
	r.StartTime = now

	delay := t.delay()
	if delay <= 0 {
		log15.Warn("task already expired", "expire", delay)

		r.EndTime = now
		r.State = StateERR
		r.Code = 400
		r.Msg = "expired"
		if err := t.setState(); err != nil {
			r.Msg = fmt.Sprintf("%s,%v", r.Msg, err)
		}
	}
	r.State = StateRunning
	if err := t.setState(); err != nil {
		r.State = StateERR
		r.EndTime = time.Now().Unix()
		r.Msg = fmt.Sprintf("%s,%v", r.Msg, err)
	}
}

func (t *Task) doRunning(stopCh chan int64) {
	tid := t.Info.Tid
	//compute timeout
	delay := t.delay()
	r := t.Info.Result
	runtime := time.Now().Unix() - r.StartTime
	timeout := delay - runtime
	log15.Info("in doRunning",
		"lasttime", r.StartTime,
		"curtime", time.Now(),
		"runtime", runtime,
		"new-timeout", timeout)
	//timeout
	if timeout <= 0 {
		r.EndTime = time.Now().Unix()
		r.State = StateERR
		r.Msg = "task already timeout"
		log15.Warn("task already timeout", "timeout", timeout)
		r.State = StateERR
		r.Msg = "timeout"
		t.setState()
		return
	}
	select {
	case <-time.After(time.Duration(timeout) * time.Second):
		//do something
		response, err := t.do()
		if err != nil {
			log15.Error("request error", "err", err)
			r.EndTime = time.Now().Unix()
			r.State = StateERR
			r.Code = 400
			r.Msg = err.Error()
			t.setState()
			//todo retry
			return
		}
		defer response.Body.Close()
		data, err := ioutil.ReadAll(response.Body)
		log15.Info("task done...", "body", fmt.Sprintf("%s", data))

		r.EndTime = time.Now().Unix()
		r.State = StateOK
		r.Code = response.StatusCode
		r.Msg = response.Status
		r.Data = data
		t.setState()
	case <-t.removeCh: //remove by outer
		log15.Info("task canceled")
		r.EndTime = time.Now().Unix()
		r.State = StateDestory
		r.Code = 200
		r.Msg = "destory"

		log15.Info("task cancele done")
		t.setState()
		t.shutdown = true
	case <-t.stopCh: //stop by self
		stopCh <- tid
		t.shutdown = true
	}
}

func (t *Task) doEnd() {
	log15.Info("task done...")
	postResult(t.Info.Meta.Callback, t.Info.Result)
}

func (t *Task) stop() {
	t.cancel()
	close(t.stopCh)
}

func (t *Task) remove() {
	t.cancel()
	close(t.removeCh)
}

func (t *Task) do() (*http.Response, error) {
	var (
		req *http.Request
		err error
		q   url.Values
	)
	meta := t.Info.Meta
	c := newHttpClient(5)
	method := strings.ToUpper(meta.Method)
	switch method {
	case "GET":
		req, err = http.NewRequest(method, meta.Url, nil)
		if err != nil {
			return nil, err
		}
		q, err = t.parseArgs()
		if err != nil {
			return nil, err
		}
		req.URL.RawQuery = q.Encode()
		return ctxhttp.Do(t.ctx, c, req)

	case "PUT":
		fallthrough

	case "DELETE":
		fallthrough

	case "POST":
		if meta.Typ == "application/json" {
			req, err = http.NewRequest(method, meta.Url, bytes.NewReader([]byte(*meta.Args)))
			if err != nil {
				return nil, err
			}
			req.Header.Set("Content-Type", "application/json")
		} else {
			q, err = t.parseArgs()
			if err != nil {
				return nil, err
			}
			log15.Info("in post result", "url", meta.Url, "q", q.Encode())
			req, err = http.NewRequest(method, meta.Url, strings.NewReader(q.Encode()))
			if err != nil {
				return nil, err
			}
			req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
		}
		return ctxhttp.Do(t.ctx, c, req)

	default:
		return nil, errors.New("unsupported method")
	}
}

func (t *Task) parseArgs() (url.Values, error) {
	var args map[string]string
	err := json.Unmarshal([]byte(*t.Info.Meta.Args), &args)
	if err != nil {
		return nil, fmt.Errorf("parse args err:%v", err)
	}
	q := url.Values{}
	for k, v := range args {
		q.Add(k, v)
	}
	return q, nil
}

func (t *Task) setState() (err error) {
	//set key
	//sid := strconv.FormatInt(t.Info.Tid, 10)
	sid := fmt.Sprintf("%s%d", id_prefix, t.Info.Tid)
	log15.Info("update task status", "id", sid)
	//set value
	value, err := proto.Marshal(t.Info)
	if err != nil {
		return
	}
	req := proto.Request{proto.OpWrite, []byte(sid), value}
	//set reply
	rep := new(proto.Reply)
	err = t.m.RPC("KVS.Apply", &req, rep)
	return
}

func newHttpClient(timeout int64) (client *http.Client) {
	tr := &http.Transport{
		Dial: func(netw, addr string) (net.Conn, error) {
			deadline := time.Now().Add(time.Duration(timeout) * time.Second)
			c, err := net.DialTimeout(netw, addr, time.Duration(timeout)*time.Second)
			if err != nil {
				return nil, err
			}
			c.SetDeadline(deadline)
			return c, nil
		},
	}
	client = &http.Client{Transport: tr}
	return
}

func (t *Task) delay() (delay int64) {
	now := time.Now().Unix()
	meta := t.Info.Meta
	if meta.Delay != 0 {
		delay = meta.Delay
	} else if meta.Time > now {
		delay = meta.Time - now
	}
	return
}

func unix2str(t int64) string {
	return time.Unix(t, 0).Format("2006-01-02 15:04:05")
}

func postResult(postUrl string, info *Result) {
	if postUrl == "" {
		return
	}
	res, err := json.Marshal(info)
	if err != nil {
		log15.Error("encode result json err", "error", err)
		return
	}
	v := url.Values{"data": {string(res)}}
	log15.Info("post result", "post url", postUrl, "arg", v)
	resp, err := http.PostForm(postUrl, v)
	if err != nil {
		log15.Error("post err", "err", err)
		return
	}
	defer resp.Body.Close()
}
