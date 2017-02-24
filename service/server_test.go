package service

import (
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/skyshore/asynctask/config"
	"github.com/skyshore/asynctask/proto"
)

var (
	port = 10000
)

func getAddr() string {
	addr := fmt.Sprintf("127.0.0.1:%d", port)
	port++
	return addr
}

func newServerConfig(t *testing.T) (cfg *config.Config, baseDir string, addr string) {
	baseDir, err := ioutil.TempDir(os.TempDir(), "kv")
	if err != nil {
		t.Fatal(err)
	}
	addr = getAddr()
	log.Printf("baseDir:%s, addr:%s", baseDir, addr)
	cfg = &config.Config{
		Raft: config.Raft{
			Advertise:         addr,
			DataDir:           filepath.Join(baseDir, "raft"),
			SnapshotInterval:  config.Duration(3 * time.Second),
			SnapshotThreshold: 1000,
			EnableSingleNode:  true,
		},
		Server: config.Server{
			Listen: addr,
		},
		DB: config.DB{
			Dir: filepath.Join(baseDir, "db"),
		},
	}
	return
}

func newServer(t *testing.T) (server *Server, addr string, baseDir string) {
	var err error
	cfg, baseDir, addr := newServerConfig(t)
	server, err = NewServer(cfg)
	if err != nil {
		log.Println("new server err", err)
		os.RemoveAll(baseDir)
		t.Fatal(err)
	}
	return
}

func TestServerClose(t *testing.T) {
	server, _, base := newServer(t)
	defer os.RemoveAll(base)
	time.Sleep(time.Second)
	server.Close()
}

func TestReadWrite(t *testing.T) {
	server, _, base := newServer(t)

	time.Sleep(3 * time.Second)
	key := []byte("key")
	value := []byte("value")
	req := &proto.Request{proto.OpWrite, key, value}
	rep := new(proto.Reply)
	err := server.RPC("KVS.Apply", req, rep)
	if err != nil {
		t.Fatal(err)
	}
	req = &proto.Request{proto.OpRead, key, value}
	err = server.RPC("KVS.Read", req, rep)
	if string(rep.Data) != string(value) {
		t.Errorf("%s %s", rep.Data, value)
		t.Fatal(err)
	}

	os.RemoveAll(base)
	server.Close()
}
