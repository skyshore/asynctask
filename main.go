package main

import (
	"flag"
	stdlog "log"
	"net/http"
	_ "net/http/pprof"

	"github.com/skyshore/asynctask/config"
	"github.com/skyshore/asynctask/log"
	"github.com/skyshore/asynctask/manager"
	"github.com/skyshore/asynctask/service"
	"github.com/BurntSushi/toml"
	gmux "github.com/gorilla/mux"
)

var (
	cfgpath = flag.String("config", "cfg.toml", "config file path")
	debug   = flag.Bool("d", true, "appenlog to stderr")
)

func setuplog(cfg *config.Config) {
	http.HandleFunc("/setloglvl", func(w http.ResponseWriter, r *http.Request) {
		r.ParseForm()
		lvl := r.FormValue("lvl")
		err := log.SetLevel(lvl)
		if err != nil {
			http.Error(w, err.Error(), 400)
		}
	})
	if !*debug {
		log.SetLogDir(cfg.LogDir)
		log.EnableRotate()
	} else {
		log.SetLevel("debug")
	}
}

func main() {
	flag.Parse()

	var cfg config.Config
	_, err := toml.DecodeFile(*cfgpath, &cfg)
	if err != nil {
		stdlog.Fatal(err)
	}

	// setup log
	setuplog(&cfg)
	srv, err := service.NewServer(&cfg)
	if err != nil {
		stdlog.Fatal(err)
	}

	m, err := manager.NewManager(&cfg, srv)
	if err != nil {
		stdlog.Fatal(err)
	}
	gmux := gmux.NewRouter()
	gmux.Handle("/asynctask/new", m)
	gmux.Handle("/asynctask/tasks", m)
	gmux.Handle("/asynctask/query", m)
	http.Handle("/", gmux)
	//start the manager
	go func() {
		http.ListenAndServe(cfg.Http, m)
	}()
	m.Run()
}
