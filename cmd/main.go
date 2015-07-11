package main

import (
	"net/http"
	_ "net/http/pprof"
	"os"
	"os/signal"
	"runtime"
	"syscall"

	"github.com/ngaut/log"
	"github.com/pingcap/mp/etc"
	"github.com/pingcap/mp/server"
)

func main() {
	runtime.GOMAXPROCS(runtime.NumCPU())

	cfg := &etc.Config{
		Addr:     ":4000",
		User:     "root",
		Password: "",
		LogLevel: "debug",
	}

	log.SetLevelByString(cfg.LogLevel)

	var svr *server.Server
	svr, err := server.NewServer(cfg, &server.QlDriver{})
	if err != nil {
		log.Error(err.Error())
		return
	}

	sc := make(chan os.Signal, 1)
	signal.Notify(sc,
		syscall.SIGHUP,
		syscall.SIGINT,
		syscall.SIGTERM,
		syscall.SIGQUIT)

	go func() {
		sig := <-sc
		log.Infof("Got signal [%d] to exit.", sig)
		svr.Close()
		os.Exit(0)
	}()

	go svr.Run()

	//todo: using configuration
	log.Warning("started")
	http.ListenAndServe(":8888", nil)
}
