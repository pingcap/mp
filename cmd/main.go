package main

import (
	"flag"
	"net/http"
	_ "net/http/pprof"
	"os"
	"os/signal"
	"runtime"
	"syscall"

	"github.com/ngaut/log"
	"github.com/pingcap/mp/server"
)

var configFile = flag.String("config", "./etc/cfg.toml", "cm config file, support json & toml")

func main() {
	runtime.GOMAXPROCS(runtime.NumCPU())

	flag.Parse()

	if len(*configFile) == 0 {
		log.Error("must use a config file")
		return
	}

	cfg, err := server.ParseConfigFile(*configFile)
	if err != nil {
		log.Error(err.Error())
		return
	}

	log.SetLevelByString(cfg.LogLevel)

	log.CrashLog("./cm-proxy.dump")

	var svr *server.Server
	svr, err = server.NewServer(cfg)
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
