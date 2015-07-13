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

func env(key, defaultValue string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return defaultValue
}

func main() {
	runtime.GOMAXPROCS(runtime.NumCPU())

	cfg := &etc.Config{
		Addr:     ":4000",
		User:     "root",
		Password: "",
		LogLevel: "debug",
	}

	log.SetLevelByString(cfg.LogLevel)
	server.CreateQlTestDatabase()
	var svr *server.Server
	var driver server.IDriver
	switch env("MP_DRIVER", "comboql") {
	case "ql":
		driver = &server.QlDriver{}
	case "mysql":
		driver = &server.MysqlDriver{}
	case "comboql":
		driver = server.NewComboDriver(true)
	case "combo":
		driver = server.NewComboDriver(false)
	}
	svr, err := server.NewServer(cfg, driver)
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
