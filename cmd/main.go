package main

import (
	"os"
	"os/signal"
	"runtime"
	"syscall"

	"flag"
	"github.com/ngaut/log"
	"github.com/pingcap/mp/etc"
	"github.com/pingcap/mp/server"
	"github.com/pingcap/tidb"
)

var (
	mysqlAddr = flag.String("myaddr", "127.0.0.1:3306", "mysql address")
	mysqlPass = flag.String("mypass", "", "mysql password")
	runMode   = flag.String("mode", "combotidb", "tidb(tidb only)/mysql(mysql only)/combotidb(combo use tidb result)/combo(combo use mysql result)")
)

func main() {
	runtime.GOMAXPROCS(runtime.NumCPU())

	cfg := &etc.Config{
		Addr:     ":4000",
		User:     "root",
		Password: "",
		LogLevel: "debug",
	}
	flag.Parse()
	log.SetLevelByString(cfg.LogLevel)
	tidb.NewDatabase()
	server.CreateTidbTestDatabase()
	var svr *server.Server
	var driver server.IDriver
	var myDriver = &server.MysqlDriver{
		Addr: *mysqlAddr,
		Pass: *mysqlPass,
	}
	switch *runMode {
	case "tidb":
		driver = &server.TidbDriver{}
	case "mysql":
		driver = myDriver
	case "combotidb":
		driver = server.NewComboDriver(true, myDriver)
	case "combo":
		driver = server.NewComboDriver(false, myDriver)
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

	log.Error(svr.Run())
}
