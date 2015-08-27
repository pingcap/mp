package main

import (
	"fmt"
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
	store     = flag.String("store", "goleveldb", "registered store name, [memory, goleveldb, boltdb]")
	storePath = flag.String("store_path", "/tmp/tidb", "tidb storage path")
)

//version infomation
var (
	buildstamp = "No Build Stamp Provided"
	githash    = "No Git Hash Provided"
)

func main() {
	fmt.Printf("Git Commit Hash:%s\nUTC Build Time :%s\n", githash, buildstamp)
	runtime.GOMAXPROCS(runtime.NumCPU())

	cfg := &etc.Config{
		Addr:     ":4000",
		User:     "root",
		Password: "",
		LogLevel: "debug",
	}
	flag.Parse()
	log.SetLevelByString(cfg.LogLevel)
	store, err := tidb.NewStore(fmt.Sprintf("%s://%s", *store, *storePath))
	if err != nil {
		log.Error(err.Error())
		return
	}
	server.CreateTidbTestDatabase(store)
	var svr *server.Server
	var driver server.IDriver
	var myDriver = &server.MysqlDriver{
		Addr: *mysqlAddr,
		Pass: *mysqlPass,
	}
	switch *runMode {
	case "tidb":
		driver = server.NewTidbDriver(store)
	case "mysql":
		driver = myDriver
	case "combotidb":
		driver = server.NewComboDriver(true, myDriver, store)
	case "combo":
		driver = server.NewComboDriver(false, myDriver, store)
	}
	svr, err = server.NewServer(cfg, driver)
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
