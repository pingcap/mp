package server

import (
	"time"

	"github.com/ngaut/log"
	"github.com/pingcap/mp/etc"
	. "gopkg.in/check.v1"
)

type QLTestSuite struct {
	qldrv  *QlDriver
	server *Server
}

var _ = Suite(new(QLTestSuite))

func (ts *QLTestSuite) SetUpSuite(c *C) {
	ts.qldrv = &QlDriver{}
	cfg := &etc.Config{
		Addr:     ":4000",
		User:     "root",
		Password: "",
		LogLevel: "debug",
	}
	ctx, _ := ts.qldrv.OpenCtx()
	_, err := ctx.Execute("CREATE DATABASE IF NOT EXISTS test")
	if err != nil {
		log.Fatal(err)
	}
	ctx.Close()
	server, err := NewServer(cfg, ts.qldrv)
	if err != nil {
		log.Fatal(err)
	}
	ts.server = server
	go ts.server.Run()
	time.Sleep(time.Millisecond * 100)
}

func (ts *QLTestSuite) TearDown(c *C) {
	ts.server.Close()
}

func (ts *QLTestSuite) TestT(c *C) {
	runTestCRUD(c)
}
