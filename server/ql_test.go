package server

import (
	"time"

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
	ctx, _ := ts.qldrv.OpenCtx(DEFAULT_CAPABILITY, 33, "test")
	_, err := ctx.Execute("CREATE DATABASE IF NOT EXISTS test")
	c.Assert(err, IsNil)
	ctx.Close()
	server, err := NewServer(cfg, ts.qldrv)
	c.Assert(err, IsNil)
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
