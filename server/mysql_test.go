package server

import (
	"time"

	"github.com/pingcap/mp/etc"
	. "gopkg.in/check.v1"
)

type MysqlTestSuite struct {
	driver *MysqlDriver
	server *Server
}

var _ = Suite(&MysqlTestSuite{})

func (ts *MysqlTestSuite) SetUpSuite(c *C) {
	ts.driver = &MysqlDriver{}
	cfg := &etc.Config{
		Addr:     ":4000",
		User:     "root",
		Password: "",
		LogLevel: "debug",
	}
	server, err := NewServer(cfg, ts.driver)
	c.Assert(err, IsNil)
	ts.server = server
	go ts.server.Run()
	time.Sleep(time.Millisecond * 100)
}

func (ts *MysqlTestSuite) TearDownSuite(c *C) {
	ts.server.Close()
}

func (ts *MysqlTestSuite) TestT(c *C) {
	if regression {
		runTestRegression(c)
	}
}
