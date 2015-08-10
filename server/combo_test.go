package server

import (
	"time"

	"github.com/pingcap/mp/etc"
	"github.com/pingcap/tidb"
	. "gopkg.in/check.v1"
)

type ComboTestSuite struct {
	driver *ComboDriver
	server *Server
}

var _ = Suite(&ComboTestSuite{})

func (ts *ComboTestSuite) SetUpSuite(c *C) {
	tidb.RemoveDatabase()
	tidb.NewDatabase()
	CreateTidbTestDatabase()
	ts.driver = NewComboDriver(true, &MysqlDriver{Addr: "127.0.0.1:3306"})
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

func (ts *ComboTestSuite) TearDownSuite(c *C) {
	ts.server.Close()
}

func (ts *ComboTestSuite) TestT(c *C) {
	if regression {
		runTestRegression(c)
	}
}

func (ts *ComboTestSuite) TestPreparedString(c *C) {
	runTestPreparedString(c)
	runTestSpecialType(c)
}
