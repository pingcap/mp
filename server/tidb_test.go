package server

import (
	"time"

	"github.com/pingcap/mp/etc"
	"github.com/pingcap/tidb"
	. "gopkg.in/check.v1"
)

type TidbTestSuite struct {
	tidbdrv *TidbDriver
	server  *Server
}

var _ = Suite(new(TidbTestSuite))

func (ts *TidbTestSuite) SetUpSuite(c *C) {
	tidb.RemoveDatabase()
	tidb.NewDatabase()
	CreateQlTestDatabase()
	ts.tidbdrv = &TidbDriver{}
	cfg := &etc.Config{
		Addr:     ":4000",
		User:     "root",
		Password: "",
		LogLevel: "debug",
	}
	server, err := NewServer(cfg, ts.tidbdrv)
	c.Assert(err, IsNil)
	ts.server = server
	go ts.server.Run()
	time.Sleep(time.Millisecond * 100)
}

func (ts *TidbTestSuite) TearDownSuite(c *C) {
	ts.server.Close()
}

func (ts *TidbTestSuite) TestRegression(c *C) {
	if regression {
		runTestRegression(c)
	}
}

func (ts *TidbTestSuite) TestUint64(c *C) {
	c.Skip("passing the test depends on tidb.")
	runTestPrepareResultFieldType(c)
}

func (ts *TidbTestSuite) TestSpecialType(c *C) {
	runTestSpecialType(c)
}

func (ts *TidbTestSuite) TestPreparedString(c *C) {
	runTestPreparedString(c)
}
