package server

import (
	"time"

	. "gopkg.in/check.v1"
	"github.com/ngaut/log"
	"github.com/pingcap/mp/etc"
)

type MysqlTestSuite struct {
	driver *MysqlDriver
	server *Server
}

var _ = Suite(&MysqlTestSuite{})

func (ts *MysqlTestSuite) SetUpSuite(c *C){
	ts.driver = &MysqlDriver{}
	cfg := &etc.Config{
		Addr:     ":4000",
		User:     "root",
		Password: "",
		LogLevel: "debug",
	}
	server, err := NewServer(cfg, ts.driver)
	if err != nil {
		log.Fatal(err)
	}
	ts.server = server
	go ts.server.Run()
	time.Sleep(time.Millisecond * 100)
}

func (ts *MysqlTestSuite) TearDownSuite(c *C) {
	ts.server.Close()
}

func (ts *MysqlTestSuite) TestT(c *C) {
	runTestCRUD(c)
}