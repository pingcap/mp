package server

import (
	"database/sql"
	_ "github.com/go-sql-driver/mysql"
	"github.com/ngaut/log"
	"github.com/pingcap/mp/protocol"
	"testing"
	"time"
)

func TestCRUD(t *testing.T) {
	cfg := &Config{
		Addr:     ":4000",
		User:     "root",
		Password: "root",
		LogLevel: "debug",
	}

	mockDrv := NewMockDriver()
	mockDrv.AddQuery("use test", nil, protocol.SERVER_STATUS_AUTOCOMMIT, 0, 0)

	serv, err := NewServer(cfg, NewMockDriver())
	if err != nil {
		log.Fatal(err)
	}
	go serv.Run()
	time.Sleep(time.Second)
	db, err := sql.Open("mysql", "root:root@tcp(127.0.0.1:4000)/test")
	if err != nil {
		log.Fatal(err)
	}
	_ = db
	//	err = db.Ping()
	//	if err != nil {
	//		log.Fatal(err)
	//	}

}
