package server

import (
	"database/sql"
	"testing"
	"time"

	_ "github.com/go-sql-driver/mysql"
	"github.com/ngaut/log"
	"github.com/pingcap/mp/etc"
	"github.com/pingcap/mp/protocol"
	. "gopkg.in/check.v1"
)

type testCRUDSuite struct {
	mockDrv *MockDriver
}

func newTestCRUDSuite() *testCRUDSuite {
	cfg := &etc.Config{
		Addr:     ":4000",
		User:     "root",
		Password: "",
		LogLevel: "debug",
	}
	ts := new(testCRUDSuite)
	ts.mockDrv = NewMockDriver()
	serv, err := NewServer(cfg, ts.mockDrv)
	if err != nil {
		log.Fatal(err)
	}
	go serv.Run()
	time.Sleep(time.Millisecond * 100)
	return ts
}

func (ts *testCRUDSuite) TestT(c *C) {
	ts.mockDrv.InitColumns("test.test.value|tiny.1|")
	status := protocol.SERVER_STATUS_AUTOCOMMIT
	ts.mockDrv.AddQuery("use test", nil, status, 0, 0)
	result := ts.mockDrv.BuildResult("..@@max_allowed_packet")
	result.AddRow(16777216)
	ts.mockDrv.AddQuery("SELECT @@max_allowed_packet", result, status, 0, 0)
	ts.mockDrv.AddQuery("DROP TABLE IF EXISTS test", nil, status, 0, 0)
	ts.mockDrv.AddQuery("CREATE TABLE test (value BOOL)", nil, status, 0, 0)
	ts.mockDrv.AddQuery("SELECT * FROM test", ts.mockDrv.BuildResult("test.test.value"), status, 0, 0)
	ts.mockDrv.AddQuery("INSERT INTO test VALUES (1)", nil, status, 0, 1)
	result = ts.mockDrv.BuildResult("test.test.value")
	result.AddRow(1)
	ts.mockDrv.AddQuery("SELECT value FROM test", result, status, 0, 0)
	ts.mockDrv.AddQuery("UPDATE test SET value = 0 WHERE value = 1", nil, status, 0, 1)
	result = ts.mockDrv.BuildResult("test.test.value")
	result.AddRow(0)
	ts.mockDrv.AddQuery("SELECT value FROM test", result, status, 0, 0)
	ts.mockDrv.AddQuery("DELETE FROM test WHERE value = 0", nil, status, 0, 1)
	ts.mockDrv.AddQuery("DELETE FROM test", nil, status, 0, 0)
	ts.mockDrv.AddQuery("DROP TABLE IF EXISTS test", nil, status, 0, 0)
	runTestCRUD(c)

	ts.mockDrv.AddQuery("use test", nil, status, 0, 0)
	result = ts.mockDrv.BuildResult("..@@max_allowed_packet")
	result.AddRow(16777216)
	ts.mockDrv.AddQuery("SELECT @@max_allowed_packet", result, status, 0, 0)
	ts.mockDrv.AddQuery("DROP TABLE IF EXISTS test", nil, status, 0, 0)

	result = ts.mockDrv.BuildResult("..@@version_comment").AddRow(protocol.DefaultVariables["version_comment"].Value)
	ts.mockDrv.AddQuery("select @@version_comment limit 1", result, status, 0, 0)
	result = ts.mockDrv.BuildResult("..DATABASE()").AddRow("")
	ts.mockDrv.AddQuery("SELECT DATABASE()", result, status, 0, 0)
	ts.mockDrv.AddQuery("use test", nil, status, 0, 0)
	ts.mockDrv.AddQuery("show databases", ts.mockDrv.BuildResult("..Database").AddRow("test"), status, 0, 0)
	ts.mockDrv.AddQuery("show tables", ts.mockDrv.BuildResult("..Tables_in_test").AddRow("test"), status, 0, 0)
	ts.mockDrv.AddQuery("DROP TABLE IF EXISTS test", nil, status, 0, 0)
	runTestClientInitial(c)
}

var _ = Suite(newTestCRUDSuite())

func TestCRUD(t *testing.T) {
	TestingT(t)
}

func runTests(c *C, dsn string, tests ...func(dbt *DBTest)) {
	db, err := sql.Open("mysql", dsn)
	c.Assert(err, IsNil, Commentf("Error connecting"))
	defer db.Close()

	db.Exec("DROP TABLE IF EXISTS test")

	dbt := &DBTest{c, db}
	for _, test := range tests {
		test(dbt)
		dbt.db.Exec("DROP TABLE IF EXISTS test")
	}
}

type DBTest struct {
	*C
	db *sql.DB
}

func (dbt *DBTest) fail(method, query string, err error) {
	if len(query) > 300 {
		query = "[query too large to print]"
	}
	dbt.Fatalf("Error on %s %s: %s", method, query, err.Error())
}

func (dbt *DBTest) mustExec(query string, args ...interface{}) (res sql.Result) {
	res, err := dbt.db.Exec(query, args...)
	dbt.Assert(err, IsNil, Commentf("Exec %s", query))
	return res
}

func (dbt *DBTest) mustQuery(query string, args ...interface{}) (rows *sql.Rows) {
	rows, err := dbt.db.Query(query, args...)
	dbt.Assert(err, IsNil, Commentf("Query %s", query))
	return rows
}

func runTestCRUD(c *C) {
	runTests(c, "root@tcp(localhost:4000)/test", func(dbt *DBTest) {
		// Create Table
		dbt.mustExec("CREATE TABLE test (value BOOL)")

		// Test for unexpected data
		var out bool
		rows := dbt.mustQuery("SELECT * FROM test")
		dbt.Assert(rows.Next(), Equals, false, Commentf("unexpected data in empty table"))

		// Create Data
		res := dbt.mustExec("INSERT INTO test VALUES (1)")
		count, err := res.RowsAffected()
		dbt.Assert(err, IsNil)
		dbt.Assert(count, Equals, int64(1))
		id, err := res.LastInsertId()
		dbt.Assert(err, IsNil)
		dbt.Assert(id, Equals, int64(0))

		// Read
		rows = dbt.mustQuery("SELECT value FROM test")
		if rows.Next() {
			rows.Scan(&out)
			dbt.Assert(out, Equals, true)
			dbt.Assert(rows.Next(), Equals, false, Commentf("unexpected data"))
		} else {
			dbt.Error("no data")
		}
		rows.Close()

		// Update
		res = dbt.mustExec(interpolateParams("UPDATE test SET value = 0 WHERE value = ?", false, 1))
		count, err = res.RowsAffected()
		dbt.Assert(err, IsNil)
		dbt.Assert(count, Equals, int64(1))

		// Check Update
		rows = dbt.mustQuery("SELECT value FROM test")
		if rows.Next() {
			rows.Scan(&out)
			dbt.Assert(out, Equals, false)
			dbt.Assert(rows.Next(), Equals, false, Commentf("unexpected data"))
		} else {
			dbt.Error("no data")
		}
		rows.Close()

		// Delete
		res = dbt.mustExec("DELETE FROM test WHERE value = 0")
		count, err = res.RowsAffected()
		dbt.Assert(err, IsNil)
		dbt.Assert(count, Equals, int64(1))

		// Check for unexpected rows
		res = dbt.mustExec("DELETE FROM test")
		count, err = res.RowsAffected()
		dbt.Assert(err, IsNil)
		dbt.Assert(count, Equals, int64(0))
	})
}

func runTestClientInitial(c *C) {
	runTests(c, "root@tcp(localhost:4000)/test", func(dbt *DBTest) {
		rows := dbt.mustQuery("select @@version_comment limit 1")
		dbt.Assert(rows.Next(), Equals, true)
		rows.Close()
		rows = dbt.mustQuery("SELECT DATABASE()")
		dbt.Assert(rows.Next(), Equals, true)
		rows.Close()
		dbt.mustExec("use test")
		dbt.mustQuery("show databases").Close()
		dbt.mustQuery("show tables").Close()
	})
}
