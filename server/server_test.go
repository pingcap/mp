package server

import (
	"database/sql"
	_ "github.com/go-sql-driver/mysql"
	"github.com/ngaut/log"
	"github.com/pingcap/mp/protocol"
	"testing"
	"time"
)

//used to test mysql console client.
func TestLong(t *testing.T) {
	cfg := &Config{
		Addr:     ":4000",
		User:     "root",
		Password: "",
		LogLevel: "debug",
	}
	status := protocol.SERVER_STATUS_AUTOCOMMIT
	mockDrv := NewMockDriver()
	mockDrv.InitColumns("..@@version_comment|varchar.255|", "test.test.value|tiny.1|")
	result := mockDrv.BuildResult("..@@version_comment").AddRow(protocol.DefaultVariables["version_comment"].Value)
	mockDrv.AddQuery("select @@version_comment limit 1", result, status, 0, 0)
	result = mockDrv.BuildResult("..DATABASE()").AddRow("")
	mockDrv.AddQuery("SELECT DATABASE()", result, status, 0, 0)
	mockDrv.AddQuery("use test", nil, status, 0, 0)
	mockDrv.AddQuery("show databases", mockDrv.BuildResult("..Database").AddRow("test"), status, 0, 0)
	mockDrv.AddQuery("show tables", mockDrv.BuildResult("..Tables_in_test").AddRow("test"), status, 0, 0)
	serv, err := NewServer(cfg, mockDrv)
	if err != nil {
		log.Fatal(err)
	}
	_ = serv
	//	serv.Run()
}

func TestCRUD(t *testing.T) {
	cfg := &Config{
		Addr:     ":4001",
		User:     "root",
		Password: "",
		LogLevel: "debug",
	}

	mockDrv := NewMockDriver()
	mockDrv.InitColumns("..@@max_allowed_packet|varchar.255|", "test.test.value|tiny.1|")

	status := protocol.SERVER_STATUS_AUTOCOMMIT

	mockDrv.AddQuery("use test", nil, status, 0, 0)

	result := mockDrv.BuildResult("..@@max_allowed_packet")
	result.AddRow(16777216)

	mockDrv.AddQuery("SELECT @@max_allowed_packet", result, status, 0, 0)
	mockDrv.AddQuery("DROP TABLE IF EXISTS test", nil, status, 0, 0)
	mockDrv.AddQuery("CREATE TABLE test (value BOOL)", nil, status, 0, 0)
	mockDrv.AddQuery("SELECT * FROM test", mockDrv.BuildResult("test.test.value"), status, 0, 0)
	mockDrv.AddQuery("INSERT INTO test VALUES (1)", nil, status, 0, 1)
	result = mockDrv.BuildResult("test.test.value")
	result.AddRow(1)
	mockDrv.AddQuery("SELECT value FROM test", result, status, 0, 0)
	mockDrv.AddQuery("UPDATE test SET value = 0 WHERE value = 1", nil, status, 0, 1)
	result = mockDrv.BuildResult("test.test.value")
	result.AddRow(0)
	mockDrv.AddQuery("SELECT value FROM test", result, status, 0, 0)
	mockDrv.AddQuery("DELETE FROM test WHERE value = 0", nil, status, 0, 1)
	mockDrv.AddQuery("DELETE FROM test", nil, status, 0, 0)
	mockDrv.AddQuery("DROP TABLE IF EXISTS test", nil, status, 0, 0)

	serv, err := NewServer(cfg, mockDrv)
	if err != nil {
		log.Fatal(err)
	}
	go serv.Run()
	time.Sleep(time.Second)
	runTestCRUD(t)
}

func runTests(t *testing.T, dsn string, tests ...func(dbt *DBTest)) {
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		t.Fatalf("Error connecting: %s", err.Error())
	}
	defer db.Close()

	db.Exec("DROP TABLE IF EXISTS test")

	dbt := &DBTest{t, db}
	for _, test := range tests {
		test(dbt)
		dbt.db.Exec("DROP TABLE IF EXISTS test")
	}
}

type DBTest struct {
	*testing.T
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
	if err != nil {
		dbt.fail("Exec", query, err)
	}
	return res
}

func (dbt *DBTest) mustQuery(query string, args ...interface{}) (rows *sql.Rows) {
	rows, err := dbt.db.Query(query, args...)
	if err != nil {
		dbt.fail("Query", query, err)
	}
	return rows
}

func runTestCRUD(t *testing.T) {
	runTests(t, "root@tcp(localhost:4001)/test", func(dbt *DBTest) {
		// Create Table
		dbt.mustExec("CREATE TABLE test (value BOOL)")

		// Test for unexpected data
		var out bool
		rows := dbt.mustQuery("SELECT * FROM test")
		if rows.Next() {
			dbt.Error("unexpected data in empty table")
		}

		// Create Data
		res := dbt.mustExec("INSERT INTO test VALUES (1)")
		count, err := res.RowsAffected()
		if err != nil {
			dbt.Fatalf("res.RowsAffected() returned error: %s", err.Error())
		}
		if count != 1 {
			dbt.Fatalf("Expected 1 affected row, got %d", count)
		}

		id, err := res.LastInsertId()
		if err != nil {
			dbt.Fatalf("res.LastInsertId() returned error: %s", err.Error())
		}
		if id != 0 {
			dbt.Fatalf("Expected InsertID 0, got %d", id)
		}

		// Read
		rows = dbt.mustQuery("SELECT value FROM test")
		if rows.Next() {
			rows.Scan(&out)
			if true != out {
				dbt.Errorf("true != %t", out)
			}

			if rows.Next() {
				dbt.Error("unexpected data")
			}
		} else {
			dbt.Error("no data")
		}
		rows.Close()

		// Update
		res = dbt.mustExec("UPDATE test SET value = 0 WHERE value = 1")
		count, err = res.RowsAffected()
		if err != nil {
			dbt.Fatalf("res.RowsAffected() returned error: %s", err.Error())
		}
		if count != 1 {
			dbt.Fatalf("Expected 1 affected row, got %d", count)
		}

		// Check Update
		rows = dbt.mustQuery("SELECT value FROM test")
		if rows.Next() {
			rows.Scan(&out)
			if false != out {
				dbt.Errorf("false != %t", out)
			}

			if rows.Next() {
				dbt.Error("unexpected data")
			}
		} else {
			dbt.Error("no data")
		}
		rows.Close()

		// Delete
		res = dbt.mustExec("DELETE FROM test WHERE value = 0")
		count, err = res.RowsAffected()
		if err != nil {
			dbt.Fatalf("res.RowsAffected() returned error: %s", err.Error())
		}
		if count != 1 {
			dbt.Fatalf("Expected 1 affected row, got %d", count)
		}

		// Check for unexpected rows
		res = dbt.mustExec("DELETE FROM test")
		count, err = res.RowsAffected()
		if err != nil {
			dbt.Fatalf("res.RowsAffected() returned error: %s", err.Error())
		}
		if count != 0 {
			dbt.Fatalf("Expected 0 affected row, got %d", count)
		}
	})
}
