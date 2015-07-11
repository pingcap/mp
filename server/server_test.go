package server

import (
	"database/sql"
	"testing"

	_ "github.com/go-sql-driver/mysql"
	. "gopkg.in/check.v1"
)

func TestT(t *testing.T) {
	TestingT(t)
}

var dsn = "root@tcp(localhost:4000)/test?strict=true"

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
	runTests(c, dsn, func(dbt *DBTest) {
		// Create Table
		dbt.mustExec("CREATE TABLE test (val TINYINT)")

		// Test for unexpected data
		var out bool
		rows := dbt.mustQuery("SELECT * FROM test")
		dbt.Assert(rows.Next(), Equals, false, Commentf("unexpected data in empty table"))

		// Create Data
		res := dbt.mustExec("INSERT INTO test VALUES (1)")
		//		res := dbt.mustExec("INSERT INTO test VALUES (?)", 1)
		count, err := res.RowsAffected()
		dbt.Assert(err, IsNil)
		dbt.Check(count, Equals, int64(1))
		id, err := res.LastInsertId()
		dbt.Assert(err, IsNil)
		dbt.Check(id, Equals, int64(0))

		// Read
		rows = dbt.mustQuery("SELECT val FROM test")
		if rows.Next() {
			rows.Scan(&out)
			dbt.Check(out, Equals, true)
			dbt.Check(rows.Next(), Equals, false, Commentf("unexpected data"))
		} else {
			dbt.Error("no data")
		}
		rows.Close()

		// Update
		res = dbt.mustExec(interpolateParams("UPDATE test SET val = 0 WHERE val = ?", false, 1))
		//		res = dbt.mustExec("UPDATE test SET val = 0 WHERE val = ?", 1)
		count, err = res.RowsAffected()
		dbt.Assert(err, IsNil)
		dbt.Check(count, Equals, int64(1))

		// Check Update
		rows = dbt.mustQuery("SELECT val FROM test")
		if rows.Next() {
			rows.Scan(&out)
			dbt.Check(out, Equals, false)
			dbt.Check(rows.Next(), Equals, false, Commentf("unexpected data"))
		} else {
			dbt.Error("no data")
		}
		rows.Close()

		// Delete
		res = dbt.mustExec("DELETE FROM test WHERE val = 0")
		//		res = dbt.mustExec("DELETE FROM test WHERE val = ?", 0)
		count, err = res.RowsAffected()
		dbt.Assert(err, IsNil)
		dbt.Check(count, Equals, int64(1))

		// Check for unexpected rows
		res = dbt.mustExec("DELETE FROM test")
		count, err = res.RowsAffected()
		dbt.Assert(err, IsNil)
		dbt.Check(count, Equals, int64(0))
	})
}
