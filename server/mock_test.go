package server

import (
	"errors"
	"strconv"
	"strings"
	"time"

	_ "github.com/go-sql-driver/mysql"
	"github.com/ngaut/log"
	"github.com/pingcap/mp/etc"
	. "github.com/pingcap/mp/protocol"
	. "gopkg.in/check.v1"
)

type MockDriver struct {
	columnMap map[string]*ColumnInfo
	ctx       *MockCtx
}

func NewMockDriver() *MockDriver {
	return &MockDriver{
		columnMap: make(map[string]*ColumnInfo),
	}
}

func (mql *MockDriver) InitColumns(columnsDefs ...string) {
	for _, v := range columnsDefs {
		ci := ParseColumn(v)
		mql.columnMap[fullName(ci)] = ci
	}
}

func fullName(ci *ColumnInfo) string {
	return ci.Schema + "." + ci.Table + "." + ci.Name
}

func parseType(typeStr string) uint8 {
	switch typeStr {
	case "decimal":
		return MYSQL_TYPE_DECIMAL
	case "tiny":
		return MYSQL_TYPE_TINY
	case "short":
		return MYSQL_TYPE_SHORT
	case "long":
		return MYSQL_TYPE_LONG
	case "float":
		return MYSQL_TYPE_FLOAT
	case "double":
		return MYSQL_TYPE_DOUBLE
	case "null":
		return MYSQL_TYPE_NULL
	case "timestamp":
		return MYSQL_TYPE_TIMESTAMP
	case "longlong":
		return MYSQL_TYPE_LONGLONG
	case "int24":
		return MYSQL_TYPE_INT24
	case "date":
		return MYSQL_TYPE_DATE
	case "time":
		return MYSQL_TYPE_TIME
	case "datetime":
		return MYSQL_TYPE_DATETIME
	case "year":
		return MYSQL_TYPE_YEAR
	case "newdate":
		return MYSQL_TYPE_NEWDATE
	case "varchar":
		return MYSQL_TYPE_VARCHAR
	case "bit":
		return MYSQL_TYPE_BIT
	case "newdecimal":
		return MYSQL_TYPE_NEWDECIMAL
	case "enum":
		return MYSQL_TYPE_ENUM
	case "set":
		return MYSQL_TYPE_SET
	case "tiny_blob":
		return MYSQL_TYPE_TINY_BLOB
	case "medium_blob":
		return MYSQL_TYPE_MEDIUM_BLOB
	case "long_blob":
		return MYSQL_TYPE_LONG_BLOB
	case "blob":
		return MYSQL_TYPE_BLOB
	case "var_string":
		return MYSQL_TYPE_VAR_STRING
	case "string":
		return MYSQL_TYPE_STRING
	case "geomery":
		return MYSQL_TYPE_GEOMETRY
	}
	return MYSQL_TYPE_NULL
}

//The column defination is written as "schema.table.column|varchar.255|pk.nn.uq.un.zf.ai.bin" for short.
func ParseColumn(columnDef string) *ColumnInfo {
	ci := new(ColumnInfo)
	parts := strings.Split(columnDef, "|")
	fullname := parts[0]
	nameParts := strings.Split(fullname, ".")
	ci.Schema = nameParts[0]
	ci.Table = nameParts[1]
	//	ci.OrgTable = ci.Table
	ci.Name = nameParts[2]
	//	ci.OrgName = ci.Name
	typeStr := parts[1]
	typeParts := strings.Split(typeStr, ".")
	ci.Type = parseType(typeParts[0])
	clen, _ := strconv.Atoi(typeParts[1])
	ci.ColumnLength = uint32(clen)
	switch ci.Type {
	case MYSQL_TYPE_STRING, MYSQL_TYPE_NULL, MYSQL_TYPE_TINY, MYSQL_TYPE_SHORT, MYSQL_TYPE_INT24, MYSQL_TYPE_LONG, MYSQL_TYPE_LONGLONG:
		ci.Decimal = 0
	default:
		ci.Decimal = 0x1f
	}
	flagStr := parts[2]
	flagParts := strings.Split(flagStr, ".")
	for _, v := range flagParts {
		switch v {
		case "pk":
			ci.Flag |= PRI_KEY_FLAG
		case "nn":
			ci.Flag |= NOT_NULL_FLAG
		case "uq":
			ci.Flag |= UNIQUE_KEY_FLAG
		case "un":
			ci.Flag |= UNSIGNED_FLAG
		case "zf":
			ci.Flag |= ZEROFILL_FLAG
		case "ai":
			ci.Flag |= AUTO_INCREMENT_FLAG
		case "bin":
			ci.Flag |= BINARY_FLAG
		}
	}
	ci.Charset = uint16(DEFAULT_COLLATION_ID)
	return ci
}

// Create result with columns info.
// Column name is defined as "schema.table.column"
func (mql *MockDriver) BuildResult(columns ...string) *ResultSet {
	res := new(ResultSet)
	for _, col := range columns {
		column := mql.columnMap[col]
		if column == nil {
			column = ParseColumn(col + "|varchar.255|")
			mql.columnMap[col] = column
		}
		res.Columns = append(res.Columns, mql.columnMap[col])
	}
	return res
}

func (mql *MockDriver) OpenCtx() (ctx Context, err error) {
	if mql.ctx == nil {
		mql.ctx = &MockCtx{
			columnMap: mql.columnMap,
		}
		mql.ctx.AddQuery("", nil, 0, 0, 0)
	}
	ctx = mql.ctx
	return
}

type MockCtx struct {
	columnMap    map[string]*ColumnInfo
	inputs       []string
	outputs      []interface{}
	status       []uint16
	lastInsertID []uint64
	affectedRows []uint64
	exeIdx       int
}

func (mCtx *MockCtx) Status() uint16 {
	return mCtx.status[mCtx.exeIdx]
}

func (mCtx *MockCtx) LastInsertID() uint64 {
	return mCtx.lastInsertID[mCtx.exeIdx]
}

func (mCtx *MockCtx) AffectedRows() uint64 {
	return mCtx.affectedRows[mCtx.exeIdx]
}

func (mCtx *MockCtx) CurrentDB() string {
	return "test"
}

func (mCtx *MockCtx) Close() (err error) {
	return
}

func (mCtx *MockCtx) Prepare(sql string) (stmt Statement, err error) {
	return
}

//Add predefined query and result for testing.
//Result type can be *Result for result set response, error for error response, nil for ok response.
func (mql *MockCtx) AddQuery(sql string, result interface{}, status uint16, lastInsertId, affectedRows uint64) {
	mql.inputs = append(mql.inputs, sql)
	mql.outputs = append(mql.outputs, result)
	mql.status = append(mql.status, status)
	mql.lastInsertID = append(mql.lastInsertID, lastInsertId)
	mql.affectedRows = append(mql.affectedRows, affectedRows)
}

func (mql *MockCtx) Execute(sql string, args ...interface{}) (rs *ResultSet, err error) {
	mql.exeIdx++
	if mql.exeIdx == len(mql.inputs) {
		err = errors.New("[mock]no more results to execute:" + sql)
		return
	}
	if sql != mql.inputs[mql.exeIdx] {
		err = errors.New("[mock]unexpected sql input:" + sql)
		return
	}
	op := mql.outputs[mql.exeIdx]
	switch op.(type) {
	case *ResultSet:
		rs = op.(*ResultSet)
	case error:
		err = op.(error)
	}
	return
}

func (mql *MockCtx) FieldList(tableName string, wildCard string) (columns []*ColumnInfo, err error) {
	prefix := mql.CurrentDB() + "." + tableName + "."
	for k, v := range mql.columnMap {
		if strings.HasPrefix(k, prefix) {
			columns = append(columns, v)
		}
	}
	return
}

func (mql *MockCtx) GetStatement(stmtId int) Statement {
	return nil
}

type mockTestSuite struct {
	mockDrv *MockDriver
	server  *Server
}

//var _ = Suite(&mockTestSuite{})

func (ts *mockTestSuite) SetUpSuite(c *C) {
	ts.mockDrv = NewMockDriver()
	cfg := &etc.Config{
		Addr:     ":4000",
		User:     "root",
		Password: "",
		LogLevel: "debug",
	}
	server, err := NewServer(cfg, ts.mockDrv)
	if err != nil {
		log.Fatal(err)
	}
	ts.server = server
	go ts.server.Run()
	time.Sleep(time.Millisecond * 100)
}

func (ts *mockTestSuite) TearDownSuite(c *C) {
	ts.server.Close()
}

func (ts *mockTestSuite) TestT(c *C) {
	ts.mockDrv.InitColumns("test.test.val|tiny.1|")
	status := SERVER_STATUS_AUTOCOMMIT
	ctx, _ := ts.mockDrv.OpenCtx()

	mockCtx := ctx.(*MockCtx)

	mockCtx.AddQuery("use test", nil, status, 0, 0)
	result := ts.mockDrv.BuildResult("..@@max_allowed_packet")
	result.AddRow(16777216)
	mockCtx.AddQuery("SELECT @@max_allowed_packet", result, status, 0, 0)
	mockCtx.AddQuery("DROP TABLE IF EXISTS test", nil, status, 0, 0)
	mockCtx.AddQuery("CREATE TABLE test (val TINYINT)", nil, status, 0, 0)
	mockCtx.AddQuery("SELECT * FROM test", ts.mockDrv.BuildResult("test.test.val"), status, 0, 0)
	mockCtx.AddQuery("INSERT INTO test VALUES (1)", nil, status, 0, 1)
	result = ts.mockDrv.BuildResult("test.test.val")
	result.AddRow(1)
	mockCtx.AddQuery("SELECT val FROM test", result, status, 0, 0)
	mockCtx.AddQuery("UPDATE test SET val = 0 WHERE val = 1", nil, status, 0, 1)
	result = ts.mockDrv.BuildResult("test.test.val")
	result.AddRow(0)
	mockCtx.AddQuery("SELECT val FROM test", result, status, 0, 0)
	mockCtx.AddQuery("DELETE FROM test WHERE val = 0", nil, status, 0, 1)
	mockCtx.AddQuery("DELETE FROM test", nil, status, 0, 0)
	mockCtx.AddQuery("DROP TABLE IF EXISTS test", nil, status, 0, 0)
	runTestCRUD(c)
}
