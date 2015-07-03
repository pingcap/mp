package server

import (
	"errors"
	. "github.com/pingcap/mp/protocol"
	"strconv"
	"strings"
)

type MockCtx struct {
	status       uint16
	lastInsertId uint64
	affectedRows uint64
}

func (mCtx *MockCtx) Status() uint16 {
	return mCtx.status
}

func (mCtx *MockCtx) LastInsertId() uint64 {
	return mCtx.lastInsertId
}

func (mCtx *MockCtx) AffectedRows() uint64 {
	return mCtx.affectedRows
}

func (mCtx *MockCtx) CurrentDatabase() string {
	return "test"
}

type MockDriver struct {
	columnMap map[string]*ColumnInfo
	exeIdx    int
	inputs    []string
	outputs   []interface{}
	ctxes     []*MockCtx
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
func (mql *MockDriver) BuildResult(columns ...string) *Result {
	res := new(Result)
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

//Add predefined query and result for testing.
//Result type can be *Result for result set response, error for error response, nil for ok response.
func (mql *MockDriver) AddQuery(sql string, result interface{}, status uint16, lastInsertId, affectedRows uint64) {
	mql.inputs = append(mql.inputs, sql)
	mql.outputs = append(mql.outputs, result)
	mql.ctxes = append(mql.ctxes, &MockCtx{status, lastInsertId, affectedRows})
}

func (mql *MockDriver) OpenCtx() Context {
	return &MockCtx{SERVER_STATUS_AUTOCOMMIT, 0, 0}
}

func (mql *MockDriver) Execute(sql string, ctx Context) (rs *Result, err error) {
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
	case *Result:
		rs = op.(*Result)
	case error:
		err = op.(error)
	}
	mctx := ctx.(*MockCtx)
	*mctx = *(mql.ctxes[mql.exeIdx])
	mql.exeIdx++
	return
}

func (mql *MockDriver) CloseCtx(ctx Context) error {
	return nil
}

func (mql *MockDriver) FieldList(tableName string, ctx Context) (columns []*ColumnInfo) {
	prefix := ctx.CurrentDatabase() + "." + tableName + "."
	for k, v := range mql.columnMap {
		if strings.HasPrefix(k, prefix) {
			columns = append(columns, v)
		}
	}
	return
}
