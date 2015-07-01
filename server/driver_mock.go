package server
import (
	"errors"
	"github.com/pingcap/mp/protocol"
	"github.com/pingcap/ql"
)

type MockCtx struct {
	status uint16
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

type MockDriver struct {
	columnMap map[string]*ColumnInfo
	exeIdx int
	inputs []string
	outputs []*Result
	ctxes []*MockCtx
}

func NewMockDriver() *MockDriver {
	return &MockDriver{
		columnMap: make(map[string]*ColumnInfo),
	}
}

func (mql *MockDriver) InitColumns(table, column string, typ uint8) {
	mql.columnMap[table+"."+column] = &ColumnInfo{
		Schema: "test",
		Table: table,
		OrgTable: table,
		ColumnLength: 255,
		Charset: uint16(protocol.DEFAULT_COLLATION_ID),
		Flag: 0,
		Decimal: 31,
		Type: typ,
	}
}

func (mql *MockDriver) BuildResult(columns []string, rows [][]interface{}) *Result {
	return nil
}

func (mql *MockDriver) AddQuery(sql string, result *Result, status uint16, lastInsertId, affectedRows uint64) {
	mql.inputs = append(mql.inputs, sql)
	mql.outputs = append(mql.outputs, result)
	mql.ctxes = append(mql.ctxes, &MockCtx{status, lastInsertId, affectedRows})
}

func (mql *MockDriver) GetCtx() ql.SessionCtx {
	return &MockCtx{protocol.SERVER_STATUS_AUTOCOMMIT, 0, 0}
}

func (mql *MockDriver) Execute(sql string, ctx ql.SessionCtx) (rs *Result, err error) {
	if mql.exeIdx == len(mql.inputs) {
		err = errors.New("[mock]no more query to execute..")
		return
	}
	if sql != mql.inputs[mql.exeIdx] {
		err = errors.New("[mock]unexpected sql input")
		return
	}
	rs = mql.outputs[mql.exeIdx]
	mctx := ctx.(*MockCtx)
	*mctx = *(mql.ctxes[mql.exeIdx])
	mql.exeIdx++
	return
}

func (mql *MockDriver) CloseCtx(ctx ql.SessionCtx) error {
	return nil
}