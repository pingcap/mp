package server

import "github.com/pingcap/ql"

type QlDriver struct {
}

type QlContext struct {
	sessionCtx      ql.SessionCtx
	currentDatabase string
}

func (qc *QlContext) Status() uint16 {
	return qc.sessionCtx.Status()
}

func (qc *QlContext) LastInsertId() uint64 {
	return qc.sessionCtx.LastInsertId()
}

func (qc *QlContext) AffectedRows() uint64 {
	return qc.sessionCtx.AffectedRows()
}

func (qc *QlContext) CurrentDatabase() string {
	return qc.currentDatabase
}

func (q *QlDriver) Execute(sql string, ctx Context) (rs *Result, err error) {
	return
}

func (q *QlDriver) OpenCtx() Context {
	qctx, _ := ql.CreateSessionCtx()
	return &QlContext{qctx, ""}
}

func (q *QlDriver) CloseCtx() (err error) {
	return
}

func (q *QlDriver) FieldList(tableName string, ctx Context) (columns []*ColumnInfo) {
	return
}

func NewQlDriver() *QlDriver {
	return &QlDriver{}
}
