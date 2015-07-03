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

func (qc *QlContext) LastInsertID() uint64 {
	return qc.sessionCtx.LastInsertID()
}

func (qc *QlContext) AffectedRows() uint64 {
	return qc.sessionCtx.AffectedRows()
}

func (qc *QlContext) CurrentDatabase() string {
	return qc.currentDatabase
}

func (q *QlDriver) Execute(sql string, ctx Context) (rs *ResultSet, err error) {
	return
}

func (q *QlDriver) OpenCtx() Context {
	qctx, _ := ql.CreateSessionCtx()
	return &QlContext{qctx, ""}
}

func (q *QlDriver) CloseCtx(ctx Context) (err error) {
	qc := ctx.(ql.SessionCtx)
	_ = qc
	return
}

func converColumnInfo(table string, qci *ql.ColumnInfo) (ci *ColumnInfo) {
	return
}

func (q *QlDriver) FieldList(tableName string, ctx Context) (columns []*ColumnInfo) {
	return
}

func NewQlDriver() *QlDriver {
	return &QlDriver{}
}
