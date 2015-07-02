package server

import "github.com/pingcap/ql"

type QlDriver struct {
}

func (q *QlDriver) Execute(sql string, ctx ql.SessionCtx) (rs *Result, err error) {
	return
}

func (q *QlDriver) OpenCtx() (ctx ql.SessionCtx) {
	ctx, _ = ql.CreateSessionCtx()
	return ctx
}

func (q *QlDriver) CloseCtx() (err error) {
	return
}

func NewQlDriver() *QlDriver {
	return &QlDriver{}
}
