package server

import (
	"encoding/json"
)

type IDriver interface {
	OpenCtx() (Context, error)
}

type Context interface {
	Status() uint16
	LastInsertID() uint64
	AffectedRows() uint64
	CurrentDB() string
	Execute(sql string, args ...interface{}) (*ResultSet, error)
	Prepare(sql string) (statement Statement, err error)
	GetStatement(stmtId int) Statement
	FieldList(tableName, wildCard string) (columns []*ColumnInfo, err error)
	Close() error
}

type Statement interface {
	ID() int
	Execute(args ...interface{}) (*ResultSet, error)
	Columns() []*ColumnInfo
	Params() []*ColumnInfo
	Close() error
}

type ResultSet struct {
	Columns []*ColumnInfo
	Rows    [][]interface{}
}

func (res *ResultSet) String() string {
	b, _ := json.MarshalIndent(res, "", "\t")
	return string(b)
}

func (res *ResultSet) AddRow(values ...interface{}) *ResultSet {
	res.Rows = append(res.Rows, values)
	return res
}
