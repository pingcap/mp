package server

import (
	"encoding/json"
)

type IDriver interface {
	OpenCtx(capability uint32, collation uint8, dbname string) (IContext, error)
}

type IContext interface {
	Status() uint16
	LastInsertID() uint64
	AffectedRows() uint64
	WarningCount() uint16
	CurrentDB() string
	Execute(sql string) (*ResultSet, error)
	Prepare(sql string) (statement IStatement, columns, params []*ColumnInfo, err error)
	GetStatement(stmtId int) IStatement
	FieldList(tableName, wildCard string) (columns []*ColumnInfo, err error)
	Close() error
}

type IStatement interface {
	ID() int
	Execute(args ...interface{}) (*ResultSet, error)
	AppendParam(paramId int, data []byte) error
	NumParams() int
	BoundParams() [][]byte
	Reset()
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
