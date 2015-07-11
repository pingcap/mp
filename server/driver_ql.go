package server

import (
	. "github.com/pingcap/mp/protocol"
	"github.com/pingcap/ql"
	"github.com/pingcap/ql/field"
)

type QlDriver struct{}

type QlContext struct {
	session      ql.Session
	currentDB    string
	warningCount uint16
}

type QlStatment struct {
}

func (qd *QlDriver) OpenCtx(capability uint32, collation uint8, dbname string) (IContext, error) {
	session, _ := ql.CreateSession()
	if dbname != "" {
		_, err := session.Execute("use " + dbname)
		if err != nil {
			return nil, err
		}
	}
	return &QlContext{session, "", 0}, nil
}

func (qc *QlContext) Status() uint16 {
	return qc.session.Status()
}

func (qc *QlContext) LastInsertID() uint64 {
	return qc.session.LastInsertID()
}

func (qc *QlContext) AffectedRows() uint64 {
	return qc.session.AffectedRows()
}

func (qc *QlContext) CurrentDB() string {
	return qc.currentDB
}

func (qc *QlContext) WarningCount() uint16 {
	return qc.warningCount
}

func (qc *QlContext) Execute(sql string, args ...interface{}) (rs *ResultSet, err error) {
	qrsList, err := qc.session.Execute(sql, args...)
	if err != nil {
		return
	}
	if len(qrsList) == 0 { // result ok
		return
	}
	qrs := qrsList[0]
	rs = new(ResultSet)
	fields, err := qrs.Fields()
	if err != nil {
		return
	}
	for _, v := range fields {
		rs.Columns = append(rs.Columns, convertColumnInfo(v))
	}
	rs.Rows, err = qrs.Rows(-1, 0)
	if err != nil {
		return
	}
	return
}

func (qc *QlContext) Close() (err error) {
	//TODO
	return
}

func (qc *QlContext) FieldList(table, wildCard string) (colums []*ColumnInfo, err error) {
	rs, err := qc.Execute("SELECT * FROM " + table + " LIMIT 0")
	if err != nil {
		return
	}
	colums = rs.Columns
	return
}

func (qc *QlContext) GetStatement(stmtId int) IStatement {
	//TODO
	return nil
}

func (qc *QlContext) Prepare(sql string) (statement IStatement, columns, params []*ColumnInfo, err error) {
	//TODO
	return
}

var qlTypeMap = map[string]byte{
	"bigint":   MYSQL_TYPE_LONGLONG,
	"blob":     MYSQL_TYPE_BLOB,
	"bool":     MYSQL_TYPE_TINY,
	"duration": MYSQL_TYPE_LONGLONG, //TODO change to proper type
	"float32":  MYSQL_TYPE_FLOAT,
	"float64":  MYSQL_TYPE_DOUBLE,
	"int16":    MYSQL_TYPE_SHORT,
	"int32":    MYSQL_TYPE_LONG,
	"int64":    MYSQL_TYPE_LONGLONG,
	"int8":     MYSQL_TYPE_TINY,
	"string":   MYSQL_TYPE_VARCHAR,
	"time":     MYSQL_TYPE_DATETIME,
	"uint16":   MYSQL_TYPE_SHORT,
	"uint32":   MYSQL_TYPE_LONG,
	"uint64":   MYSQL_TYPE_LONGLONG,
	"uint8":    MYSQL_TYPE_TINY,
}

func convertColumnInfo(qlfield *field.ResultField) (ci *ColumnInfo) {
	ci = new(ColumnInfo)
	ci.Schema = qlfield.DBName
	ci.Flag = uint16(qlfield.Flag)
	ci.Name = qlfield.Name
	ci.Table = qlfield.TableName
	ci.Charset = uint16(CharsetIds[qlfield.Charset])
	ci.ColumnLength = uint32(qlfield.Flen)
	ci.Type = qlTypeMap[qlfield.TypeStr]
	return
}
