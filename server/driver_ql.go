package server

import (
	"github.com/ngaut/log"
	. "github.com/pingcap/mysqldef"
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
	return &QlContext{session, dbname, 0}, nil
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
	qrsList, err := qc.session.Execute(interpolateParams(sql, qc.session.Status()&ServerStatusNoBackslashEscaped > 0, args...))
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

func convertColumnInfo(qlfield *field.ResultField) (ci *ColumnInfo) {
	ci = new(ColumnInfo)
	ci.Schema = ""
	ci.Flag = uint16(qlfield.Flag)
	ci.Name = qlfield.Name
	ci.Table = qlfield.TableName
	ci.Charset = uint16(CharsetIds[qlfield.Charset])
	ci.ColumnLength = uint32(qlfield.Flen)
	ci.Type = uint8(qlfield.Tp)
	return
}

func CreateQlTestDatabase() {
	qd := &QlDriver{}
	qc, err := qd.OpenCtx(DefaultCapability, DefaultCollationID, "")
	if err != nil {
		log.Fatal(err)
	}
	qc.Execute("CREATE DATABASE IF NOT EXISTS test")
	qc.Execute("CREATE DATABASE IF NOT EXISTS gotest")
	qc.Close()
}
