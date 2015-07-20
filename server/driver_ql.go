package server

import (
	"github.com/ngaut/log"
	. "github.com/pingcap/mysqldef"
	"github.com/pingcap/tidb"
	"github.com/pingcap/tidb/field"
)

type TidbDriver struct{}

type TidbContext struct {
	session      tidb.Session
	currentDB    string
	warningCount uint16
}

type TidbStatement struct {
}

func (qd *TidbDriver) OpenCtx(capability uint32, collation uint8, dbname string) (IContext, error) {
	session, _ := tidb.CreateSession()
	if dbname != "" {
		_, err := session.Execute("use " + dbname)
		if err != nil {
			return nil, err
		}
	}
	return &TidbContext{session, dbname, 0}, nil
}

func (tc *TidbContext) Status() uint16 {
	return tc.session.Status()
}

func (tc *TidbContext) LastInsertID() uint64 {
	return tc.session.LastInsertID()
}

func (tc *TidbContext) AffectedRows() uint64 {
	return tc.session.AffectedRows()
}

func (tc *TidbContext) CurrentDB() string {
	return tc.currentDB
}

func (tc *TidbContext) WarningCount() uint16 {
	return tc.warningCount
}

func (tc *TidbContext) Execute(sql string, args ...interface{}) (rs *ResultSet, err error) {
	qrsList, err := tc.session.Execute(interpolateParams(sql, tc.session.Status()&ServerStatusNoBackslashEscaped > 0, args...))
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

func (tc *TidbContext) Close() (err error) {
	//TODO
	return
}

func (tc *TidbContext) FieldList(table, wildCard string) (colums []*ColumnInfo, err error) {
	rs, err := tc.Execute("SELECT * FROM " + table + " LIMIT 0")
	if err != nil {
		return
	}
	colums = rs.Columns
	return
}

func (tc *TidbContext) GetStatement(stmtId int) IStatement {
	//TODO
	return nil
}

func (tc *TidbContext) Prepare(sql string) (statement IStatement, columns, params []*ColumnInfo, err error) {
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
	td := &TidbDriver{}
	tc, err := td.OpenCtx(DefaultCapability, DefaultCollationID, "")
	if err != nil {
		log.Fatal(err)
	}
	tc.Execute("CREATE DATABASE IF NOT EXISTS test")
	tc.Execute("CREATE DATABASE IF NOT EXISTS gotest")
	tc.Close()
}
