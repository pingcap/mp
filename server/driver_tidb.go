package server

import (
	"github.com/ngaut/log"
	. "github.com/pingcap/mysqldef"
	"github.com/pingcap/tidb"
	"github.com/pingcap/tidb/field"
	"github.com/pingcap/tidb/types"
)

type TidbDriver struct{}

type TidbContext struct {
	session      tidb.Session
	currentDB    string
	warningCount uint16
	stmts        map[int]*TidbStatement
}

type TidbStatement struct {
	id          uint32
	numParams   int
	boundParams [][]byte
	ctx         *TidbContext
}

func (ts *TidbStatement) ID() int {
	return int(ts.id)
}

func (ts *TidbStatement) Execute(args ...interface{}) (rs *ResultSet, err error) {
	//TODO: temporary solution for passing test, will change later.
	for i := range args {
		switch v := args[i].(type) {
		case int64:
			args[i] = types.IdealInt(v)
		case uint64:
			args[i] = types.IdealUint(v)
		case float64:
			args[i] = types.IdealFloat(v)
		}
	}
	tidbRecordset, err := ts.ctx.session.ExecutePreparedStmt(ts.id, args...)
	if err != nil {
		return nil, err
	}
	if tidbRecordset == nil {
		return
	}
	rs = new(ResultSet)
	fields, err := tidbRecordset.Fields()
	if err != nil {
		return
	}
	for _, v := range fields {
		rs.Columns = append(rs.Columns, convertColumnInfo(v))
	}
	rs.Rows, err = tidbRecordset.Rows(-1, 0)
	if err != nil {
		return
	}
	return
}

func (ts *TidbStatement) AppendParam(paramID int, data []byte) error {
	if paramID >= len(ts.boundParams) {
		return NewDefaultError(ER_WRONG_ARGUMENTS, "stmt_send_longdata")
	}
	ts.boundParams[paramID] = append(ts.boundParams[paramID], data...)
	return nil
}

func (ts *TidbStatement) NumParams() int {
	return ts.numParams
}

func (ts *TidbStatement) BoundParams() [][]byte {
	return ts.boundParams
}

func (ts *TidbStatement) Reset() {
	for i := range ts.boundParams {
		ts.boundParams[i] = nil
	}
}

func (ms *TidbStatement) Close() error {
	//TODO close at tidb level
	err := ms.ctx.session.DropPreparedStmt(ms.id)
	if err != nil {
		return err
	}
	delete(ms.ctx.stmts, int(ms.id))
	return nil
}

func (qd *TidbDriver) OpenCtx(capability uint32, collation uint8, dbname string) (IContext, error) {
	session, _ := tidb.CreateSession()
	if dbname != "" {
		_, err := session.Execute("use " + dbname)
		if err != nil {
			return nil, err
		}
	}
	tc := &TidbContext{
		session:   session,
		currentDB: dbname,
		stmts:     make(map[int]*TidbStatement),
	}
	return tc, nil
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

func (tc *TidbContext) Execute(sql string) (rs *ResultSet, err error) {
	qrsList, err := tc.session.Execute(sql)
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
	//return tc.session.Close()
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
	tcStmt := tc.stmts[stmtId]
	if tcStmt != nil {
		return tcStmt
	}
	return nil
}

func (tc *TidbContext) Prepare(sql string) (statement IStatement, columns, params []*ColumnInfo, err error) {
	stmtId, paramCount, fields, err := tc.session.PrepareStmt(sql)
	if err != nil {
		return
	}
	stmt := &TidbStatement{
		id:          stmtId,
		numParams:   paramCount,
		boundParams: make([][]byte, paramCount),
		ctx:         tc,
	}
	statement = stmt
	columns = make([]*ColumnInfo, len(fields))
	for i := range fields {
		columns[i] = convertColumnInfo(fields[i])
	}
	params = make([]*ColumnInfo, paramCount)
	for i := range params {
		params[i] = &ColumnInfo{
			Type: TypeBlob,
		}
	}
	tc.stmts[int(stmtId)] = stmt
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

func CreateTidbTestDatabase() {
	td := &TidbDriver{}
	tc, err := td.OpenCtx(DefaultCapability, DefaultCollationID, "")
	if err != nil {
		log.Fatal(err)
	}
	tc.Execute("CREATE DATABASE IF NOT EXISTS test")
	tc.Execute("CREATE DATABASE IF NOT EXISTS gotest")
	tc.Close()
}
