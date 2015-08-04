package server

import (
	"fmt"

	"github.com/ngaut/log"
	"github.com/pingcap/tidb/types"
	"github.com/reborndb/go/errors2"
	"reflect"
)

type ComboDriver struct {
	UseTidbResult bool // if true use the result from ql, otherwise the result from mysql will be used.
}

type ResultDesc struct {
}

type Compare struct {
	sql          string
	rset         [2]*ResultSet
	status       [2]uint16
	affectedRows [2]uint64
	lastInsertID [2]uint64
	warningCount [2]uint16
	err          [2]error
}

func (d *Compare) String() string {
	s := "diff for " + d.sql + ":\n"
	if d.rset[0] == nil && d.rset[1] != nil {
		s += "expect empty result, got non-empty result.\n"
		return s
	} else if d.rset[0] != nil && d.rset[1] == nil {
		s += "expect non-empty result, got empty result.\n"
		return s
	} else if d.rset[0] != nil {
		mysqlRset := d.rset[0]
		tidbRset := d.rset[1]
		if len(mysqlRset.Columns) != len(tidbRset.Columns) {
			s += fmt.Sprintf("expect columns count %d, got %d\n", len(mysqlRset.Columns), len(tidbRset.Columns))
			return s
		}

		for i, mCol := range mysqlRset.Columns {
			tCol := tidbRset.Columns[i]
			if mCol.Type != tCol.Type {
				s += fmt.Sprintf("expect column %s type %s, got %s\n", mCol.Name, types.TypeStr(mCol.Type), types.TypeStr(tCol.Type))
				return s
			}
			if mCol.ColumnLength != tCol.ColumnLength {
				s += fmt.Sprintf("expect column %s length %d, got %d\n", mCol.Name, mCol.ColumnLength, tCol.ColumnLength)
				return s
			}
			if mCol.Flag != tCol.Flag {
				s += fmt.Sprintf("expect column %s flag %d, got %d\n", mCol.Name, mCol.Flag, tCol.Flag)
				return s
			}
			if mCol.Charset != tCol.Charset {
				s += fmt.Sprintf("expect column %s charset %d, got %d\n", mCol.Name, mCol.Charset, tCol.Charset)
				return s
			}
			if mCol.Decimal != tCol.Decimal {
				s += fmt.Sprintf("expect column %s charset %d, got %d\n", mCol.Name, mCol.Decimal, tCol.Decimal)
				return s
			}
			//TODO compare more column info
		}

		if len(mysqlRset.Rows) != len(tidbRset.Rows) {
			s += fmt.Sprintf("expect rows count %d, got %d\n", len(mysqlRset.Rows), len(tidbRset.Rows))
			return s
		}
		if !reflect.DeepEqual(d.rset[0].Rows, d.rset[1].Rows) {
			s += fmt.Sprintf("expect %v\n", d.rset[0].Rows)
			s += fmt.Sprintf("got %v\n", d.rset[1].Rows)
		}
	}
	if d.err[0] == nil && d.err[1] != nil {
		s += fmt.Sprintf("expect nil error, got %s\n", d.err[1].Error())
		return s
	} else if d.err[0] != nil && d.err[1] == nil {
		s += fmt.Sprintf("expected err %s, got nil error\n", d.err[0])
		return s
	}
	if errors2.ErrorNotEqual(d.err[0], d.err[1]) {
		s += fmt.Sprintf("expected err %s, got %s\n", d.err[0], d.err[1])
		return s
	}
	if d.rset[0] == nil && d.rset[1] == nil {
		if d.affectedRows[0] != d.affectedRows[1] {
			s += fmt.Sprintf("expect affected rows %d, got %d\n", d.affectedRows[0], d.affectedRows[1])
			return s
		}
		if d.lastInsertID[0] != d.lastInsertID[1] {
			s += fmt.Sprintf("expect last insert ID %d, got %d\n")
			return s
		}
	}
	if d.status[0] != d.status[1] {
		s += fmt.Sprintf("expect status %d, got %d\n", d.status[0], d.status[1])
		return s
	}
	if d.warningCount[0] != d.warningCount[1] {
		s += fmt.Sprintf("expect warning count %d, %d\n", d.warningCount[0], d.warningCount[1])
		return s
	}
	return "" // no diffierence return empty string
}

//Combo context will send request to both mysql and tidb, then compare the results
type ComboContext struct {
	useTidbResult bool
	mc            IContext
	tc            IContext
	stmts         map[int]IStatement
}

type ComboStatement struct {
	cc  *ComboContext
	sql string
	ms  IStatement
	ts  IStatement
}

func (cs *ComboStatement) ID() int {
	if cs.cc.useTidbResult {
		return cs.ts.ID()
	} else {
		return cs.ms.ID()
	}
}

func (cs *ComboStatement) Execute(args ...interface{}) (*ResultSet, error) {
	mrs, merr := cs.ms.Execute(args...)
	trs, terr := cs.ts.Execute(args...)
	comp := new(Compare)
	comp.sql = cs.sql
	comp.rset[0] = mrs
	comp.rset[1] = trs
	comp.affectedRows[0] = cs.cc.mc.AffectedRows()
	comp.affectedRows[1] = cs.cc.tc.AffectedRows()
	comp.lastInsertID[0] = cs.cc.mc.LastInsertID()
	comp.lastInsertID[1] = cs.cc.tc.LastInsertID()
	comp.status[0] = cs.cc.mc.Status()
	comp.status[1] = cs.cc.tc.Status()
	comp.warningCount[0] = cs.cc.mc.WarningCount()
	comp.warningCount[1] = cs.cc.tc.WarningCount()
	comp.err[0] = merr
	comp.err[1] = terr
	compStr := comp.String()
	if compStr != "" {
		log.Warning(compStr)
	}
	if cs.cc.useTidbResult {
		return trs, terr
	}
	return mrs, merr
}

func (cs *ComboStatement) AppendParam(paramId int, data []byte) error {
	err := cs.ts.AppendParam(paramId, data)
	if err != nil {
		return err
	}
	return cs.ms.AppendParam(paramId, data)
}

func (cs *ComboStatement) NumParams() int {
	if cs.cc.useTidbResult {
		return cs.ts.NumParams()
	} else {
		return cs.ms.NumParams()
	}
}

func (cs *ComboStatement) BoundParams() [][]byte {
	if cs.cc.useTidbResult {
		return cs.ts.BoundParams()
	} else {
		return cs.ms.BoundParams()
	}
}

func (cs *ComboStatement) Reset() {
	cs.ts.Reset()
	cs.ms.Reset()
}

func (cs *ComboStatement) Close() error {
	cs.ts.Close()
	cs.ms.Close()
	delete(cs.cc.stmts, cs.ID())
	return nil
}

func NewComboDriver(useTidbResult bool) *ComboDriver {
	return &ComboDriver{
		UseTidbResult: useTidbResult,
	}
}

func (cd *ComboDriver) OpenCtx(capability uint32, collation uint8, dbname string) (IContext, error) {
	md := &MysqlDriver{}
	mc, err := md.OpenCtx(capability, collation, dbname)
	if err != nil {
		return nil, err
	}
	td := &TidbDriver{}
	tc, err := td.OpenCtx(capability, collation, dbname)
	if err != nil {
		return nil, err
	}
	comCtx := &ComboContext{
		mc:            mc,
		tc:            tc,
		useTidbResult: cd.UseTidbResult,
		stmts:         make(map[int]IStatement),
	}
	return comCtx, nil
}

func (cc *ComboContext) Status() uint16 {
	if cc.useTidbResult {
		return cc.tc.Status()
	}
	return cc.mc.Status()
}

func (cc *ComboContext) LastInsertID() uint64 {
	if cc.useTidbResult {
		return cc.tc.LastInsertID()
	}
	return cc.mc.LastInsertID()
}

func (cc *ComboContext) AffectedRows() uint64 {
	if cc.useTidbResult {
		return cc.tc.AffectedRows()
	}
	return cc.mc.AffectedRows()
}

func (cc *ComboContext) CurrentDB() string {
	if cc.useTidbResult {
		return cc.tc.CurrentDB()
	}
	return cc.mc.CurrentDB()
}

func (cc *ComboContext) WarningCount() uint16 {
	if cc.useTidbResult {
		return cc.tc.WarningCount()
	}
	return cc.mc.WarningCount()
}

func (cc *ComboContext) Close() error {
	cc.mc.Close()
	cc.tc.Close()
	return nil
}

func (cc *ComboContext) Execute(sql string) (rs *ResultSet, err error) {
	mrs, merr := cc.mc.Execute(sql)
	trs, terr := cc.tc.Execute(sql)
	comp := new(Compare)
	comp.sql = sql
	comp.rset[0] = mrs
	comp.rset[1] = trs
	comp.affectedRows[0] = cc.mc.AffectedRows()
	comp.affectedRows[1] = cc.tc.AffectedRows()
	comp.lastInsertID[0] = cc.mc.LastInsertID()
	comp.lastInsertID[1] = cc.tc.LastInsertID()
	comp.status[0] = cc.mc.Status()
	comp.status[1] = cc.tc.Status()
	comp.warningCount[0] = cc.mc.WarningCount()
	comp.warningCount[1] = cc.tc.WarningCount()
	comp.err[0] = merr
	comp.err[1] = terr
	compStr := comp.String()
	if compStr != "" {
		log.Warning(compStr)
	}
	if cc.useTidbResult {
		return trs, terr
	}
	return mrs, merr
}

type PrepareCompare struct {
	sql      string
	mColumns []*ColumnInfo
	tColumns []*ColumnInfo
	mParams  []*ColumnInfo
	tParams  []*ColumnInfo
	mErr     error
	tErr     error
}

func (pc *PrepareCompare) String() string {
	s := "diff for prepare " + pc.sql + ":\n"
	if len(pc.tParams) != len(pc.mParams) {
		s += fmt.Sprintf("expect params count %d, got %d\n", len(pc.mParams), len(pc.tParams))
		return s
	}
	for i, tParam := range pc.tParams {
		mParam := pc.mParams[i]
		if tParam.Type != mParam.Type {
			s += fmt.Sprintf("expect param %d type %s, got %s\n", i, types.TypeStr(mParam.Type), types.TypeStr(tParam.Type))
			return s
		}
	}
	if pc.mErr == nil && pc.tErr != nil {
		s += fmt.Sprintf("expect nil error, got %s\n", pc.tErr.Error())
		return s
	} else if pc.mErr != nil && pc.tErr == nil {
		s += fmt.Sprintf("expected err %s, got nil error\n", pc.mErr)
		return s
	}
	if errors2.ErrorNotEqual(pc.mErr, pc.tErr) {
		s += fmt.Sprintf("expected err %s, got %s\n", pc.mErr, pc.tErr)
		return s
	}
	return ""
}

func (cc *ComboContext) Prepare(sql string) (statement IStatement, columns, params []*ColumnInfo, err error) {
	mStatement, mColumns, mParams, mErr := cc.mc.Prepare(sql)
	tStatement, tColumns, tParams, tErr := cc.tc.Prepare(sql)
	prepareCompare := &PrepareCompare{
		sql:      sql,
		mColumns: mColumns,
		tColumns: tColumns,
		mParams:  mParams,
		tParams:  tParams,
		mErr:     mErr,
		tErr:     tErr,
	}

	compStr := prepareCompare.String()
	if len(compStr) != 0 {
		log.Warning(compStr)
	}
	if mErr == nil && tErr != nil {
		// both prepare must be executed successfully, or we will get panic later.
		err = tErr
		return
	}
	comboStmt := &ComboStatement{
		cc:  cc,
		sql: sql,
		ms:  mStatement,
		ts:  tStatement,
	}
	statement = comboStmt
	if cc.useTidbResult {
		cc.stmts[tStatement.ID()] = comboStmt
		columns, params, err = tColumns, tParams, tErr
	} else {
		cc.stmts[mStatement.ID()] = comboStmt
		columns, params, err = mColumns, mParams, mErr
	}
	return
}

func (cc *ComboContext) GetStatement(stmtId int) IStatement {
	return cc.stmts[stmtId]
}

func (cc *ComboContext) FieldList(tableName, wildCard string) (columns []*ColumnInfo, err error) {
	return cc.mc.FieldList(tableName, wildCard)
}
