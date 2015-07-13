package server

import (
	"fmt"

	. "github.com/pingcap/mp/protocol"
	"github.com/pingcap/ql"
	"github.com/reborndb/go/errors2"
	"github.com/ngaut/log"
)

type ComboDriver struct {
	UseQlResult bool // if true use the result from ql, otherwise the result from mysql will be used.
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
		qlRset := d.rset[1]
		if len(mysqlRset.Columns) != len(qlRset.Columns) {
			s += fmt.Sprintf("expect columns count %d, got %d\n", len(mysqlRset.Columns), len(qlRset.Columns))
			return s
		}
		//TODO compare column defination
		if len(mysqlRset.Rows) != len(qlRset.Rows) {
			s += fmt.Sprintf("expect rows count %d, got %d\n", len(mysqlRset.Rows), len(qlRset.Rows))
			return s
		}
		//TODO compare row values.
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

//Combo context will send request to both mysql and ql, then compare the results
type ComboContext struct {
	useQlResult bool
	mc          *MysqlConn
	qc          *QlContext
}

func NewComboDriver(useQlResult bool) *ComboDriver {
	return &ComboDriver{
		UseQlResult: useQlResult,
	}
}

func (cd *ComboDriver) OpenCtx(capability uint32, collation uint8, dbname string) (IContext, error) {
	mc := new(MysqlConn)
	mc.stmts = make(map[int]*MysqlStatement)
	mc.capability = capability & DEFAULT_CAPABILITY
	mc.collation = CollationId(collation)
	err := mc.Connect(":3306", "root", "", dbname)
	if err != nil {
		return nil, err
	}

	session, _ := ql.CreateSession()
	_, err = session.Execute("CREATE DATABASE IF NOT EXISTS test")
//	if err != nil {
//		return nil, err
//	}
	if dbname != "" {
		_, err = session.Execute("use " + dbname)
		if err != nil {
			return nil, err
		}
	}
	qc := &QlContext{
		session: session,
	}

	comCtx := new(ComboContext)
	comCtx.mc = mc
	comCtx.qc = qc
	comCtx.useQlResult = cd.UseQlResult
	return comCtx, nil
}

func (cc *ComboContext) Status() uint16 {
	if cc.useQlResult {
		return cc.qc.Status()
	}
	return cc.mc.Status()
}

func (cc *ComboContext) LastInsertID() uint64 {
	if cc.useQlResult {
		return cc.qc.LastInsertID()
	}
	return cc.mc.LastInsertID()
}

func (cc *ComboContext) AffectedRows() uint64 {
	if cc.useQlResult {
		return cc.qc.AffectedRows()
	}
	return cc.mc.AffectedRows()
}

func (cc *ComboContext) CurrentDB() string {
	if cc.useQlResult {
		return cc.qc.CurrentDB()
	}
	return cc.mc.CurrentDB()
}

func (cc *ComboContext) WarningCount() uint16 {
	if cc.useQlResult {
		return cc.qc.WarningCount()
	}
	return cc.mc.WarningCount()
}

func (cc *ComboContext) Close() error {
	cc.mc.Close()
	cc.qc.Close()
	return nil
}

func (cc *ComboContext) Execute(sql string, args ...interface{}) (rs *ResultSet, err error) {
	mrs, merr := cc.mc.Execute(sql, args...)
	qrs, qerr := cc.qc.Execute(sql, args...)
	comp := new(Compare)
	comp.sql = sql
	comp.rset[0] = mrs
	comp.rset[1] = qrs
	comp.affectedRows[0] = cc.mc.affectedRows
	comp.affectedRows[1] = cc.qc.AffectedRows()
	comp.lastInsertID[0] = cc.mc.lastInsertID
	comp.lastInsertID[1] = cc.qc.LastInsertID()
	comp.status[0] = cc.mc.status
	comp.status[1] = cc.qc.Status()
	comp.warningCount[0] = cc.mc.warningCount
	comp.warningCount[1] = cc.qc.warningCount
	comp.err[0] = merr
	comp.err[1] = qerr
	compStr := comp.String()
	if compStr != "" {
		log.Warning(compStr)
	}
	if cc.useQlResult {
		return qrs, qerr
	}
	return mrs, merr
}

func (cc *ComboContext) Prepare(sql string) (statement IStatement, columns, params []*ColumnInfo, err error) {
	return cc.mc.Prepare(sql)
}

func (cc *ComboContext) GetStatement(stmtId int) IStatement {
	return cc.mc.GetStatement(stmtId)
}

func (cc *ComboContext) FieldList(tableName, wildCard string) (columns []*ColumnInfo, err error) {
	return cc.mc.FieldList(tableName, wildCard)
}
