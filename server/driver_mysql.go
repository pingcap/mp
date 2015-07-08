package server

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"net"
	"strings"
	"time"

	. "github.com/pingcap/mp/protocol"
)

type MysqlDriver struct{}

type MysqlStatement struct {
	mConn   *MysqlConn
	id      int
	sql     string
	columns []*ColumnInfo
	params  []*ColumnInfo
}

type MysqlConn struct {
	conn net.Conn

	pkg *PacketIO

	addr     string
	user     string
	password string
	db       string

	capability uint32

	status       uint16
	lastInsertID uint64
	affectedRows uint64

	collation CollationId
	charset   string
	salt      []byte

	lastPing int64

	pkgErr error

	stmts map[int]*MysqlStatement //statement id : parameters column info
}

func (md *MysqlDriver) OpenCtx() (ctx Context, err error) {
	mc := new(MysqlConn)
	mc.stmts = make(map[int]*MysqlStatement)
	err = mc.Connect(":3306", "root", "", "test")
	if err != nil {
		return nil, err
	}
	ctx = mc
	return
}

func (ms *MysqlStatement) Execute(args ...interface{}) (rs *ResultSet, err error) {
	return ms.mConn.Execute(ms.sql, args...)
}

func (ms *MysqlStatement) Columns() []*ColumnInfo {
	return ms.columns
}

func (ms *MysqlStatement) Params() []*ColumnInfo {
	return ms.params
}

func (ms *MysqlStatement) ID() int {
	return ms.id
}

func (ms *MysqlStatement) Close() error {
	err := ms.mConn.writeCommandBuf(byte(COM_STMT_CLOSE), Uint32ToBytes(uint32(ms.id)))
	if err != nil {
		return err
	}
	err = ms.mConn.readOK()
	if err != nil {
		return err
	}
	delete(ms.mConn.stmts, ms.id)
	return nil
}

func (mc *MysqlConn) Status() uint16 {
	return mc.status
}

func (mc *MysqlConn) LastInsertID() uint64 {
	return mc.lastInsertID
}

func (mc *MysqlConn) AffectedRows() uint64 {
	return mc.affectedRows
}

func (mc *MysqlConn) CurrentDB() string {
	return mc.db
}

func (mc *MysqlConn) Connect(addr string, user string, password string, db string) error {
	mc.addr = addr
	mc.user = user
	mc.password = password
	mc.db = db

	//use utf8
	mc.collation = DEFAULT_COLLATION_ID
	mc.charset = DEFAULT_CHARSET

	return mc.ReConnect()
}

func (mc *MysqlConn) ReConnect() error {
	if mc.conn != nil {
		mc.conn.Close()
	}

	netConn, err := net.Dial("tcp", mc.addr)
	if err != nil {
		return err
	}

	mc.conn = netConn
	mc.pkg = NewPacketIO(netConn)

	if err := mc.readInitialHandshake(); err != nil {
		mc.conn.Close()
		return err
	}
	if err := mc.writeAuthHandshake(); err != nil {
		mc.conn.Close()

		return err
	}
	if err = mc.readOK(); err != nil {
		mc.conn.Close()
		return err
	}
	//we must always use autocommit
	if !mc.IsAutoCommit() {
		if _, err := mc.exec("set autocommit = 1"); err != nil {
			mc.conn.Close()

			return err
		}
	}

	mc.lastPing = time.Now().Unix()

	return nil
}

func (mc *MysqlConn) readPacket() ([]byte, error) {
	d, err := mc.pkg.ReadPacket()
	mc.pkgErr = err
	return d, err
}

func (mc *MysqlConn) writePacket(data []byte) error {
	err := mc.pkg.WritePacket(data)
	mc.pkgErr = err
	if err != nil {
		return err
	}
	err = mc.pkg.Flush()
	mc.pkgErr = err
	return err
}

func (mc *MysqlConn) readInitialHandshake() error {
	data, err := mc.readPacket()
	if err != nil {
		return err
	}

	if data[0] == ERR_HEADER {
		return errors.New("read initial handshake error")
	}

	if data[0] < MinProtocolVersion {
		return fmt.Errorf("invalid protocol version %d, must >= 10", data[0])
	}

	//skip mysql version and connection id
	//mysql version end with 0x00
	//connection id length is 4
	pos := 1 + bytes.IndexByte(data[1:], 0x00) + 1 + 4

	mc.salt = append(mc.salt, data[pos:pos+8]...)

	//skip filter
	pos += 8 + 1

	//capability lower 2 bytes
	mc.capability = uint32(binary.LittleEndian.Uint16(data[pos : pos+2]))

	pos += 2

	if len(data) > pos {
		//skip server charset
		//c.charset = data[pos]
		pos += 1

		mc.status = binary.LittleEndian.Uint16(data[pos : pos+2])
		pos += 2

		mc.capability = uint32(binary.LittleEndian.Uint16(data[pos:pos+2]))<<16 | mc.capability

		pos += 2

		//skip auth data len or [00]
		//skip reserved (all [00])
		pos += 10 + 1

		// The documentation is ambiguous about the length.
		// The official Python library uses the fixed length 12
		// mysql-proxy also use 12
		// which is not documented but seems to work.
		mc.salt = append(mc.salt, data[pos:pos+12]...)
	}

	return nil
}

func (mc *MysqlConn) writeAuthHandshake() error {
	// Adjust client capability flags based on server support
	capability := CLIENT_PROTOCOL_41 | CLIENT_SECURE_CONNECTION |
		CLIENT_LONG_PASSWORD | CLIENT_TRANSACTIONS | CLIENT_LONG_FLAG

	capability &= mc.capability

	//packet length
	//capbility 4
	//max-packet size 4
	//charset 1
	//reserved all[0] 23
	length := 4 + 4 + 1 + 23

	//username
	length += len(mc.user) + 1

	//we only support secure connection
	auth := CalcPassword(mc.salt, []byte(mc.password))

	length += 1 + len(auth)

	if len(mc.db) > 0 {
		capability |= CLIENT_CONNECT_WITH_DB

		length += len(mc.db) + 1
	}

	mc.capability = capability

	data := make([]byte, length+4)

	//capability [32 bit]
	data[4] = byte(capability)
	data[5] = byte(capability >> 8)
	data[6] = byte(capability >> 16)
	data[7] = byte(capability >> 24)

	//MaxPacketSize [32 bit] (none)
	//data[8] = 0x00
	//data[9] = 0x00
	//data[10] = 0x00
	//data[11] = 0x00

	//Charset [1 byte]
	data[12] = byte(mc.collation)

	//Filler [23 bytes] (all 0x00)
	pos := 13 + 23

	//User [null terminated string]
	if len(mc.user) > 0 {
		pos += copy(data[pos:], mc.user)
	}
	//data[pos] = 0x00
	pos++

	// auth [length encoded integer]
	data[pos] = byte(len(auth))
	pos += 1 + copy(data[pos+1:], auth)

	// db [null terminated string]
	if len(mc.db) > 0 {
		pos += copy(data[pos:], mc.db)
		//data[pos] = 0x00
	}

	return mc.writePacket(data)
}

func (mc *MysqlConn) writeCommand(command byte) error {
	mc.pkg.Sequence = 0

	return mc.writePacket([]byte{
		0x01, //1 bytes long
		0x00,
		0x00,
		0x00, //sequence
		command,
	})
}

func (mc *MysqlConn) writeCommandBuf(command byte, arg []byte) error {
	mc.pkg.Sequence = 0

	length := len(arg) + 1

	data := make([]byte, length+4)

	data[4] = command

	copy(data[5:], arg)

	return mc.writePacket(data)
}

func (mc *MysqlConn) writeCommandStr(command byte, arg string) error {
	mc.pkg.Sequence = 0

	length := len(arg) + 1

	data := make([]byte, length+4)

	data[4] = command

	copy(data[5:], arg)

	return mc.writePacket(data)
}

func (mc *MysqlConn) writeCommandUint32(command byte, arg uint32) error {
	mc.pkg.Sequence = 0

	return mc.writePacket([]byte{
		0x05, //5 bytes long
		0x00,
		0x00,
		0x00, //sequence

		command,

		byte(arg),
		byte(arg >> 8),
		byte(arg >> 16),
		byte(arg >> 24),
	})
}

func (mc *MysqlConn) writeCommandStrStr(command byte, arg1 string, arg2 string) error {
	mc.pkg.Sequence = 0

	data := make([]byte, 4, 6+len(arg1)+len(arg2))

	data = append(data, command)
	data = append(data, arg1...)
	data = append(data, 0)
	data = append(data, arg2...)

	return mc.writePacket(data)
}

func (mc *MysqlConn) UseDB(dbName string) error {
	if mc.db == dbName {
		return nil
	}

	if err := mc.writeCommandStr(byte(COM_INIT_DB), dbName); err != nil {
		return err
	}

	if err := mc.readOK(); err != nil {
		return err
	}

	mc.db = dbName
	return nil
}

func (mc *MysqlConn) GetDB() string {
	return mc.db
}

func (mc *MysqlConn) Execute(command string, args ...interface{}) (*ResultSet, error) {
	if len(args) != 0 {
		command = interpolateParams(command, mc.status&SERVER_STATUS_NO_BACKSLASH_ESCAPED > 0, args...)
	}
	return mc.exec(command)
}

func (mc *MysqlConn) Begin() error {
	_, err := mc.exec("begin")
	return err
}

func (mc *MysqlConn) Commit() error {
	_, err := mc.exec("commit")
	return err
}

func (mc *MysqlConn) Rollback() error {
	_, err := mc.exec("rollback")
	return err
}

func (mc *MysqlConn) SetCharset(charset string) error {
	charset = strings.Trim(charset, "\"'`")
	if mc.charset == charset {
		return nil
	}

	cid, ok := CharsetIds[charset]
	if !ok {
		return fmt.Errorf("invalid charset %s", charset)
	}

	if _, err := mc.exec(fmt.Sprintf("set names %s", charset)); err != nil {
		return err
	} else {
		mc.collation = cid
		return nil
	}
}

func (mc *MysqlConn) FieldList(table string, wildcard string) ([]*ColumnInfo, error) {
	if err := mc.writeCommandStrStr(byte(COM_FIELD_LIST), table, wildcard); err != nil {
		return nil, err
	}

	data, err := mc.readPacket()
	if err != nil {
		return nil, err
	}

	fs := make([]*ColumnInfo, 0, 4)
	var f *ColumnInfo
	if data[0] == ERR_HEADER {
		return nil, mc.handleErrorPacket(data)
	} else {
		for {
			if data, err = mc.readPacket(); err != nil {
				return nil, err
			}

			// EOF Packet
			if mc.isEOFPacket(data) {
				return fs, nil
			}

			if f, err = ParseColumnInfo(data); err != nil {
				return nil, err
			}
			fs = append(fs, f)
		}
	}
	return nil, fmt.Errorf("field list error")
}

func (mc *MysqlConn) exec(query string) (*ResultSet, error) {
	if err := mc.writeCommandStr(byte(COM_QUERY), query); err != nil {
		return nil, err
	}

	return mc.readResult(false)
}

func (mc *MysqlConn) readResultSet(data []byte, binary bool) (*ResultSet, error) {
	result := &ResultSet{}

	// column count
	count, _, n := LengthEncodedInt(data)

	if n-len(data) != 0 {
		return nil, ErrMalformPacket
	}
	var err error
	result.Columns, err = mc.readColumns(int(count))
	if err != nil {
		return nil, err
	}

	if err := mc.readResultRows(result, binary); err != nil {
		return nil, err
	}

	return result, nil
}

func (mc *MysqlConn) readColumns(count int) (columns []*ColumnInfo, err error) {
	columns = make([]*ColumnInfo, count)
	var i int = 0
	var data []byte

	for {
		data, err = mc.readPacket()
		if err != nil {
			return
		}

		// EOF Packet
		if mc.isEOFPacket(data) {
			if mc.capability&CLIENT_PROTOCOL_41 > 0 {
				//result.Warnings = binary.LittleEndian.Uint16(data[1:])
				//todo add strict_mode, warning will be treat as error
				mc.status = binary.LittleEndian.Uint16(data[3:])
			}

			if i != count {
				err = ErrMalformPacket
			}

			return
		}

		columns[i], err = ParseColumnInfo(data)
		if err != nil {
			return
		}

		i++
	}
}

func (mc *MysqlConn) readResultRows(result *ResultSet, isBinary bool) (err error) {
	var data []byte
	var rowDatas [][]byte
	for {
		data, err = mc.readPacket()

		if err != nil {
			return
		}

		// EOF Packet
		if mc.isEOFPacket(data) {
			if mc.capability&CLIENT_PROTOCOL_41 > 0 {
				//result.Warnings = binary.LittleEndian.Uint16(data[1:])
				//todo add strict_mode, warning will be treat as error
				mc.status = binary.LittleEndian.Uint16(data[3:])
			}

			break
		}

		rowDatas = append(rowDatas, data)
	}

	result.Rows = make([][]interface{}, len(rowDatas))

	for i, rowData := range rowDatas {
		if isBinary {
			result.Rows[i], err = ParseRowValuesBinary(result.Columns, rowData)
		} else {
			result.Rows[i], err = ParseRowValuesText(result.Columns, rowData)
		}

		if err != nil {
			return err
		}
	}

	return nil
}

func (mc *MysqlConn) readUntilEOF() (err error) {
	var data []byte

	for {
		data, err = mc.readPacket()

		if err != nil {
			return
		}

		// EOF Packet
		if mc.isEOFPacket(data) {
			return
		}
	}
	return
}

func (mc *MysqlConn) isEOFPacket(data []byte) bool {
	return data[0] == EOF_HEADER && len(data) <= 5
}

func (mc *MysqlConn) handleOKPacket(data []byte) error {
	var n int
	var pos int = 1

	mc.affectedRows, _, n = LengthEncodedInt(data[pos:])
	pos += n
	mc.lastInsertID, _, n = LengthEncodedInt(data[pos:])
	pos += n

	if mc.capability&CLIENT_PROTOCOL_41 > 0 {
		mc.status = binary.LittleEndian.Uint16(data[pos:])
		pos += 2

		//todo:strict_mode, check warnings as error
		//Warnings := binary.LittleEndian.Uint16(data[pos:])
		//pos += 2
	} else if mc.capability&CLIENT_TRANSACTIONS > 0 {
		mc.status = binary.LittleEndian.Uint16(data[pos:])
		pos += 2
	}

	//info
	return nil
}

func (mc *MysqlConn) handleErrorPacket(data []byte) error {
	e := new(SqlError)

	var pos int = 1

	e.Code = binary.LittleEndian.Uint16(data[pos:])
	pos += 2

	if mc.capability&CLIENT_PROTOCOL_41 > 0 {
		//skip '#'
		pos++
		e.State = string(data[pos : pos+5])
		pos += 5
	}

	e.Message = string(data[pos:])

	return e
}

func (mc *MysqlConn) readOK() error {
	data, err := mc.readPacket()
	if err != nil {
		return err
	}

	if data[0] == OK_HEADER {
		return mc.handleOKPacket(data)
	} else if data[0] == ERR_HEADER {
		return mc.handleErrorPacket(data)
	} else {
		return errors.New("invalid ok packet")
	}
}

func (mc *MysqlConn) readResult(binary bool) (*ResultSet, error) {
	data, err := mc.readPacket()
	if err != nil {
		return nil, err
	}

	if data[0] == OK_HEADER {
		return nil, mc.handleOKPacket(data)
	} else if data[0] == ERR_HEADER {
		return nil, mc.handleErrorPacket(data)
	} else if data[0] == LocalInFile_HEADER {
		return nil, ErrMalformPacket
	}

	return mc.readResultSet(data, binary)
}

func (mc *MysqlConn) IsAutoCommit() bool {
	return mc.status&SERVER_STATUS_AUTOCOMMIT > 0
}

func (mc *MysqlConn) IsInTransaction() bool {
	return mc.status&SERVER_STATUS_IN_TRANS > 0
}

func (mc *MysqlConn) GetCharset() string {
	return mc.charset
}

func (mc *MysqlConn) Prepare(query string) (stmt Statement, err error) {
	if err := mc.writeCommandStr(byte(COM_STMT_PREPARE), query); err != nil {
		return nil, err
	}

	data, err := mc.readPacket()
	if err != nil {
		return nil, err
	}

	if data[0] == ERR_HEADER {
		return nil, mc.handleErrorPacket(data)
	} else if data[0] != OK_HEADER {
		return nil, ErrMalformPacket
	}

	pos := 1
	mStmt := new(MysqlStatement)

	//for statement id
	mStmt.id = int(binary.LittleEndian.Uint32(data[pos:]))
	pos += 4

	//number columns
	numColumns := int(binary.LittleEndian.Uint16(data[pos:]))
	pos += 2

	//number params
	numParams := int(binary.LittleEndian.Uint16(data[pos:]))
	pos += 2

	//warnings
	//warnings = binary.LittleEndian.Uint16(data[pos:])

	if numParams > 0 {
		mStmt.params, err = mc.readColumns(numParams)
		if err != nil {
			return
		}
	}

	if numColumns > 0 {
		mStmt.columns, err = mc.readColumns(numColumns)
		if err != nil {
			return
		}
	}
	mc.stmts[mStmt.id] = mStmt
	stmt = mStmt
	return
}

func (mc *MysqlConn) CloseStmt(stmtId int) (err error) {
	return
}

func (mc *MysqlConn) GetStatement(stmtId int) Statement {
	return mc.stmts[stmtId]
}

func (mc *MysqlConn) Close() error {
	return mc.conn.Close()
}