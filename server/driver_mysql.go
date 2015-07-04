package server

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"math"
	"net"
	"strings"
	"time"

	. "github.com/pingcap/mp/protocol"
)

type MysqlCtx struct {
	status       uint16
	lastInsertID uint64
	affectedRows uint64
	conn         *MysqlConn
}

type MysqlDriver struct {
}

func (md *MysqlDriver) OpenCtx() Context {
	mc := new(MysqlConn)
	mc.Connect(":3306", "root", "", "test")
	return mc
}

func (md *MysqlDriver) CloseCtx(ctx Context) error {
	mc := ctx.(*MysqlConn)
	return mc.Close()
}

func (md *MysqlDriver) Execute(ctx Context, sql string, args ...interface{}) (rs *ResultSet, err error) {
	mc := ctx.(*MysqlConn)
	mc.Execute(sql, args...)
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
}

func (mCtx *MysqlConn) Status() uint16 {
	return mCtx.status
}

func (mCtx *MysqlConn) LastInsertID() uint64 {
	return mCtx.lastInsertID
}

func (mCtx *MysqlConn) AffectedRows() uint64 {
	return mCtx.affectedRows
}

func (mCtx *MysqlConn) CurrentDatabase() string {
	return mCtx.db
}

func (c *MysqlConn) Connect(addr string, user string, password string, db string) error {
	c.addr = addr
	c.user = user
	c.password = password
	c.db = db

	//use utf8
	c.collation = DEFAULT_COLLATION_ID
	c.charset = DEFAULT_CHARSET

	return c.ReConnect()
}

func (c *MysqlConn) ReConnect() error {
	if c.conn != nil {
		c.conn.Close()
	}

	n := "tcp"
	if strings.Contains(c.addr, "/") {
		n = "unix"
	}

	netConn, err := net.Dial(n, c.addr)
	if err != nil {
		return err
	}

	c.conn = netConn
	c.pkg = NewPacketIO(netConn)

	if err := c.readInitialHandshake(); err != nil {
		c.conn.Close()
		return err
	}

	if err := c.writeAuthHandshake(); err != nil {
		c.conn.Close()

		return err
	}

	if _, err := c.readOK(); err != nil {
		c.conn.Close()

		return err
	}

	//we must always use autocommit
	if !c.IsAutoCommit() {
		if _, err := c.exec("set autocommit = 1"); err != nil {
			c.conn.Close()

			return err
		}
	}

	c.lastPing = time.Now().Unix()

	return nil
}

func (c *MysqlConn) Close() error {
	if c.conn != nil {
		c.conn.Close()
		c.conn = nil
	}

	return nil
}

func (c *MysqlConn) readPacket() ([]byte, error) {
	d, err := c.pkg.ReadPacket()
	c.pkgErr = err
	return d, err
}

func (c *MysqlConn) writePacket(data []byte) error {
	err := c.pkg.WritePacket(data)
	c.pkgErr = err
	return err
}

func (c *MysqlConn) readInitialHandshake() error {
	data, err := c.readPacket()
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

	c.salt = append(c.salt, data[pos:pos+8]...)

	//skip filter
	pos += 8 + 1

	//capability lower 2 bytes
	c.capability = uint32(binary.LittleEndian.Uint16(data[pos : pos+2]))

	pos += 2

	if len(data) > pos {
		//skip server charset
		//c.charset = data[pos]
		pos += 1

		c.status = binary.LittleEndian.Uint16(data[pos : pos+2])
		pos += 2

		c.capability = uint32(binary.LittleEndian.Uint16(data[pos:pos+2]))<<16 | c.capability

		pos += 2

		//skip auth data len or [00]
		//skip reserved (all [00])
		pos += 10 + 1

		// The documentation is ambiguous about the length.
		// The official Python library uses the fixed length 12
		// mysql-proxy also use 12
		// which is not documented but seems to work.
		c.salt = append(c.salt, data[pos:pos+12]...)
	}

	return nil
}

func (c *MysqlConn) writeAuthHandshake() error {
	// Adjust client capability flags based on server support
	capability := CLIENT_PROTOCOL_41 | CLIENT_SECURE_CONNECTION |
		CLIENT_LONG_PASSWORD | CLIENT_TRANSACTIONS | CLIENT_LONG_FLAG

	capability &= c.capability

	//packet length
	//capbility 4
	//max-packet size 4
	//charset 1
	//reserved all[0] 23
	length := 4 + 4 + 1 + 23

	//username
	length += len(c.user) + 1

	//we only support secure connection
	auth := CalcPassword(c.salt, []byte(c.password))

	length += 1 + len(auth)

	if len(c.db) > 0 {
		capability |= CLIENT_CONNECT_WITH_DB

		length += len(c.db) + 1
	}

	c.capability = capability

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
	data[12] = byte(c.collation)

	//Filler [23 bytes] (all 0x00)
	pos := 13 + 23

	//User [null terminated string]
	if len(c.user) > 0 {
		pos += copy(data[pos:], c.user)
	}
	//data[pos] = 0x00
	pos++

	// auth [length encoded integer]
	data[pos] = byte(len(auth))
	pos += 1 + copy(data[pos+1:], auth)

	// db [null terminated string]
	if len(c.db) > 0 {
		pos += copy(data[pos:], c.db)
		//data[pos] = 0x00
	}

	return c.writePacket(data)
}

func (c *MysqlConn) writeCommand(command byte) error {
	c.pkg.Sequence = 0

	return c.writePacket([]byte{
		0x01, //1 bytes long
		0x00,
		0x00,
		0x00, //sequence
		command,
	})
}

func (c *MysqlConn) writeCommandBuf(command byte, arg []byte) error {
	c.pkg.Sequence = 0

	length := len(arg) + 1

	data := make([]byte, length+4)

	data[4] = command

	copy(data[5:], arg)

	return c.writePacket(data)
}

func (c *MysqlConn) writeCommandStr(command byte, arg string) error {
	c.pkg.Sequence = 0

	length := len(arg) + 1

	data := make([]byte, length+4)

	data[4] = command

	copy(data[5:], arg)

	return c.writePacket(data)
}

func (c *MysqlConn) writeCommandUint32(command byte, arg uint32) error {
	c.pkg.Sequence = 0

	return c.writePacket([]byte{
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

func (c *MysqlConn) writeCommandStrStr(command byte, arg1 string, arg2 string) error {
	c.pkg.Sequence = 0

	data := make([]byte, 4, 6+len(arg1)+len(arg2))

	data = append(data, command)
	data = append(data, arg1...)
	data = append(data, 0)
	data = append(data, arg2...)

	return c.writePacket(data)
}

func (c *MysqlConn) Ping() error {
	n := time.Now().Unix()

	if n-c.lastPing > pingPeriod {
		if err := c.writeCommand(COM_PING); err != nil {
			return err
		}

		if _, err := c.readOK(); err != nil {
			return err
		}
	}

	c.lastPing = n

	return nil
}

func (c *MysqlConn) UseDB(dbName string) error {
	if c.db == dbName {
		return nil
	}

	if err := c.writeCommandStr(COM_INIT_DB, dbName); err != nil {
		return err
	}

	if _, err := c.readOK(); err != nil {
		return err
	}

	c.db = dbName
	return nil
}

func (c *MysqlConn) GetDB() string {
	return c.db
}

func (c *MysqlConn) Execute(command string, args ...interface{}) (*ResultSet, error) {
	if len(args) == 0 {
		return c.exec(command)
	} else {
		if s, err := c.Prepare(command); err != nil {
			return nil, err
		} else {
			var r *ResultSet
			r, err = s.Execute(args...)
			s.Close()
			return r, err
		}
	}
}

func (c *MysqlConn) Begin() error {
	_, err := c.exec("begin")
	return err
}

func (c *MysqlConn) Commit() error {
	_, err := c.exec("commit")
	return err
}

func (c *MysqlConn) Rollback() error {
	_, err := c.exec("rollback")
	return err
}

func (c *MysqlConn) SetCharset(charset string) error {
	charset = strings.Trim(charset, "\"'`")
	if c.charset == charset {
		return nil
	}

	cid, ok := CharsetIds[charset]
	if !ok {
		return fmt.Errorf("invalid charset %s", charset)
	}

	if _, err := c.exec(fmt.Sprintf("set names %s", charset)); err != nil {
		return err
	} else {
		c.collation = cid
		return nil
	}
}

func (c *MysqlConn) FieldList(table string, wildcard string) ([]*ColumnInfo, error) {
	if err := c.writeCommandStrStr(COM_FIELD_LIST, table, wildcard); err != nil {
		return nil, err
	}

	data, err := c.readPacket()
	if err != nil {
		return nil, err
	}

	fs := make([]*ColumnInfo, 0, 4)
	var f *ColumnInfo
	if data[0] == ERR_HEADER {
		return nil, c.handleErrorPacket(data)
	} else {
		for {
			if data, err = c.readPacket(); err != nil {
				return nil, err
			}

			// EOF Packet
			if c.isEOFPacket(data) {
				return fs, nil
			}

			if f, err = FieldData(data).Parse(); err != nil {
				return nil, err
			}
			fs = append(fs, f)
		}
	}
	return nil, fmt.Errorf("field list error")
}

func (c *MysqlConn) exec(query string) (*ResultSet, error) {
	if err := c.writeCommandStr(COM_QUERY, query); err != nil {
		return nil, err
	}

	return c.readResult(false)
}

func (c *MysqlConn) readResultset(data []byte, binary bool) (*ResultSet, error) {
	resultSet := &ResultSet{}

	// column count
	count, _, n := LengthEncodedInt(data)

	if n-len(data) != 0 {
		return nil, ErrMalformPacket
	}

	resultSet.Columns = make([]*ColumnInfo, count)

	if err := c.readResultColumns(result); err != nil {
		return nil, err
	}

	if err := c.readResultRows(result, binary); err != nil {
		return nil, err
	}

	return result, nil
}

func (c *MysqlConn) readResultColumns(result *ResultSet) (err error) {
	var i int = 0
	var data []byte

	for {
		data, err = c.readPacket()
		if err != nil {
			return
		}

		// EOF Packet
		if c.isEOFPacket(data) {
			if c.capability&CLIENT_PROTOCOL_41 > 0 {
				//result.Warnings = binary.LittleEndian.Uint16(data[1:])
				//todo add strict_mode, warning will be treat as error
				result.Status = binary.LittleEndian.Uint16(data[3:])
				c.status = result.Status
			}

			if i != len(result.Fields) {
				err = ErrMalformPacket
			}

			return
		}

		result.Fields[i], err = FieldData(data).Parse()
		if err != nil {
			return
		}

		result.FieldNames[string(result.Fields[i].Name)] = i

		i++
	}
}

func (c *MysqlConn) readResultRows(result *Result, isBinary bool) (err error) {
	var data []byte

	for {
		data, err = c.readPacket()

		if err != nil {
			return
		}

		// EOF Packet
		if c.isEOFPacket(data) {
			if c.capability&CLIENT_PROTOCOL_41 > 0 {
				//result.Warnings = binary.LittleEndian.Uint16(data[1:])
				//todo add strict_mode, warning will be treat as error
				result.Status = binary.LittleEndian.Uint16(data[3:])
				c.status = result.Status
			}

			break
		}

		result.RowDatas = append(result.RowDatas, data)
	}

	result.Values = make([][]interface{}, len(result.RowDatas))

	for i := range result.Values {
		result.Values[i], err = result.RowDatas[i].Parse(result.Fields, isBinary)

		if err != nil {
			return err
		}
	}

	return nil
}

func (c *MysqlConn) readUntilEOF() (err error) {
	var data []byte

	for {
		data, err = c.readPacket()

		if err != nil {
			return
		}

		// EOF Packet
		if c.isEOFPacket(data) {
			return
		}
	}
	return
}

func (c *MysqlConn) isEOFPacket(data []byte) bool {
	return data[0] == EOF_HEADER && len(data) <= 5
}

func (c *MysqlConn) handleOKPacket(data []byte) (*ResultSet, error) {
	var n int
	var pos int = 1

	r := new(ResultSet)

	r.AffectedRows, _, n = LengthEncodedInt(data[pos:])
	pos += n
	r.InsertId, _, n = LengthEncodedInt(data[pos:])
	pos += n

	if c.capability&CLIENT_PROTOCOL_41 > 0 {
		r.Status = binary.LittleEndian.Uint16(data[pos:])
		c.status = r.Status
		pos += 2

		//todo:strict_mode, check warnings as error
		//Warnings := binary.LittleEndian.Uint16(data[pos:])
		//pos += 2
	} else if c.capability&CLIENT_TRANSACTIONS > 0 {
		r.Status = binary.LittleEndian.Uint16(data[pos:])
		c.status = r.Status
		pos += 2
	}

	//info
	return r, nil
}

func (c *MysqlConn) handleErrorPacket(data []byte) error {
	e := new(SqlError)

	var pos int = 1

	e.Code = binary.LittleEndian.Uint16(data[pos:])
	pos += 2

	if c.capability&CLIENT_PROTOCOL_41 > 0 {
		//skip '#'
		pos++
		e.State = string(data[pos : pos+5])
		pos += 5
	}

	e.Message = string(data[pos:])

	return e
}

func (c *MysqlConn) readOK() (*ResultSet, error) {
	data, err := c.readPacket()
	if err != nil {
		return nil, err
	}

	if data[0] == OK_HEADER {
		return c.handleOKPacket(data)
	} else if data[0] == ERR_HEADER {
		return nil, c.handleErrorPacket(data)
	} else {
		return nil, errors.New("invalid ok packet")
	}
}

func (c *MysqlConn) readResult(binary bool) (*ResultSet, error) {
	data, err := c.readPacket()
	if err != nil {
		return nil, err
	}

	if data[0] == OK_HEADER {
		return c.handleOKPacket(data)
	} else if data[0] == ERR_HEADER {
		return nil, c.handleErrorPacket(data)
	} else if data[0] == LocalInFile_HEADER {
		return nil, ErrMalformPacket
	}

	return c.readResultset(data, binary)
}

func (c *MysqlConn) IsAutoCommit() bool {
	return c.status&SERVER_STATUS_AUTOCOMMIT > 0
}

func (c *MysqlConn) IsInTransaction() bool {
	return c.status&SERVER_STATUS_IN_TRANS > 0
}

func (c *MysqlConn) GetCharset() string {
	return c.charset
}

type Stmt struct {
	conn  *Conn
	id    uint32
	query string

	params  int
	columns int
}

func (s *Stmt) ParamNum() int {
	return s.params
}

func (s *Stmt) ColumnNum() int {
	return s.columns
}

func (s *Stmt) Execute(args ...interface{}) (*ResultSet, error) {
	if err := s.write(args...); err != nil {
		return nil, err
	}

	return s.conn.readResult(true)
}

func (s *Stmt) Close() error {
	if err := s.conn.writeCommandUint32(COM_STMT_CLOSE, s.id); err != nil {
		return err
	}

	return nil
}

func (s *Stmt) write(args ...interface{}) error {
	paramsNum := s.params

	if len(args) != paramsNum {
		return fmt.Errorf("argument mismatch, need %d but got %d", s.params, len(args))
	}

	paramTypes := make([]byte, paramsNum<<1)
	paramValues := make([][]byte, paramsNum)

	//NULL-bitmap, length: (num-params+7)
	nullBitmap := make([]byte, (paramsNum+7)>>3)

	var length int = int(1 + 4 + 1 + 4 + ((paramsNum + 7) >> 3) + 1 + (paramsNum << 1))

	var newParamBoundFlag byte = 0

	for i := range args {
		if args[i] == nil {
			nullBitmap[i/8] |= (1 << (uint(i) % 8))
			paramTypes[i<<1] = MYSQL_TYPE_NULL
			continue
		}

		newParamBoundFlag = 1

		switch v := args[i].(type) {
		case int8:
			paramTypes[i<<1] = MYSQL_TYPE_TINY
			paramValues[i] = []byte{byte(v)}
		case int16:
			paramTypes[i<<1] = MYSQL_TYPE_SHORT
			paramValues[i] = Uint16ToBytes(uint16(v))
		case int32:
			paramTypes[i<<1] = MYSQL_TYPE_LONG
			paramValues[i] = Uint32ToBytes(uint32(v))
		case int:
			paramTypes[i<<1] = MYSQL_TYPE_LONGLONG
			paramValues[i] = Uint64ToBytes(uint64(v))
		case int64:
			paramTypes[i<<1] = MYSQL_TYPE_LONGLONG
			paramValues[i] = Uint64ToBytes(uint64(v))
		case uint8:
			paramTypes[i<<1] = MYSQL_TYPE_TINY
			paramTypes[(i<<1)+1] = 0x80
			paramValues[i] = []byte{v}
		case uint16:
			paramTypes[i<<1] = MYSQL_TYPE_SHORT
			paramTypes[(i<<1)+1] = 0x80
			paramValues[i] = Uint16ToBytes(uint16(v))
		case uint32:
			paramTypes[i<<1] = MYSQL_TYPE_LONG
			paramTypes[(i<<1)+1] = 0x80
			paramValues[i] = Uint32ToBytes(uint32(v))
		case uint:
			paramTypes[i<<1] = MYSQL_TYPE_LONGLONG
			paramTypes[(i<<1)+1] = 0x80
			paramValues[i] = Uint64ToBytes(uint64(v))
		case uint64:
			paramTypes[i<<1] = MYSQL_TYPE_LONGLONG
			paramTypes[(i<<1)+1] = 0x80
			paramValues[i] = Uint64ToBytes(uint64(v))
		case bool:
			paramTypes[i<<1] = MYSQL_TYPE_TINY
			if v {
				paramValues[i] = []byte{1}
			} else {
				paramValues[i] = []byte{0}

			}
		case float32:
			paramTypes[i<<1] = MYSQL_TYPE_FLOAT
			paramValues[i] = Uint32ToBytes(math.Float32bits(v))
		case float64:
			paramTypes[i<<1] = MYSQL_TYPE_DOUBLE
			paramValues[i] = Uint64ToBytes(math.Float64bits(v))
		case string:
			paramTypes[i<<1] = MYSQL_TYPE_STRING
			paramValues[i] = append(PutLengthEncodedInt(uint64(len(v))), v...)
		case []byte:
			paramTypes[i<<1] = MYSQL_TYPE_STRING
			paramValues[i] = append(PutLengthEncodedInt(uint64(len(v))), v...)
		default:
			return fmt.Errorf("invalid argument type %T", args[i])
		}

		length += len(paramValues[i])
	}

	data := make([]byte, 4, 4+length)

	data = append(data, COM_STMT_EXECUTE)
	data = append(data, byte(s.id), byte(s.id>>8), byte(s.id>>16), byte(s.id>>24))

	//flag: CURSOR_TYPE_NO_CURSOR
	data = append(data, 0x00)

	//iteration-count, always 1
	data = append(data, 1, 0, 0, 0)

	if s.params > 0 {
		data = append(data, nullBitmap...)

		//new-params-bound-flag
		data = append(data, newParamBoundFlag)

		if newParamBoundFlag == 1 {
			//type of each parameter, length: num-params * 2
			data = append(data, paramTypes...)

			//value of each parameter
			for _, v := range paramValues {
				data = append(data, v...)
			}
		}
	}

	s.conn.pkg.Sequence = 0

	return s.conn.writePacket(data)
}

func (c *MysqlConn) Prepare(query string) (*Stmt, error) {
	if err := c.writeCommandStr(COM_STMT_PREPARE, query); err != nil {
		return nil, err
	}

	data, err := c.readPacket()
	if err != nil {
		return nil, err
	}

	if data[0] == ERR_HEADER {
		return nil, c.handleErrorPacket(data)
	} else if data[0] != OK_HEADER {
		return nil, ErrMalformPacket
	}

	s := new(Stmt)
	s.conn = c

	pos := 1

	//for statement id
	s.id = binary.LittleEndian.Uint32(data[pos:])
	pos += 4

	//number columns
	s.columns = int(binary.LittleEndian.Uint16(data[pos:]))
	pos += 2

	//number params
	s.params = int(binary.LittleEndian.Uint16(data[pos:]))
	pos += 2

	//warnings
	//warnings = binary.LittleEndian.Uint16(data[pos:])

	if s.params > 0 {
		if err := s.conn.readUntilEOF(); err != nil {
			return nil, err
		}
	}

	if s.columns > 0 {
		if err := s.conn.readUntilEOF(); err != nil {
			return nil, err
		}
	}

	return s, nil
}
