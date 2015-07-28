package server

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"math"
	"net"
	"time"

	"github.com/juju/errors"
	"github.com/pingcap/mp/hack"
	. "github.com/pingcap/mysqldef"
)

type MysqlDriver struct{}

type MysqlStatement struct {
	mConn       *MysqlConn
	id          int
	sql         string
	boundParams [][]byte
	numColumns  int
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
	warningCount uint16

	collation byte
	charset   string
	salt      []byte

	pkgErr error

	stmts map[int]*MysqlStatement //statement id : parameters column info
}

func (md *MysqlDriver) OpenCtx(capability uint32, collation uint8, dbname string) (ctx IContext, err error) {
	mc := new(MysqlConn)
	mc.stmts = make(map[int]*MysqlStatement)
	mc.capability = capability & DefaultCapability
	mc.collation = collation
	err = mc.connect(":3306", "root", "", dbname)
	if err != nil {
		return nil, err
	}
	ctx = mc
	return
}

// Reference: https://dev.mysql.com/doc/internals/en/com-stmt-execute.html
func (ms *MysqlStatement) sendExecuteCommand(args ...interface{}) error {
	const minPktLen = 4 + 1 + 4 + 1 + 4
	mc := ms.mConn

	var data = make([]byte, minPktLen)
	if data == nil {
		// can not take the buffer. Something must be wrong with the connection
		return ErrBadConn
	}

	// command [1 byte]
	data[4] = ComStmtExecute

	// statement_id [4 bytes]
	data[5] = byte(ms.id)
	data[6] = byte(ms.id >> 8)
	data[7] = byte(ms.id >> 16)
	data[8] = byte(ms.id >> 24)

	// flags (0: CURSOR_TYPE_NO_CURSOR) [1 byte]
	data[9] = 0x00

	// iteration_count (uint32(1)) [4 bytes]
	data[10] = 0x01
	data[11] = 0x00
	data[12] = 0x00
	data[13] = 0x00

	if len(args) > 0 {
		pos := minPktLen

		var nullMask []byte
		if maskLen, typesLen := (len(args)+7)/8, 1+2*len(args); pos+maskLen+typesLen >= len(data) {
			// buffer has to be extended but we don't know by how much so
			// we depend on append after all data with known sizes fit.
			// We stop at that because we deal with a lot of columns here
			// which makes the required allocation size hard to guess.
			tmp := make([]byte, pos+maskLen+typesLen)
			copy(tmp[:pos], data[:pos])
			data = tmp
			nullMask = data[pos : pos+maskLen]
			pos += maskLen
		} else {
			nullMask = data[pos : pos+maskLen]
			for i := 0; i < maskLen; i++ {
				nullMask[i] = 0
			}
			pos += maskLen
		}

		// newParameterBoundFlag 1 [1 byte]
		data[pos] = 0x01
		pos++

		// type of each parameter [len(args)*2 bytes]
		paramTypes := data[pos:]
		pos += len(args) * 2

		// value of each parameter [n bytes]
		paramValues := data[pos:pos]
		valuesCap := cap(paramValues)

		for i, arg := range args {
			// build NULL-bitmap
			if arg == nil {
				nullMask[i/8] |= 1 << (uint(i) & 7)
				paramTypes[i+i] = TypeNull
				paramTypes[i+i+1] = 0x00
				continue
			}

			// cache types and values
			switch v := arg.(type) {
			case int64:
				paramTypes[i+i] = TypeLonglong
				paramTypes[i+i+1] = 0x00

				if cap(paramValues)-len(paramValues)-8 >= 0 {
					paramValues = paramValues[:len(paramValues)+8]
					binary.LittleEndian.PutUint64(
						paramValues[len(paramValues)-8:],
						uint64(v),
					)
				} else {
					paramValues = append(paramValues,
						dumpUint64(uint64(v))...,
					)
				}

			case float64:
				paramTypes[i+i] = TypeDouble
				paramTypes[i+i+1] = 0x00

				if cap(paramValues)-len(paramValues)-8 >= 0 {
					paramValues = paramValues[:len(paramValues)+8]
					binary.LittleEndian.PutUint64(
						paramValues[len(paramValues)-8:],
						math.Float64bits(v),
					)
				} else {
					paramValues = append(paramValues,
						dumpUint64(math.Float64bits(v))...,
					)
				}

			case bool:
				paramTypes[i+i] = TypeTiny
				paramTypes[i+i+1] = 0x00

				if v {
					paramValues = append(paramValues, 0x01)
				} else {
					paramValues = append(paramValues, 0x00)
				}

			case []byte:
				paramTypes[i+i] = TypeString
				paramTypes[i+i+1] = 0x00
				paramValues = append(paramValues, dumpLengthEncodedInt(uint64(len(v)))...)
				paramValues = append(paramValues, v...)

			case string:
				paramTypes[i+i] = TypeString
				paramTypes[i+i+1] = 0x00
				paramValues = append(paramValues, dumpLengthEncodedInt(uint64(len(v)))...)
				paramValues = append(paramValues, v...)

			case time.Time:
				paramTypes[i+i] = TypeString
				paramTypes[i+i+1] = 0x00

				var val []byte
				if v.IsZero() {
					val = []byte("0000-00-00")
				} else {
					val = []byte(FormatDatetime(v))
				}

				paramValues = append(paramValues, dumpLengthEncodedInt(uint64(len(val)))...)
				paramValues = append(paramValues, val...)

			default:
				return fmt.Errorf("Can't convert type: %T", arg)
			}
		}
		// Check if param values exceeded the available buffer
		// In that case we must build the data packet with the new values buffer
		if valuesCap != cap(paramValues) {
			data = append(data[:pos], paramValues...)
		}

		pos += len(paramValues)
		data = data[:pos]
	}
	mc.pkg.Sequence = 0
	mc.warningCount = 0
	return mc.writePacket(data)
}

func (ms *MysqlStatement) Execute(args ...interface{}) (rs *ResultSet, err error) {
	ms.Reset()
	if len(args) != ms.NumParams() {
		return nil, fmt.Errorf(
			"Arguments count mismatch (Got: %d Has: %d)",
			len(args),
			ms.NumParams(),
		)
	}
	err = ms.sendExecuteCommand(args...)
	if err != nil {
		return
	}

	if ms.numColumns > 0 {
		rs, err = ms.mConn.readResult(true)
	} else {
		err = ms.mConn.readOK()
	}
	return
}

func (ms *MysqlStatement) AppendParam(paramId int, data []byte) (err error) {
	if paramId >= len(ms.boundParams) {
		return NewDefaultError(ER_WRONG_ARGUMENTS, "stmt_send_longdata")
	}
	ms.boundParams[paramId] = append(ms.boundParams[paramId], data...)
	return
}

func (ms *MysqlStatement) BoundParams() [][]byte {
	return ms.boundParams
}

func (ms *MysqlStatement) NumParams() int {
	return len(ms.boundParams)
}

func (ms *MysqlStatement) Reset() {
	for i := range ms.boundParams {
		ms.boundParams[i] = nil
	}
}

func (ms *MysqlStatement) ID() int {
	return ms.id
}

func (ms *MysqlStatement) Close() error {
	err := ms.mConn.writeCommandBuf(byte(ComStmtClose), dumpUint32(uint32(ms.id)))
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

func (mc *MysqlConn) WarningCount() uint16 {
	return mc.warningCount
}

func (mc *MysqlConn) CurrentDB() string {
	return mc.db
}

func (mc *MysqlConn) connect(addr string, user string, password string, db string) error {
	mc.addr = addr
	mc.user = user
	mc.password = password
	mc.db = db
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

	if data[0] == ErrHeader {
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

	//server capability lower 2 bytes
	pos += 2

	if len(data) > pos {
		//skip server collation
		pos += 1

		mc.status = binary.LittleEndian.Uint16(data[pos : pos+2])
		pos += 2

		//server capability upper 2 bytes
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

	//packet length
	//capbility 4
	//max-packet size 4
	//charset 1
	//reserved all[0] 23
	length := 4 + 4 + 1 + 23

	//username
	length += len(mc.user) + 1

	//we only support secure connection
	auth := calcPassword(mc.salt, []byte(mc.password))

	length += 1 + len(auth)
	if len(mc.db) > 0 {
		length += len(mc.db) + 1
	}

	data := make([]byte, length+4)

	//capability [32 bit]
	data[4] = byte(mc.capability)
	data[5] = byte(mc.capability >> 8)
	data[6] = byte(mc.capability >> 16)
	data[7] = byte(mc.capability >> 24)

	//MaxPacketSize [32 bit] (none)
	//data[8] = 0x00
	//data[9] = 0x00
	//data[10] = 0x00
	//data[11] = 0x00

	//Collation [1 byte]
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

func (mc *MysqlConn) writeCommandBuf(command byte, arg []byte) error {
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

	if err := mc.writeCommandBuf(byte(ComInitDB), hack.Slice(dbName)); err != nil {
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

func (mc *MysqlConn) Execute(command string) (*ResultSet, error) {
	return mc.exec(command)
}

func (mc *MysqlConn) FieldList(table string, wildcard string) ([]*ColumnInfo, error) {
	if err := mc.writeCommandStrStr(byte(ComFieldList), table, wildcard); err != nil {
		return nil, err
	}

	data, err := mc.readPacket()
	if err != nil {
		return nil, err
	}

	fs := make([]*ColumnInfo, 0, 4)
	var f *ColumnInfo
	if data[0] == ErrHeader {
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
	mc.warningCount = 0
	if err := mc.writeCommandBuf(byte(ComQuery), hack.Slice(query)); err != nil {
		return nil, errors.Trace(err)
	}

	return mc.readResult(false)
}

func (mc *MysqlConn) readResultSet(data []byte, binary bool) (*ResultSet, error) {
	result := &ResultSet{}

	// column count
	count, _, n := parseLengthEncodedInt(data)

	if n-len(data) != 0 {
		return nil, ErrMalformPacket
	}
	var err error
	result.Columns, err = mc.readColumns(int(count))
	if err != nil {
		return nil, errors.Trace(err)
	}

	if err := mc.readResultRows(result, binary); err != nil {
		return nil, errors.Trace(err)
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
			err = errors.Trace(err)
			return
		}

		// EOF Packet
		if mc.isEOFPacket(data) {
			if mc.capability&ClientProtocol41 > 0 {
				//result.Warnings = binary.LittleEndian.Uint16(data[1:])
				//todo add strict_mode, warning will be treat as error
				mc.status = binary.LittleEndian.Uint16(data[3:])
			}

			if i != count {
				err = errors.Trace(ErrMalformPacket)
			}

			return
		}

		columns[i], err = ParseColumnInfo(data)
		if err != nil {
			err = errors.Trace(err)
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
			err = errors.Trace(err)
			return
		}

		// EOF Packet
		if mc.isEOFPacket(data) {
			if mc.capability&ClientProtocol41 > 0 {
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
			result.Rows[i], err = parseRowValuesBinary(result.Columns, rowData)
		} else {
			result.Rows[i], err = parseRowValuesText(result.Columns, rowData)
		}

		if err != nil {
			return errors.Trace(err)
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
	return data[0] == EOFHeader && len(data) <= 5
}

func (mc *MysqlConn) handleOKPacket(data []byte) (err error) {
	var n int
	var pos int = 1

	mc.affectedRows, _, n = parseLengthEncodedInt(data[pos:])
	pos += n
	mc.lastInsertID, _, n = parseLengthEncodedInt(data[pos:])
	pos += n

	if mc.capability&ClientProtocol41 > 0 {
		mc.status = binary.LittleEndian.Uint16(data[pos:])
		pos += 2

		//todo:strict_mode, check warnings as error
		mc.warningCount = binary.LittleEndian.Uint16(data[pos:])
		pos += 2
	} else if mc.capability&ClientTransactions > 0 {
		mc.status = binary.LittleEndian.Uint16(data[pos:])
		pos += 2
	}
	//info
	return
}

func (mc *MysqlConn) handleErrorPacket(data []byte) error {
	e := new(SqlError)

	var pos int = 1

	e.Code = binary.LittleEndian.Uint16(data[pos:])
	pos += 2

	if mc.capability&ClientProtocol41 > 0 {
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

	if data[0] == OKHeader {
		return mc.handleOKPacket(data)
	} else if data[0] == ErrHeader {
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

	if data[0] == OKHeader {
		return nil, mc.handleOKPacket(data)
	} else if data[0] == ErrHeader {
		return nil, mc.handleErrorPacket(data)
	} else if data[0] == LocalInFileHeader {
		return nil, ErrMalformPacket
	}

	return mc.readResultSet(data, binary)
}

func (mc *MysqlConn) Prepare(query string) (stmt IStatement, columns, params []*ColumnInfo, err error) {
	if err = mc.writeCommandBuf(byte(ComStmtPrepare), hack.Slice(query)); err != nil {
		return
	}

	var data []byte
	data, err = mc.readPacket()
	if err != nil {
		return
	}

	if data[0] == ErrHeader {
		err = mc.handleErrorPacket(data)
		return
	} else if data[0] != OKHeader {
		err = ErrMalformPacket
		return
	}

	pos := 1
	mStmt := new(MysqlStatement)

	mStmt.mConn = mc
	mStmt.sql = query

	//for statement id
	mStmt.id = int(binary.LittleEndian.Uint32(data[pos:]))
	pos += 4

	//number columns
	numColumns := int(binary.LittleEndian.Uint16(data[pos:]))
	pos += 2
	mStmt.numColumns = numColumns

	//number params
	numParams := int(binary.LittleEndian.Uint16(data[pos:]))
	pos += 2

	mStmt.boundParams = make([][]byte, numParams)
	//warnings
	//warnings = binary.LittleEndian.Uint16(data[pos:])

	if numParams > 0 {
		params, err = mc.readColumns(numParams)
		if err != nil {
			return
		}
	}

	if numColumns > 0 {
		columns, err = mc.readColumns(numColumns)
		if err != nil {
			return
		}
	}
	mc.stmts[mStmt.id] = mStmt
	stmt = mStmt
	return
}

func (mc *MysqlConn) GetStatement(stmtId int) IStatement {
	return mc.stmts[stmtId]
}

func (mc *MysqlConn) Close() error {
	return mc.conn.Close()
}
