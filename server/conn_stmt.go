package server

import (
	"encoding/binary"
	"fmt"
	"math"
	"reflect"
	"strconv"
	"strings"

	"github.com/ngaut/log"
	"github.com/pingcap/mp/hack"
	. "github.com/pingcap/mysqldef"
)

func (cc *ClientConn) handleStmtPrepare(sql string) error {
	stmt, columns, params, err := cc.ctx.Prepare(sql)
	if err != nil {
		return err
	}
	data := make([]byte, 4, 128)

	//status ok
	data = append(data, 0)
	//stmt id
	data = append(data, dumpUint32(uint32(stmt.ID()))...)
	//number columns
	data = append(data, dumpUint16(uint16(len(columns)))...)
	//number params
	data = append(data, dumpUint16(uint16(len(params)))...)
	//filter [00]
	data = append(data, 0)
	//warning count
	data = append(data, 0, 0) //TODO support warning count

	if err := cc.writePacket(data); err != nil {
		return err
	}

	if len(params) > 0 {
		for i := 0; i < len(params); i++ {
			data = data[0:4]
			data = append(data, params[i].Dump(cc.alloc)...)

			if err := cc.writePacket(data); err != nil {
				return err
			}
		}

		if err := cc.writeEOF(); err != nil {
			return err
		}
	}

	if len(columns) > 0 {
		for i := 0; i < len(columns); i++ {
			data = data[0:4]
			data = append(data, columns[i].Dump(cc.alloc)...)

			if err := cc.writePacket(data); err != nil {
				return err
			}
		}

		if err := cc.writeEOF(); err != nil {
			return err
		}

	}
	return cc.flush()
}

func (cc *ClientConn) handleStmtExecute(data []byte) (err error) {
	if len(data) < 9 {
		return ErrMalformPacket
	}

	pos := 0
	stmtId := binary.LittleEndian.Uint32(data[0:4])
	pos += 4

	stmt := cc.ctx.GetStatement(int(stmtId))
	if stmt == nil {
		return NewDefaultError(ER_UNKNOWN_STMT_HANDLER,
			strconv.FormatUint(uint64(stmtId), 10), "stmt_execute")
	}

	flag := data[pos]
	pos++
	//now we only support CURSOR_TYPE_NO_CURSOR flag
	if flag != 0 {
		return NewError(ER_UNKNOWN_ERROR, fmt.Sprintf("unsupported flag %d", flag))
	}

	//skip iteration-count, always 1
	pos += 4

	var (
		nullBitmaps []byte
		paramTypes  []byte
		paramValues []byte
	)
	numParams := stmt.NumParams()
	args := make([]interface{}, numParams)
	if numParams > 0 {
		nullBitmapLen := (numParams + 7) >> 3
		if len(data) < (pos + nullBitmapLen + 1) {
			return ErrMalformPacket
		}
		nullBitmaps = data[pos : pos+nullBitmapLen]
		pos += nullBitmapLen

		//new param bound flag
		if data[pos] == 1 {
			pos++
			if len(data) < (pos + (numParams << 1)) {
				return ErrMalformPacket
			}

			paramTypes = data[pos : pos+(numParams<<1)]
			pos += (numParams << 1)
			paramValues = data[pos:]
		}

		err = parseStmtArgs(args, stmt.BoundParams(), nullBitmaps, paramTypes, paramValues)
		if err != nil {
			return err
		}
	}
	rs, err := stmt.Execute(args...)
	if err != nil {
		return err
	}
	if rs == nil {
		return cc.writeOK()
	}

	return cc.writeResultset(rs, true)
}

func parseStmtArgs(args []interface{}, boundParams [][]byte, nullBitmap, paramTypes, paramValues []byte) (err error) {
	pos := 0
	var v []byte
	var n int = 0
	var isNull bool

	for i := 0; i < len(args); i++ {
		if nullBitmap[i>>3]&(1<<(uint(i)%8)) > 0 {
			args[i] = nil
			continue
		}
		if boundParams[i] != nil {
			args[i] = boundParams[i]
			continue
		}

		tp := paramTypes[i<<1]
		isUnsigned := (paramTypes[(i<<1)+1] & 0x80) > 0

		switch tp {
		case TypeNull:
			args[i] = nil
			continue

		case TypeTiny:
			if len(paramValues) < (pos + 1) {
				err = ErrMalformPacket
				return
			}

			if isUnsigned {
				args[i] = uint64(paramValues[pos])
			} else {
				args[i] = int64(paramValues[pos])
			}

			pos++
			continue

		case TypeShort, TypeYear:
			if len(paramValues) < (pos + 2) {
				err = ErrMalformPacket
				return
			}

			if isUnsigned {
				args[i] = uint64(binary.LittleEndian.Uint16(paramValues[pos : pos+2]))
			} else {
				args[i] = int64((binary.LittleEndian.Uint16(paramValues[pos : pos+2])))
			}
			pos += 2
			continue

		case TypeInt24, TypeLong:
			if len(paramValues) < (pos + 4) {
				err = ErrMalformPacket
				return
			}

			if isUnsigned {
				args[i] = uint64(binary.LittleEndian.Uint32(paramValues[pos : pos+4]))
			} else {
				args[i] = int64(binary.LittleEndian.Uint32(paramValues[pos : pos+4]))
			}
			pos += 4
			continue

		case TypeLonglong:
			if len(paramValues) < (pos + 8) {
				err = ErrMalformPacket
				return
			}

			if isUnsigned {
				args[i] = binary.LittleEndian.Uint64(paramValues[pos : pos+8])
			} else {
				args[i] = int64(binary.LittleEndian.Uint64(paramValues[pos : pos+8]))
			}
			pos += 8
			continue

		case TypeFloat:
			if len(paramValues) < (pos + 4) {
				err = ErrMalformPacket
				return
			}

			args[i] = float64(math.Float32frombits(binary.LittleEndian.Uint32(paramValues[pos : pos+4])))
			pos += 4
			continue

		case TypeDouble:
			if len(paramValues) < (pos + 8) {
				err = ErrMalformPacket
				return
			}

			args[i] = math.Float64frombits(binary.LittleEndian.Uint64(paramValues[pos : pos+8]))
			pos += 8
			continue

		case TypeDecimal, TypeNewDecimal, TypeVarchar,
			TypeBit, TypeEnum, TypeSet, TypeTinyBlob,
			TypeMediumBlob, TypeLongBlob, TypeBlob,
			TypeVarString, TypeString, TypeGeometry,
			TypeDate, TypeNewDate,
			TypeTimestamp, TypeDatetime, TypeTime:
			if len(paramValues) < (pos + 1) {
				err = ErrMalformPacket
				return
			}

			v, isNull, n, err = parseLengthEncodedBytes(paramValues[pos:])
			pos += n
			if err != nil {
				return
			}

			if !isNull {
				args[i] = v
				continue
			} else {
				args[i] = nil
				continue
			}
		default:
			err = fmt.Errorf("Stmt Unknown FieldType %d", tp)
			return
		}
	}
	return
}

func (cc *ClientConn) handleStmtClose(data []byte) (err error) {
	if len(data) < 4 {
		return
	}

	stmtId := int(binary.LittleEndian.Uint32(data[0:4]))
	stmt := cc.ctx.GetStatement(stmtId)
	if stmt != nil {
		stmt.Close()
	}
	return
}

func (cc *ClientConn) handleStmtSendLongData(data []byte) (err error) {
	if len(data) < 6 {
		return ErrMalformPacket
	}

	stmtId := int(binary.LittleEndian.Uint32(data[0:4]))

	stmt := cc.ctx.GetStatement(stmtId)
	if stmt == nil {
		return NewDefaultError(ER_UNKNOWN_STMT_HANDLER,
			strconv.Itoa(stmtId), "stmt_send_longdata")
	}

	paramId := int(binary.LittleEndian.Uint16(data[4:6]))
	return stmt.AppendParam(paramId, data[6:])
}

func (cc *ClientConn) handleStmtReset(data []byte) (err error) {
	if len(data) < 4 {
		return ErrMalformPacket
	}

	stmtId := int(binary.LittleEndian.Uint32(data[0:4]))
	stmt := cc.ctx.GetStatement(stmtId)
	if stmt == nil {
		return NewDefaultError(ER_UNKNOWN_STMT_HANDLER,
			strconv.Itoa(stmtId), "stmt_reset")
	}
	stmt.Reset()
	return cc.writeOK()
}

// reserveBuffer checks cap(buf) and expand buffer to len(buf) + appendSize.
// If cap(buf) is not enough, reallocate new buffer.
func reserveBuffer(buf []byte, appendSize int) []byte {
	newSize := len(buf) + appendSize
	if cap(buf) < newSize {
		// Grow buffer exponentially
		newBuf := make([]byte, len(buf)*2+appendSize)
		copy(newBuf, buf)
		buf = newBuf
	}
	return buf[:newSize]
}

// escapeBytesBackslash escapes []byte with backslashes (\)
// This escapes the contents of a string (provided as []byte) by adding backslashes before special
// characters, and turning others into specific escape sequences, such as
// turning newlines into \n and null bytes into \0.
// https://github.com/mysql/mysql-server/blob/mysql-5.7.5/mysys/charset.c#L823-L932
func escapeBackslash(buf, v []byte) []byte {
	pos := len(buf)
	buf = reserveBuffer(buf, len(v)*2)

	for _, c := range v {
		switch c {
		case '\x00':
			buf[pos] = '\\'
			buf[pos+1] = '0'
			pos += 2
		case '\n':
			buf[pos] = '\\'
			buf[pos+1] = 'n'
			pos += 2
		case '\r':
			buf[pos] = '\\'
			buf[pos+1] = 'r'
			pos += 2
		case '\x1a':
			buf[pos] = '\\'
			buf[pos+1] = 'Z'
			pos += 2
		case '\'':
			buf[pos] = '\\'
			buf[pos+1] = '\''
			pos += 2
		case '"':
			buf[pos] = '\\'
			buf[pos+1] = '"'
			pos += 2
		case '\\':
			buf[pos] = '\\'
			buf[pos+1] = '\\'
			pos += 2
		default:
			buf[pos] = c
			pos += 1
		}
	}

	return buf[:pos]
}

// escapeBytesQuotes escapes apostrophes in []byte by doubling them up.
// This escapes the contents of a string by doubling up any apostrophes that
// it contains. This is used when the NO_BACKSLASH_ESCAPES SQL_MODE is in
// effect on the server.
// https://github.com/mysql/mysql-server/blob/mysql-5.7.5/mysys/charset.c#L963-L1038
func escapeQuotes(buf, v []byte) []byte {
	pos := len(buf)
	buf = reserveBuffer(buf, len(v)*2)

	for _, c := range v {
		if c == '\'' {
			buf[pos] = '\''
			buf[pos+1] = '\''
			pos += 2
		} else {
			buf[pos] = c
			pos++
		}
	}

	return buf[:pos]
}

func interpolateParams(query string, noBackslashEscapes bool, args ...interface{}) (s string) {
	var buf []byte
	argPos := 0

	for i := 0; i < len(query); i++ {
		q := strings.IndexByte(query[i:], '?') //TODO handle the case when "?" in quotes.
		if q == -1 {
			buf = append(buf, query[i:]...)
			break
		}
		buf = append(buf, query[i:i+q]...)
		i += q

		arg := args[argPos]
		argPos++

		if arg == nil {
			buf = append(buf, "NULL"...)
			continue
		}

		switch v := arg.(type) {
		case uint:
			buf = strconv.AppendUint(buf, uint64(v), 10)
		case int:
			buf = strconv.AppendInt(buf, int64(v), 10)
		case uint64:
			buf = strconv.AppendUint(buf, v, 10)
		case int64:
			buf = strconv.AppendInt(buf, v, 10)
		case float64:
			buf = strconv.AppendFloat(buf, v, 'g', -1, 64)
		case bool:
			if v {
				buf = append(buf, '1')
			} else {
				buf = append(buf, '0')
			}
		case []byte:
			if v == nil {
				buf = append(buf, "''"...) //empty string
			} else {
				buf = append(buf, '\'')
				if noBackslashEscapes {
					buf = escapeQuotes(buf, v)
				} else {
					buf = escapeBackslash(buf, v)
				}
				buf = append(buf, '\'')
			}
		case string:
			buf = append(buf, '\'')
			if noBackslashEscapes {
				buf = escapeQuotes(buf, hack.Slice(v))
			} else {
				buf = escapeBackslash(buf, hack.Slice(v))
			}
			buf = append(buf, '\'')
		default:
			log.Debug("unkonw type", reflect.TypeOf(arg))
			return ""
		}
	}
	return string(buf)
}
