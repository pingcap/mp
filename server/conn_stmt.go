package server

import (
	"encoding/binary"
	"strconv"
	"strings"

	"fmt"
	"github.com/pingcap/mp/hack"
	. "github.com/pingcap/mp/protocol"
	"math"
)

func (c *ClientConn) handleStmtPrepare(sql string) (err error) {

	stmt, err := c.ctx.Prepare(sql)
	if err != nil {
		return err
	}
	return c.writePrepare(stmt)
}

func (c *ClientConn) writePrepare(stmt Statement) error {
	data := make([]byte, 4, 128)

	columns := stmt.Columns()
	params := stmt.Params()

	//status ok
	data = append(data, 0)
	//stmt id
	data = append(data, Uint32ToBytes(uint32(stmt.ID()))...)
	//number columns
	data = append(data, Uint16ToBytes(uint16(len(columns)))...)
	//number params
	data = append(data, Uint16ToBytes(uint16(len(params)))...)
	//filter [00]
	data = append(data, 0)
	//warning count
	data = append(data, 0, 0)

	if err := c.writePacket(data); err != nil {
		return err
	}

	if len(params) > 0 {
		for i := 0; i < len(params); i++ {
			data = data[0:4]
			data = append(data, params[i].Dump(c.alloc)...)

			if err := c.writePacket(data); err != nil {
				return err
			}
		}

		if err := c.writeEOF(c.ctx.Status()); err != nil {
			return err
		}
	}

	if len(columns) > 0 {
		for i := 0; i < len(columns); i++ {
			data = data[0:4]
			data = append(data, columns[i].Dump(c.alloc)...)

			if err := c.writePacket(data); err != nil {
				return err
			}
		}

		if err := c.writeEOF(c.ctx.Status()); err != nil {
			return err
		}

	}
	return c.flush()
}

func (c *ClientConn) handleStmtExecute(data []byte) (err error) {
	if len(data) < 9 {
		return ErrMalformPacket
	}

	pos := 0
	stmtId := binary.LittleEndian.Uint32(data[0:4])
	pos += 4

	stmt := c.ctx.GetStatement(int(stmtId))
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

	var nullBitmaps []byte
	var paramTypes []byte
	var paramValues []byte

	numParams := len(stmt.Params())
	var args []interface{}
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

		args, err = c.parseStmtArgs(numParams, nullBitmaps, paramTypes, paramValues)
		if err != nil {
			return err
		}
	}

	rs, err := stmt.Execute(args...)
	if err != nil {
		return err
	}
	return c.writeResultset(rs, true)
}

func (c *ClientConn) parseStmtArgs(numParams int, nullBitmap, paramTypes, paramValues []byte) (args []interface{}, err error) {
	args = make([]interface{}, numParams)

	pos := 0

	var v []byte
	var n int = 0
	var isNull bool

	for i := 0; i < numParams; i++ {
		if nullBitmap[i>>3]&(1<<(uint(i)%8)) > 0 {
			args[i] = nil
			continue
		}

		tp := paramTypes[i<<1]
		isUnsigned := (paramTypes[(i<<1)+1] & 0x80) > 0

		switch tp {
		case MYSQL_TYPE_NULL:
			args[i] = nil
			continue

		case MYSQL_TYPE_TINY:
			if len(paramValues) < (pos + 1) {
				err = ErrMalformPacket
				return
			}

			if isUnsigned {
				args[i] = uint8(paramValues[pos])
			} else {
				args[i] = int8(paramValues[pos])
			}

			pos++
			continue

		case MYSQL_TYPE_SHORT, MYSQL_TYPE_YEAR:
			if len(paramValues) < (pos + 2) {
				err = ErrMalformPacket
				return
			}

			if isUnsigned {
				args[i] = uint16(binary.LittleEndian.Uint16(paramValues[pos : pos+2]))
			} else {
				args[i] = int16((binary.LittleEndian.Uint16(paramValues[pos : pos+2])))
			}
			pos += 2
			continue

		case MYSQL_TYPE_INT24, MYSQL_TYPE_LONG:
			if len(paramValues) < (pos + 4) {
				err = ErrMalformPacket
				return
			}

			if isUnsigned {
				args[i] = uint32(binary.LittleEndian.Uint32(paramValues[pos : pos+4]))
			} else {
				args[i] = int32(binary.LittleEndian.Uint32(paramValues[pos : pos+4]))
			}
			pos += 4
			continue

		case MYSQL_TYPE_LONGLONG:
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

		case MYSQL_TYPE_FLOAT:
			if len(paramValues) < (pos + 4) {
				err = ErrMalformPacket
				return
			}

			args[i] = float32(math.Float32frombits(binary.LittleEndian.Uint32(paramValues[pos : pos+4])))
			pos += 4
			continue

		case MYSQL_TYPE_DOUBLE:
			if len(paramValues) < (pos + 8) {
				err = ErrMalformPacket
				return
			}

			args[i] = math.Float64frombits(binary.LittleEndian.Uint64(paramValues[pos : pos+8]))
			pos += 8
			continue

		case MYSQL_TYPE_DECIMAL, MYSQL_TYPE_NEWDECIMAL, MYSQL_TYPE_VARCHAR,
			MYSQL_TYPE_BIT, MYSQL_TYPE_ENUM, MYSQL_TYPE_SET, MYSQL_TYPE_TINY_BLOB,
			MYSQL_TYPE_MEDIUM_BLOB, MYSQL_TYPE_LONG_BLOB, MYSQL_TYPE_BLOB,
			MYSQL_TYPE_VAR_STRING, MYSQL_TYPE_STRING, MYSQL_TYPE_GEOMETRY,
			MYSQL_TYPE_DATE, MYSQL_TYPE_NEWDATE,
			MYSQL_TYPE_TIMESTAMP, MYSQL_TYPE_DATETIME, MYSQL_TYPE_TIME:
			if len(paramValues) < (pos + 1) {
				err = ErrMalformPacket
				return
			}

			v, isNull, n, err = LengthEncodedBytes(paramValues[pos:])
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

func (c *ClientConn) handleStmtClose(data []byte) (err error) {
	return c.writeError(nil)
}

func (c *ClientConn) handleStmtSendLongData(data []byte) (err error) {
	return c.writeError(nil)
}

func (c *ClientConn) handleStmtReset(data []byte) (err error) {
	return c.writeError(nil)
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
		case int:
			buf = strconv.AppendInt(buf, int64(v), 10)
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
				buf = append(buf, "NULL"...)
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
			return ""
		}
	}
	return string(buf)
}
