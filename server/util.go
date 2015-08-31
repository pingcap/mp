package server

import (
	"encoding/binary"
	"fmt"
	"io"
	"math"
	"strconv"
	"time"

	"github.com/juju/errors"
	"github.com/ngaut/arena"
	"github.com/pingcap/mp/hack"
	. "github.com/pingcap/mysqldef"
)

func parseLengthEncodedInt(b []byte) (num uint64, isNull bool, n int) {
	switch b[0] {
	// 251: NULL
	case 0xfb:
		n = 1
		isNull = true
		return

	// 252: value of following 2
	case 0xfc:
		num = uint64(b[1]) | uint64(b[2])<<8
		n = 3
		return

	// 253: value of following 3
	case 0xfd:
		num = uint64(b[1]) | uint64(b[2])<<8 | uint64(b[3])<<16
		n = 4
		return

	// 254: value of following 8
	case 0xfe:
		num = uint64(b[1]) | uint64(b[2])<<8 | uint64(b[3])<<16 |
			uint64(b[4])<<24 | uint64(b[5])<<32 | uint64(b[6])<<40 |
			uint64(b[7])<<48 | uint64(b[8])<<56
		n = 9
		return
	}

	// 0-250: value of first byte
	num = uint64(b[0])
	n = 1
	return
}

func dumpLengthEncodedInt(n uint64) []byte {
	switch {
	case n <= 250:
		return tinyIntCache[n]

	case n <= 0xffff:
		return []byte{0xfc, byte(n), byte(n >> 8)}

	case n <= 0xffffff:
		return []byte{0xfd, byte(n), byte(n >> 8), byte(n >> 16)}

	case n <= 0xffffffffffffffff:
		return []byte{0xfe, byte(n), byte(n >> 8), byte(n >> 16), byte(n >> 24),
			byte(n >> 32), byte(n >> 40), byte(n >> 48), byte(n >> 56)}
	}

	return nil
}

func parseLengthEncodedBytes(b []byte) ([]byte, bool, int, error) {
	// Get length
	num, isNull, n := parseLengthEncodedInt(b)
	if num < 1 {
		return nil, isNull, n, nil
	}

	n += int(num)

	// Check data length
	if len(b) >= n {
		return b[n-int(num) : n], false, n, nil
	}

	return nil, false, n, io.EOF
}

func parseLengthEncodedString(b []byte) (string, bool, int, error) {
	// Get length
	num, isNull, n := parseLengthEncodedInt(b)
	if num < 1 {
		return "", isNull, n, nil
	}

	n += int(num)

	// Check data length
	if len(b) >= n {
		return hack.String(b[n-int(num) : n]), false, n, nil
	}

	return "", false, n, io.EOF
}

func skipLengthEnodedString(b []byte) (int, error) {
	// Get length
	num, _, n := parseLengthEncodedInt(b)
	if num < 1 {
		return n, nil
	}

	n += int(num)

	// Check data length
	if len(b) >= n {
		return n, nil
	}
	return n, io.EOF
}

func dumpLengthEncodedString(b []byte, alloc arena.ArenaAllocator) []byte {
	data := alloc.AllocBytes(len(b) + 9)
	data = append(data, dumpLengthEncodedInt(uint64(len(b)))...)
	data = append(data, b...)
	return data
}

func dumpUint16(n uint16) []byte {
	return []byte{
		byte(n),
		byte(n >> 8),
	}
}

func dumpUint32(n uint32) []byte {
	return []byte{
		byte(n),
		byte(n >> 8),
		byte(n >> 16),
		byte(n >> 24),
	}
}

func dumpUint64(n uint64) []byte {
	return []byte{
		byte(n),
		byte(n >> 8),
		byte(n >> 16),
		byte(n >> 24),
		byte(n >> 32),
		byte(n >> 40),
		byte(n >> 48),
		byte(n >> 56),
	}
}

var tinyIntCache [251][]byte

func init() {
	for i := 0; i < len(tinyIntCache); i++ {
		tinyIntCache[i] = []byte{byte(i)}
	}
}

func parseBinaryTime(n int, data []byte) (dur time.Duration, err error) {
	if n == 0 {
		return
	}
	var sign time.Duration = 1
	if data[0] == 1 {
		sign = -1
	}
	switch n {
	case 8:
		dur = time.Duration(data[1])*24*time.Hour + time.Duration(data[5])*time.Hour +
			time.Duration(data[6])*time.Minute + time.Duration(data[7])*time.Second
	case 12:
		dur = time.Duration(data[1])*24*time.Hour + time.Duration(data[5])*time.Hour +
			time.Duration(data[6])*time.Minute + time.Duration(data[7])*time.Second +
			time.Duration(binary.LittleEndian.Uint32(data[8:12]))*time.Microsecond
	default:
		err = fmt.Errorf("invalid time packet length %d", n)
	}

	dur *= sign
	return
}

func dumpBinaryTime(dur time.Duration) (data []byte) {
	if dur == 0 {
		data = tinyIntCache[0]
		return
	}
	data = make([]byte, 13)
	data[0] = 12
	if dur < 0 {
		data[1] = 1
		dur = -dur
	}
	days := dur / (24 * time.Hour)
	dur -= days * 24 * time.Hour
	data[2] = byte(days)
	hours := dur / time.Hour
	dur -= hours * time.Hour
	data[6] = byte(hours)
	minutes := dur / time.Minute
	dur -= minutes * time.Minute
	data[7] = byte(minutes)
	seconds := dur / time.Second
	dur -= seconds * time.Second
	data[8] = byte(seconds)
	if dur == 0 {
		data[0] = 8
		return data[:9]
	}
	binary.LittleEndian.PutUint32(data[9:13], uint32(dur/time.Microsecond))
	return
}

// Mysql Timestamp type is time zone dependent, other date time types are not.
func parseBinaryDateTime(num int, data []byte, mysqlType uint8, loc *time.Location) (t time.Time, err error) {
	if loc == nil || mysqlType != TypeTimestamp {
		loc = time.Local
	}
	switch num {
	case 0:
		t = time.Time{}
	case 4:
		t = time.Date(
			int(binary.LittleEndian.Uint16(data[:2])), // year
			time.Month(data[2]),                       // month
			int(data[3]),                              // day
			0, 0, 0, 0,
			loc,
		)
	case 7:
		t = time.Date(
			int(binary.LittleEndian.Uint16(data[:2])), // year
			time.Month(data[2]),                       // month
			int(data[3]),                              // day
			int(data[4]),                              // hour
			int(data[5]),                              // minutes
			int(data[6]),                              // seconds
			0,
			loc,
		)
	case 11:
		t = time.Date(
			int(binary.LittleEndian.Uint16(data[:2])), // year
			time.Month(data[2]),                       // month
			int(data[3]),                              // day
			int(data[4]),                              // hour
			int(data[5]),                              // minutes
			int(data[6]),                              // seconds
			int(binary.LittleEndian.Uint32(data[7:11]))*1000, // nanoseconds
			loc,
		)
	default:
		err = fmt.Errorf("Invalid DATETIME-packet length %d", num)
	}
	return
}

func dumpBinaryDateTime(t Time, loc *time.Location) (data []byte) {
	if t.Type == TypeTimestamp && loc != nil {
		t.Time = t.In(loc)
	}
	switch t.Type {
	case TypeTimestamp, TypeDatetime:
		data = append(data, 11)
		data = append(data, dumpUint16(uint16(t.Year()))...) //year
		data = append(data, byte(t.Month()), byte(t.Day()), byte(t.Hour()), byte(t.Minute()), byte(t.Second()))
		data = append(data, dumpUint32(uint32((t.Nanosecond() / 1000)))...)
	case TypeDate, TypeNewDate:
		data = append(data, 4)
		data = append(data, dumpUint16(uint16(t.Year()))...) //year
		data = append(data, byte(t.Month()), byte(t.Day()))
	}
	return
}

func parseRowValuesBinary(columns []*ColumnInfo, rowData []byte) ([]interface{}, error) {
	values := make([]interface{}, len(columns))
	if rowData[0] != OKHeader {
		return nil, ErrMalformPacket
	}

	pos := 1 + ((len(columns) + 7 + 2) >> 3)

	nullBitmap := rowData[1:pos]
	var isNull bool
	var err error
	var n int
	var v []byte
	for i := range values {
		if nullBitmap[(i+2)/8]&(1<<(uint(i+2)%8)) > 0 {
			values[i] = nil
			continue
		}

		switch columns[i].Type {
		case TypeNull:
			values[i] = nil
			continue

		case TypeTiny:
			values[i] = int64(rowData[pos])
			pos++
			continue
		case TypeShort, TypeYear:
			values[i] = int64((binary.LittleEndian.Uint16(rowData[pos : pos+2])))
			pos += 2
			continue

		case TypeInt24, TypeLong:
			values[i] = int64(binary.LittleEndian.Uint32(rowData[pos : pos+4]))
			pos += 4
			continue

		case TypeLonglong:
			values[i] = int64(binary.LittleEndian.Uint64(rowData[pos : pos+8]))
			pos += 8
			continue

		case TypeFloat:
			values[i] = float64(math.Float32frombits(binary.LittleEndian.Uint32(rowData[pos : pos+4])))
			pos += 4
			continue

		case TypeDouble:
			values[i] = math.Float64frombits(binary.LittleEndian.Uint64(rowData[pos : pos+8]))
			pos += 8
			continue

		case TypeDecimal, TypeNewDecimal, TypeVarchar,
			TypeBit, TypeEnum, TypeSet, TypeTinyBlob,
			TypeMediumBlob, TypeLongBlob, TypeBlob,
			TypeVarString, TypeString, TypeGeometry:
			v, isNull, n, err = parseLengthEncodedBytes(rowData[pos:])
			pos += n
			if err != nil {
				return nil, err
			}

			if !isNull {
				values[i] = v
				continue
			} else {
				values[i] = nil
				continue
			}
		case TypeDate, TypeNewDate, TypeDatetime, TypeTimestamp:
			var num uint64
			num, isNull, n = parseLengthEncodedInt(rowData[pos:])

			pos += n

			if isNull {
				values[i] = nil
				continue
			}
			values[i], err = parseBinaryDateTime(int(num), rowData[pos:], columns[i].Type, nil)
			pos += int(num)

			if err != nil {
				return nil, err
			}
			continue
		case TypeDuration:
			var num uint64
			num, isNull, n = parseLengthEncodedInt(rowData[pos:])

			pos += n

			if isNull {
				values[i] = nil
				continue
			}

			values[i], err = parseBinaryTime(int(num), rowData[pos:])
			pos += int(num)

			if err != nil {
				return nil, err
			}
		default:
			return nil, fmt.Errorf("Stmt Unknown FieldType %d %s", columns[i].Type, columns[i].Name)
		}
	}
	return values, err
}

func uniformValue(value interface{}) interface{} {
	switch v := value.(type) {
	case int8:
		return int64(v)
	case int16:
		return int64(v)
	case int32:
		return int64(v)
	case int64:
		return int64(v)
	case uint8:
		return uint64(v)
	case uint16:
		return uint64(v)
	case uint32:
		return uint64(v)
	case uint64:
		return uint64(v)
	default:
		return value
	}
}

func dumpRowValuesBinary(alloc arena.ArenaAllocator, columns []*ColumnInfo, row []interface{}) (data []byte, err error) {
	if len(columns) != len(row) {
		err = ErrMalformPacket
		return
	}
	data = append(data, OKHeader)
	nullsLen := ((len(columns) + 7 + 2) / 8)
	nulls := make([]byte, nullsLen)
	for i, val := range row {
		if val == nil {
			byte_pos := (i + 2) / 8
			bit_pos := byte((i + 2) % 8)
			nulls[byte_pos] |= 1 << bit_pos
		}
	}
	data = append(data, nulls...)
	for i, val := range row {
		val = uniformValue(val)
		switch v := val.(type) {
		case int64:
			switch columns[i].Type {
			case TypeTiny:
				data = append(data, byte(v))
			case TypeShort, TypeYear:
				data = append(data, dumpUint16(uint16(v))...)
			case TypeInt24, TypeLong:
				data = append(data, dumpUint32(uint32(v))...)
			case TypeLonglong:
				data = append(data, dumpUint64(uint64(v))...)
			}
		case uint64:
			switch columns[i].Type {
			case TypeTiny:
				data = append(data, byte(v))
			case TypeShort, TypeYear:
				data = append(data, dumpUint16(uint16(v))...)
			case TypeInt24, TypeLong:
				data = append(data, dumpUint32(uint32(v))...)
			case TypeLonglong:
				data = append(data, dumpUint64(uint64(v))...)
			}
		case float32:
			floatBits := math.Float32bits(float32(val.(float64)))
			data = append(data, dumpUint32(floatBits)...)
		case float64:
			floatBits := math.Float64bits(val.(float64))
			data = append(data, dumpUint64(floatBits)...)
		case string:
			data = append(data, dumpLengthEncodedString(hack.Slice(v), alloc)...)
		case []byte:
			data = append(data, dumpLengthEncodedString(v, alloc)...)
		case Time:
			data = append(data, dumpBinaryDateTime(v, nil)...)
		case time.Time:
			myTime := Time{v, columns[i].Type, DefaultFsp}
			data = append(data, dumpBinaryDateTime(myTime, nil)...)
		case Duration:
			data = append(data, dumpBinaryTime(v.Duration)...)
		case time.Duration:
			data = append(data, dumpBinaryTime(v)...)
		case Decimal:
			data = append(data, dumpLengthEncodedString(hack.Slice(v.String()), alloc)...)
		}
	}
	return
}

func parseRowValuesText(columns []*ColumnInfo, rowData []byte) (values []interface{}, err error) {
	values = make([]interface{}, len(columns))
	var v []byte
	var isNull, isUnsigned bool
	var pos int = 0
	var n int = 0
	for i, col := range columns {
		v, isNull, n, err = parseLengthEncodedBytes(rowData[pos:])
		if err != nil {
			return nil, errors.Trace(err)
		}

		pos += n

		if isNull {
			values[i] = nil
		} else {
			isUnsigned = (col.Flag&UnsignedFlag > 0)

			switch col.Type {
			case TypeTiny, TypeShort, TypeInt24, TypeLonglong:
				if isUnsigned {
					values[i], err = strconv.ParseUint(hack.String(v), 10, 64)
				} else {
					values[i], err = strconv.ParseInt(hack.String(v), 10, 64)
				}
			case TypeFloat, TypeDouble:
				values[i], err = strconv.ParseFloat(hack.String(v), 64)
			case TypeDate, TypeNewDate, TypeDatetime, TypeTimestamp, TypeDuration:
				values[i] = hack.String(v)
			case TypeYear:
				values[i], err = ParseYear(hack.String(v))
			case TypeString, TypeVarString, TypeVarchar:
				values[i] = hack.String(v)
			case TypeBlob, TypeLongBlob, TypeMediumBlob, TypeTinyBlob:
				if col.Charset != uint16(CharsetIds["binary"]) {
					values[i] = hack.String(v)
				} else {
					values[i] = v
				}
			default:
				values[i] = v
			}

			if err != nil {
				return nil, errors.Trace(err)
			}
		}
	}
	return
}

func dumpTextValue(mysqlType uint8, value interface{}) ([]byte, error) {
	switch v := value.(type) {
	case int8:
		return strconv.AppendInt(nil, int64(v), 10), nil
	case int16:
		return strconv.AppendInt(nil, int64(v), 10), nil
	case int32:
		return strconv.AppendInt(nil, int64(v), 10), nil
	case int64:
		return strconv.AppendInt(nil, int64(v), 10), nil
	case int:
		return strconv.AppendInt(nil, int64(v), 10), nil
	case uint8:
		return strconv.AppendUint(nil, uint64(v), 10), nil
	case uint16:
		return strconv.AppendUint(nil, uint64(v), 10), nil
	case uint32:
		return strconv.AppendUint(nil, uint64(v), 10), nil
	case uint64:
		return strconv.AppendUint(nil, uint64(v), 10), nil
	case uint:
		return strconv.AppendUint(nil, uint64(v), 10), nil
	case float32:
		return strconv.AppendFloat(nil, float64(v), 'f', -1, 64), nil
	case float64:
		return strconv.AppendFloat(nil, float64(v), 'f', -1, 64), nil
	case []byte:
		return v, nil
	case string:
		return hack.Slice(v), nil
	case Time:
		return hack.Slice(v.String()), nil
	case Duration:
		return hack.Slice(v.String()), nil
	case Decimal:
		return hack.Slice(v.String()), nil
	default:
		return nil, fmt.Errorf("invalid type %T", value)
	}
}
