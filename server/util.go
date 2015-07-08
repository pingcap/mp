package server

import (
	"crypto/rand"
	"crypto/sha1"
	"encoding/binary"
	"fmt"
	"io"
	"math"
	"runtime"
	"strconv"

	"github.com/ngaut/arena"
	"github.com/pingcap/mp/hack"
	. "github.com/pingcap/mp/protocol"
)

func Pstack() string {
	buf := make([]byte, 1024)
	n := runtime.Stack(buf, false)
	return string(buf[0:n])
}

func CalcPassword(scramble, password []byte) []byte {
	if len(password) == 0 {
		return nil
	}

	// stage1Hash = SHA1(password)
	crypt := sha1.New()
	crypt.Write(password)
	stage1 := crypt.Sum(nil)

	// scrambleHash = SHA1(scramble + SHA1(stage1Hash))
	// inner Hash
	crypt.Reset()
	crypt.Write(stage1)
	hash := crypt.Sum(nil)

	// outer Hash
	crypt.Reset()
	crypt.Write(scramble)
	crypt.Write(hash)
	scramble = crypt.Sum(nil)

	// token = scrambleHash XOR stage1Hash
	for i := range scramble {
		scramble[i] ^= stage1[i]
	}
	return scramble
}

func RandomBuf(size int) ([]byte, error) {
	buf := make([]byte, size)

	if _, err := io.ReadFull(rand.Reader, buf); err != nil {
		return nil, err
	}

	// avoid to generate '\0'
	for i, b := range buf {
		if uint8(b) == 0 {
			buf[i] = '0'
		}
	}

	return buf, nil
}

func LengthEncodedInt(b []byte) (num uint64, isNull bool, n int) {
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

func PutLengthEncodedInt(n uint64) []byte {
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

func LengthEncodedBytes(b []byte) ([]byte, bool, int, error) {
	// Get length
	num, isNull, n := LengthEncodedInt(b)
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

func LengthEncodedString(b []byte) (string, bool, int, error) {
	// Get length
	num, isNull, n := LengthEncodedInt(b)
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

func SkipLengthEnodedString(b []byte) (int, error) {
	// Get length
	num, _, n := LengthEncodedInt(b)
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

func PutLengthEncodedString(b []byte, alloc arena.ArenaAllocator) []byte {
	data := alloc.AllocBytes(len(b) + 9)
	data = append(data, PutLengthEncodedInt(uint64(len(b)))...)
	data = append(data, b...)
	return data
}

func Uint16ToBytes(n uint16) []byte {
	return []byte{
		byte(n),
		byte(n >> 8),
	}
}

func Uint32ToBytes(n uint32) []byte {
	return []byte{
		byte(n),
		byte(n >> 8),
		byte(n >> 16),
		byte(n >> 24),
	}
}

func Uint64ToBytes(n uint64) []byte {
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

func FormatBinaryDate(n int, data []byte) ([]byte, error) {
	switch n {
	case 0:
		return []byte("0000-00-00"), nil
	case 4:
		return []byte(fmt.Sprintf("%04d-%02d-%02d",
			binary.LittleEndian.Uint16(data[:2]),
			data[2],
			data[3])), nil
	default:
		return nil, fmt.Errorf("invalid date packet length %d", n)
	}
}

func FormatBinaryDateTime(n int, data []byte) ([]byte, error) {
	switch n {
	case 0:
		return []byte("0000-00-00 00:00:00"), nil
	case 4:
		return []byte(fmt.Sprintf("%04d-%02d-%02d 00:00:00",
			binary.LittleEndian.Uint16(data[:2]),
			data[2],
			data[3])), nil
	case 7:
		return []byte(fmt.Sprintf(
			"%04d-%02d-%02d %02d:%02d:%02d",
			binary.LittleEndian.Uint16(data[:2]),
			data[2],
			data[3],
			data[4],
			data[5],
			data[6])), nil
	case 11:
		return []byte(fmt.Sprintf(
			"%04d-%02d-%02d %02d:%02d:%02d.%06d",
			binary.LittleEndian.Uint16(data[:2]),
			data[2],
			data[3],
			data[4],
			data[5],
			data[6],
			binary.LittleEndian.Uint32(data[7:11]))), nil
	default:
		return nil, fmt.Errorf("invalid datetime packet length %d", n)
	}
}

func FormatBinaryTime(n int, data []byte) ([]byte, error) {
	if n == 0 {
		return []byte("0000-00-00"), nil
	}

	var sign byte
	if data[0] == 1 {
		sign = byte('-')
	}

	switch n {
	case 8:
		return []byte(fmt.Sprintf(
			"%c%02d:%02d:%02d",
			sign,
			uint16(data[1])*24+uint16(data[5]),
			data[6],
			data[7],
		)), nil
	case 12:
		return []byte(fmt.Sprintf(
			"%c%02d:%02d:%02d.%06d",
			sign,
			uint16(data[1])*24+uint16(data[5]),
			data[6],
			data[7],
			binary.LittleEndian.Uint32(data[8:12]),
		)), nil
	default:
		return nil, fmt.Errorf("invalid time packet length %d", n)
	}
}

var (
	DONTESCAPE   = byte(255)
	EncodeMap    [256]byte
	tinyIntCache [251][]byte
)

/*
func Escape(sql string) string {
	dest := make([]byte, 0, 2*len(sql))

	for i, w := 0, 0; i < len(sql); i += w {
		runeValue, width := utf8.DecodeRuneInString(sql[i:])
		if c := EncodeMap[byte(runeValue)]; c == DONTESCAPE {
			dest = append(dest, sql[i:i+width]...)
		} else {
			dest = append(dest, '\\', c)
		}
		w = width
	}

	return string(dest)
}
*/

var encodeRef = map[byte]byte{
	'\x00': '0',
	'\'':   '\'',
	'"':    '"',
	'\b':   'b',
	'\n':   'n',
	'\r':   'r',
	'\t':   't',
	26:     'Z', // ctl-Z
	'\\':   '\\',
}

var defCache []byte

func init() {
	for i := 0; i < len(tinyIntCache); i++ {
		tinyIntCache[i] = []byte{byte(i)}
	}

	for i := range EncodeMap {
		EncodeMap[i] = DONTESCAPE
	}

	for i := range EncodeMap {
		if to, ok := encodeRef[byte(i)]; ok {
			EncodeMap[byte(i)] = to
		}
	}

	defCache = PutLengthEncodedString([]byte("def"), arena.StdAllocator)
}

func ParseRowValuesBinary(columns []*ColumnInfo, rowData []byte) ([]interface{}, error) {
	values := make([]interface{}, len(columns))
	if rowData[0] != OK_HEADER {
		return nil, ErrMalformPacket
	}

	pos := 1 + ((len(columns) + 7 + 2) >> 3)

	nullBitmap := rowData[1:pos]

	var isUnsigned bool
	var isNull bool
	var err error
	var n int
	var v []byte
	for i := range values {
		if nullBitmap[(i+2)/8]&(1<<(uint(i+2)%8)) > 0 {
			values[i] = nil
			continue
		}

		isUnsigned = columns[i].Flag&UNSIGNED_FLAG > 0

		switch columns[i].Type {
		case MYSQL_TYPE_NULL:
			values[i] = nil
			continue

		case MYSQL_TYPE_TINY:
			if isUnsigned {
				values[i] = uint64(rowData[pos])
			} else {
				values[i] = int64(rowData[pos])
			}
			pos++
			continue

		case MYSQL_TYPE_SHORT, MYSQL_TYPE_YEAR:
			if isUnsigned {
				values[i] = uint64(binary.LittleEndian.Uint16(rowData[pos : pos+2]))
			} else {
				values[i] = int64((binary.LittleEndian.Uint16(rowData[pos : pos+2])))
			}
			pos += 2
			continue

		case MYSQL_TYPE_INT24, MYSQL_TYPE_LONG:
			if isUnsigned {
				values[i] = uint64(binary.LittleEndian.Uint32(rowData[pos : pos+4]))
			} else {
				values[i] = int64(binary.LittleEndian.Uint32(rowData[pos : pos+4]))
			}
			pos += 4
			continue

		case MYSQL_TYPE_LONGLONG:
			if isUnsigned {
				values[i] = binary.LittleEndian.Uint64(rowData[pos : pos+8])
			} else {
				values[i] = int64(binary.LittleEndian.Uint64(rowData[pos : pos+8]))
			}
			pos += 8
			continue

		case MYSQL_TYPE_FLOAT:
			values[i] = float64(math.Float32frombits(binary.LittleEndian.Uint32(rowData[pos : pos+4])))
			pos += 4
			continue

		case MYSQL_TYPE_DOUBLE:
			values[i] = math.Float64frombits(binary.LittleEndian.Uint64(rowData[pos : pos+8]))
			pos += 8
			continue

		case MYSQL_TYPE_DECIMAL, MYSQL_TYPE_NEWDECIMAL, MYSQL_TYPE_VARCHAR,
			MYSQL_TYPE_BIT, MYSQL_TYPE_ENUM, MYSQL_TYPE_SET, MYSQL_TYPE_TINY_BLOB,
			MYSQL_TYPE_MEDIUM_BLOB, MYSQL_TYPE_LONG_BLOB, MYSQL_TYPE_BLOB,
			MYSQL_TYPE_VAR_STRING, MYSQL_TYPE_STRING, MYSQL_TYPE_GEOMETRY:
			v, isNull, n, err = LengthEncodedBytes(rowData[pos:])
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
		case MYSQL_TYPE_DATE, MYSQL_TYPE_NEWDATE:
			var num uint64
			num, isNull, n = LengthEncodedInt(rowData[pos:])

			pos += n

			if isNull {
				values[i] = nil
				continue
			}

			values[i], err = FormatBinaryDate(int(num), rowData[pos:])
			pos += int(num)

			if err != nil {
				return nil, err
			}

		case MYSQL_TYPE_TIMESTAMP, MYSQL_TYPE_DATETIME:
			var num uint64
			num, isNull, n = LengthEncodedInt(rowData[pos:])

			pos += n

			if isNull {
				values[i] = nil
				continue
			}

			values[i], err = FormatBinaryDateTime(int(num), rowData[pos:])
			pos += int(num)

			if err != nil {
				return nil, err
			}

		case MYSQL_TYPE_TIME:
			var num uint64
			num, isNull, n = LengthEncodedInt(rowData[pos:])

			pos += n

			if isNull {
				values[i] = nil
				continue
			}

			values[i], err = FormatBinaryTime(int(num), rowData[pos:])
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

func ParseRowValuesText(columns []*ColumnInfo, rowData []byte) (values []interface{}, err error) {
	values = make([]interface{}, len(columns))
	var v []byte
	var isNull, isUnsigned bool
	var pos int = 0
	var n int = 0
	for i, col := range columns {
		v, isNull, n, err = LengthEncodedBytes(rowData[pos:])
		if err != nil {
			return nil, err
		}

		pos += n

		if isNull {
			values[i] = nil
		} else {
			isUnsigned = (col.Flag&UNSIGNED_FLAG > 0)

			switch col.Type {
			case MYSQL_TYPE_TINY, MYSQL_TYPE_SHORT, MYSQL_TYPE_INT24,
				MYSQL_TYPE_LONGLONG, MYSQL_TYPE_YEAR:
				if isUnsigned {
					values[i], err = strconv.ParseUint(string(v), 10, 64)
				} else {
					values[i], err = strconv.ParseInt(string(v), 10, 64)
				}
			case MYSQL_TYPE_FLOAT, MYSQL_TYPE_DOUBLE:
				values[i], err = strconv.ParseFloat(string(v), 64)
			default:
				values[i] = v
			}

			if err != nil {
				return nil, err
			}
		}
	}
	return
}

func EncodeRowValuesBinary(columns []*ColumnInfo, row []interface{}) (data []byte, err error) {
	if len(columns) != len(row) {
		err = ErrMalformPacket
		return
	}
	data = append(data, OK_HEADER)
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
	//TODO
	//	for i, val := range row {
	//		isUnsigned := columns[i].Flag&UNSIGNED_FLAG > 0
	//
	//
	//		switch val.(type) {
	//		case int, int64:
	//
	//		}
	//	}
	return
}
