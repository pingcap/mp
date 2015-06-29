package server

import (
	"fmt"
	"github.com/juju/errors"
	"github.com/ngaut/arena"
	"github.com/pingcap/mp/hack"
	. "github.com/pingcap/mp/protocol"
	"github.com/pingcap/ql"
	"strconv"
)

type Result struct {
	Status       uint16
	InsertId     uint64
	AffectedRows uint64
}

func (c *Conn) dumpField(field string, alloc arena.ArenaAllocator) []byte {
	tableName := []byte("test")
	fieldName := []byte(field)
	defaultValue := []byte("")
	l := len(c.db.Name()) + len(tableName) + len(tableName) + len(fieldName) + len(fieldName) + len(defaultValue) + 48

	data := make([]byte, 0, l)

	data = append(data, PutLengthEncodedString([]byte("def"), alloc)...)

	data = append(data, PutLengthEncodedString([]byte(c.db.Name()), alloc)...)

	data = append(data, PutLengthEncodedString(tableName, alloc)...)
	data = append(data, PutLengthEncodedString(tableName, alloc)...)

	data = append(data, PutLengthEncodedString(fieldName, alloc)...)
	data = append(data, PutLengthEncodedString(fieldName, alloc)...)

	data = append(data, 0x0c)

	data = append(data, Uint16ToBytes(uint16(CharsetIds[DEFAULT_CHARSET]))...)
	data = append(data, Uint32ToBytes(45)...)
	data = append(data, MYSQL_TYPE_VARCHAR)
	data = append(data, Uint16ToBytes(0)...)
	data = append(data, 0x1f)
	data = append(data, 0, 0)

	if defaultValue != nil {
		data = append(data, Uint64ToBytes(uint64(len(defaultValue)))...)
		data = append(data, defaultValue...)
	}

	return data
}

func formatValue(value interface{}) ([]byte, error) {
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
	default:
		return nil, fmt.Errorf("invalid type %T", value)
	}
}

func (c *Conn) writeResultset(status uint16, rs ql.Recordset) error {
	fields, err := rs.Fields()
	if err != nil {
		return err
	}
	columnLen := PutLengthEncodedInt(uint64(len(fields)))
	data := c.alloc.AllocBytesWithLen(4, 1024)
	data = append(data, columnLen...)
	if err := c.writePacket(data); err != nil {
		return errors.Trace(err)
	}

	for _, v := range fields {
		data = data[0:4]
		data = append(data, c.dumpField(v, c.alloc)...)
		if err := c.writePacket(data); err != nil {
			return errors.Trace(err)
		}
	}

	if err := c.writeEOF(status); err != nil {
		return errors.Trace(err)
	}

	rows, err := rs.Rows(-1, 0)
	if err != nil {
		return err
	}
	for _, row := range rows {
		data = data[0:4]

		for _, value := range row {
			valData, err := formatValue(value)
			if err != nil {
				return errors.Trace(err)
			}
			data = append(data, valData...)
		}

		if err := c.writePacket(data); err != nil {
			return errors.Trace(err)
		}
	}

	err = c.writeEOF(status)
	if err != nil {
		return errors.Trace(err)
	}

	return errors.Trace(c.flush())
}
