package server

import (
	"fmt"
	"github.com/juju/errors"
	"github.com/pingcap/mp/hack"
	. "github.com/pingcap/mp/protocol"
	"strconv"
)


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

func (c *Conn) writeResultset(rs *Result) error {
	columnLen := PutLengthEncodedInt(uint64(len(rs.Columns)))
	data := c.alloc.AllocBytesWithLen(4, 1024)
	data = append(data, columnLen...)
	if err := c.writePacket(data); err != nil {
		return errors.Trace(err)
	}

	for _, v := range rs.Columns {
		data = data[0:4]
		data = append(data, v.Dump(c.alloc)...)
		if err := c.writePacket(data); err != nil {
			return errors.Trace(err)
		}
	}

	if err := c.writeEOF(c.ctx.Status()); err != nil {
		return errors.Trace(err)
	}
	for _, row := range rs.Rows {
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

	err := c.writeEOF(c.ctx.Status())
	if err != nil {
		return errors.Trace(err)
	}

	return errors.Trace(c.flush())
}
