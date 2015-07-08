package server

import (
	"fmt"
	"strconv"

	"github.com/juju/errors"
	"github.com/pingcap/mp/hack"
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

func (cc *ClientConn) writeResultset(rs *ResultSet, binary bool) error {
	columnLen := PutLengthEncodedInt(uint64(len(rs.Columns)))
	data := cc.alloc.AllocBytesWithLen(4, 1024)
	data = append(data, columnLen...)
	if err := cc.writePacket(data); err != nil {
		return errors.Trace(err)
	}

	for _, v := range rs.Columns {
		data = data[0:4]
		data = append(data, v.Dump(cc.alloc)...)
		if err := cc.writePacket(data); err != nil {
			return errors.Trace(err)
		}
	}

	if err := cc.writeEOF(cc.ctx.Status()); err != nil {
		return errors.Trace(err)
	}
	for _, row := range rs.Rows {
		data = data[0:4]
		if binary {
			rowData, err := EncodeRowValuesBinary(rs.Columns, row)
			if err != nil {
				return errors.Trace(err)
			}
			data = append(data, rowData...)
		} else {
			for _, value := range row {
				valData, err := formatValue(value)
				if err != nil {
					return errors.Trace(err)
				}
				data = append(data, PutLengthEncodedString(valData, cc.alloc)...)
			}
		}

		if err := cc.writePacket(data); err != nil {
			return errors.Trace(err)
		}
	}

	err := cc.writeEOF(cc.ctx.Status())
	if err != nil {
		return errors.Trace(err)
	}

	return errors.Trace(cc.flush())
}
