package server

import (
	"encoding/binary"

	"github.com/ngaut/arena"
	. "github.com/pingcap/mp/protocol"
)

type ColumnInfo struct {
	Schema             string
	Table              string
	OrgTable           string
	Name               string
	OrgName            string
	ColumnLength       uint32
	Charset            uint16
	Flag               uint16
	Decimal            uint8
	Type               uint8
	DefaultValueLength uint64
	DefaultValue       []byte
}

func ParseColumnInfo(data []byte) (col *ColumnInfo, err error) {
	col = new(ColumnInfo)

	var n int
	pos := 0
	//skip catelog, always def
	n, err = skipLengthEnodedString(data)
	if err != nil {
		return
	}
	pos += n

	//schema
	col.Schema, _, n, err = parseLengthEncodedString(data[pos:])
	if err != nil {
		return
	}
	pos += n

	//table
	col.Table, _, n, err = parseLengthEncodedString(data[pos:])
	if err != nil {
		return
	}
	pos += n

	//org_table
	col.OrgTable, _, n, err = parseLengthEncodedString(data[pos:])
	if err != nil {
		return
	}
	pos += n

	//name
	col.Name, _, n, err = parseLengthEncodedString(data[pos:])
	if err != nil {
		return
	}
	pos += n

	//org_name
	col.OrgName, _, n, err = parseLengthEncodedString(data[pos:])
	if err != nil {
		return
	}
	pos += n

	//skip oc
	pos += 1

	//charset
	col.Charset = binary.LittleEndian.Uint16(data[pos:])
	pos += 2

	//column length
	col.ColumnLength = binary.LittleEndian.Uint32(data[pos:])
	pos += 4

	//type
	col.Type = data[pos]
	pos++

	//flag
	col.Flag = binary.LittleEndian.Uint16(data[pos:])
	pos += 2

	//decimals 1
	col.Decimal = data[pos]
	pos++

	//filter [0x00][0x00]
	pos += 2

	col.DefaultValue = nil
	//if more data, command was field list
	if len(data) > pos {
		//length of default value lenenc-int
		col.DefaultValueLength, _, n = parseLengthEncodedInt(data[pos:])
		pos += n

		if pos+int(col.DefaultValueLength) > len(data) {
			err = ErrMalformPacket
			return
		}

		//default value string[$len]
		col.DefaultValue = data[pos:(pos + int(col.DefaultValueLength))]
	}
	return
}

func (column *ColumnInfo) Dump(alloc arena.ArenaAllocator) []byte {
	l := len(column.Schema) + len(column.Table) + len(column.OrgTable) + len(column.Name) + len(column.OrgName) + len(column.DefaultValue) + 48

	data := make([]byte, 0, l)

	data = append(data, dumpLengthEncodedString([]byte("def"), alloc)...)

	data = append(data, dumpLengthEncodedString([]byte(column.Schema), alloc)...)

	data = append(data, dumpLengthEncodedString([]byte(column.Table), alloc)...)
	data = append(data, dumpLengthEncodedString([]byte(column.OrgTable), alloc)...)

	data = append(data, dumpLengthEncodedString([]byte(column.Name), alloc)...)
	data = append(data, dumpLengthEncodedString([]byte(column.OrgName), alloc)...)

	data = append(data, 0x0c)

	data = append(data, dumpUint16(column.Charset)...)
	data = append(data, dumpUint32(column.ColumnLength)...)
	data = append(data, column.Type)
	data = append(data, dumpUint16(column.Flag)...)
	data = append(data, column.Decimal)
	data = append(data, 0, 0)

	if column.DefaultValue != nil {
		data = append(data, dumpUint64(uint64(len(column.DefaultValue)))...)
		data = append(data, []byte(column.DefaultValue)...)
	}

	return data
}
