package server

import (
	"github.com/ngaut/arena"
	"github.com/pingcap/mp/protocol"
	"github.com/pingcap/ql"
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

func (column *ColumnInfo) Dump(alloc arena.ArenaAllocator) []byte {
	l := len(column.Schema) + len(column.Table) + len(column.OrgTable) + len(column.Name) + len(column.OrgName) + len(column.DefaultValue) + 48

	data := make([]byte, 0, l)

	data = append(data, protocol.PutLengthEncodedString([]byte("def"), alloc)...)

	data = append(data, protocol.PutLengthEncodedString([]byte(column.Schema), alloc)...)

	data = append(data, protocol.PutLengthEncodedString([]byte(column.Table), alloc)...)
	data = append(data, protocol.PutLengthEncodedString([]byte(column.OrgTable), alloc)...)

	data = append(data, protocol.PutLengthEncodedString([]byte(column.Name), alloc)...)
	data = append(data, protocol.PutLengthEncodedString([]byte(column.OrgName), alloc)...)

	data = append(data, 0x0c)

	data = append(data, protocol.Uint16ToBytes(column.Charset)...)
	data = append(data, protocol.Uint32ToBytes(column.ColumnLength)...)
	data = append(data, column.Type)
	data = append(data, protocol.Uint16ToBytes(column.Flag)...)
	data = append(data, column.Decimal)
	data = append(data, 0, 0)

	if column.DefaultValue != nil {
		data = append(data, protocol.Uint64ToBytes(uint64(len(column.DefaultValue)))...)
		data = append(data, []byte(column.DefaultValue)...)
	}

	return data
}

type Result struct {
	Columns []*ColumnInfo
	Rows    [][]interface{}
}

type SessionCtx interface {
	Status() uint16
	LastInsertId() uint64
	AffectedRows() uint64
}

type IDriver interface {
	Execute(sql string, ctx ql.SessionCtx) (*Result, error)
	GetCtx() ql.SessionCtx
	CloseCtx(ql.SessionCtx) error
}
