package server

import (
	"bufio"
	"fmt"
	"io"
	"net"

	"github.com/juju/errors"
	. "github.com/pingcap/mp/protocol"
)

type PacketIO struct {
	rb *bufio.Reader
	wb *bufio.Writer

	Sequence uint8
}

func NewPacketIO(conn net.Conn) *PacketIO {
	p := &PacketIO{
		rb: bufio.NewReaderSize(conn, 2048),
		wb: bufio.NewWriterSize(conn, 2048),
	}

	return p
}

func (p *PacketIO) ReadPacket() ([]byte, error) {
	header := []byte{0, 0, 0, 0}

	if _, err := io.ReadFull(p.rb, header); err != nil {
		return nil, errors.Trace(err)
	}

	length := int(uint32(header[0]) | uint32(header[1])<<8 | uint32(header[2])<<16)
	if length < 1 {
		return nil, errors.Trace(fmt.Errorf("invalid payload length %d", length))
	}

	sequence := uint8(header[3])
	if sequence != p.Sequence {
		return nil, errors.Trace(fmt.Errorf("invalid sequence %d != %d", sequence, p.Sequence))
	}

	p.Sequence++

	data := make([]byte, length)
	if _, err := io.ReadFull(p.rb, data); err != nil {
		return nil, errors.Trace(err)
	} else {
		if length < MaxPayloadLen {
			return data, nil
		}

		var buf []byte
		buf, err = p.ReadPacket()
		if err != nil {
			return nil, errors.Trace(err)
		} else {
			return append(data, buf...), nil
		}
	}
}

//data already have header
func (p *PacketIO) WritePacket(data []byte) error {
	length := len(data) - 4

	for length >= MaxPayloadLen {
		data[0] = 0xff
		data[1] = 0xff
		data[2] = 0xff

		data[3] = p.Sequence

		if n, err := p.wb.Write(data[:4+MaxPayloadLen]); err != nil {
			return ErrBadConn
		} else if n != (4 + MaxPayloadLen) {
			return ErrBadConn
		} else {
			p.Sequence++
			length -= MaxPayloadLen
			data = data[MaxPayloadLen:]
		}
	}

	data[0] = byte(length)
	data[1] = byte(length >> 8)
	data[2] = byte(length >> 16)
	data[3] = p.Sequence

	if n, err := p.wb.Write(data); err != nil {
		return errors.Trace(ErrBadConn)
	} else if n != len(data) {
		return errors.Trace(ErrBadConn)
	} else {
		p.Sequence++
		return nil
	}
}

func (p *PacketIO) Flush() error {
	return p.wb.Flush()
}
