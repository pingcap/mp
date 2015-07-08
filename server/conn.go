package server

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"net"
	"runtime"

	"github.com/juju/errors"
	"github.com/ngaut/arena"
	"github.com/ngaut/log"
	"github.com/pingcap/mp/hack"
	"github.com/pingcap/mp/protocol"
)

var DEFAULT_CAPABILITY uint32 = protocol.CLIENT_LONG_PASSWORD | protocol.CLIENT_LONG_FLAG |
	protocol.CLIENT_CONNECT_WITH_DB | protocol.CLIENT_PROTOCOL_41 |
	protocol.CLIENT_TRANSACTIONS | protocol.CLIENT_SECURE_CONNECTION

type ClientConn struct {
	pkg          *PacketIO
	conn         net.Conn
	server       *Server
	capability   uint32
	connectionId uint32
	collation    protocol.CollationId
	charset      string
	user         string
	salt         []byte
	alloc        arena.ArenaAllocator
	lastCmd      string
	ctx          Context
}

func (cc *ClientConn) String() string {
	return fmt.Sprintf("conn: %s, status: %d, charset: %s, user: %s, lastInsertId: %d",
		cc.conn.RemoteAddr(), cc.ctx.Status(), cc.charset, cc.user, cc.ctx.LastInsertID(),
	)
}

func (cc *ClientConn) Handshake() error {
	if err := cc.writeInitialHandshake(); err != nil {
		return errors.Trace(err)
	}
	if err := cc.readHandshakeResponse(); err != nil {
		cc.writeError(err)
		return errors.Trace(err)
	}
	err := cc.writeOK()
	cc.pkg.Sequence = 0

	return err
}

func (cc *ClientConn) Close() error {
	cc.conn.Close()
	return cc.ctx.Close()
}

func (cc *ClientConn) writeInitialHandshake() error {
	data := make([]byte, 4, 128)

	//min version 10
	data = append(data, 10)
	//server version[00]
	data = append(data, protocol.ServerVersion...)
	data = append(data, 0)
	//connection id
	data = append(data, byte(cc.connectionId), byte(cc.connectionId>>8), byte(cc.connectionId>>16), byte(cc.connectionId>>24))
	//auth-plugin-data-part-1
	data = append(data, cc.salt[0:8]...)
	//filter [00]
	data = append(data, 0)
	//capability flag lower 2 bytes, using default capability here
	data = append(data, byte(DEFAULT_CAPABILITY), byte(DEFAULT_CAPABILITY>>8))
	//charset, utf-8 default
	data = append(data, uint8(protocol.DEFAULT_COLLATION_ID))
	//status
	data = append(data, byte(cc.ctx.Status()), byte(cc.ctx.Status()>>8))
	//below 13 byte may not be used
	//capability flag upper 2 bytes, using default capability here
	data = append(data, byte(DEFAULT_CAPABILITY>>16), byte(DEFAULT_CAPABILITY>>24))
	//filter [0x15], for wireshark dump, value is 0x15
	data = append(data, 0x15)
	//reserved 10 [00]
	data = append(data, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0)
	//auth-plugin-data-part-2
	data = append(data, cc.salt[8:]...)
	//filter [00]
	data = append(data, 0)
	err := cc.writePacket(data)
	if err != nil {
		return err
	}
	return cc.flush()
}

func (cc *ClientConn) readPacket() ([]byte, error) {
	return cc.pkg.ReadPacket()
}

func (cc *ClientConn) writePacket(data []byte) error {
	return cc.pkg.WritePacket(data)
}

func (cc *ClientConn) readHandshakeResponse() error {
	data, err := cc.readPacket()

	if err != nil {
		return errors.Trace(err)
	}

	pos := 0
	//capability
	cc.capability = binary.LittleEndian.Uint32(data[:4])
	pos += 4
	//skip max packet size
	pos += 4
	//charset, skip, if you want to use another charset, use set names
	cc.collation = protocol.CollationId(data[pos])
	pos++
	//skip reserved 23[00]
	pos += 23
	//user name
	cc.user = string(data[pos : pos+bytes.IndexByte(data[pos:], 0)])
	pos += len(cc.user) + 1
	//auth length and auth
	authLen := int(data[pos])
	pos++
	auth := data[pos : pos+authLen]
	checkAuth := CalcPassword(cc.salt, []byte(cc.server.CfgGetPwd(cc.user)))
	if !bytes.Equal(auth, checkAuth) && !cc.server.SkipAuth() {
		return errors.Trace(protocol.NewDefaultError(protocol.ER_ACCESS_DENIED_ERROR, cc.conn.RemoteAddr().String(), cc.user, "Yes"))
	}

	pos += authLen
	if cc.capability|protocol.CLIENT_CONNECT_WITH_DB > 0 {
		if len(data[pos:]) == 0 {
			return nil
		}

		db := string(data[pos : pos+bytes.IndexByte(data[pos:], 0)])
		if err := cc.useDB(db); err != nil {
			return errors.Trace(err)
		}
	}

	return nil
}

func (cc *ClientConn) Run() {
	defer func() {
		r := recover()
		if err, ok := r.(error); ok {
			const size = 4096
			buf := make([]byte, size)
			buf = buf[:runtime.Stack(buf, false)]

			log.Errorf("lastCmd %s, %v, %s", cc.lastCmd, err, buf)
		}

		cc.Close()
	}()

	for {
		cc.alloc.Reset()
		data, err := cc.readPacket()
		if err != nil {
			if err.Error() != io.EOF.Error() {
				log.Info(err)
			}
			return
		}
		log.Debug("packet length", len(data))

		if err := cc.dispatch(data); err != nil {
			log.Errorf("dispatch error %s, %s", errors.ErrorStack(err), cc)
			if err != protocol.ErrBadConn { //todo: fix this
				cc.writeError(err)
			}
		}

		cc.pkg.Sequence = 0
	}
}

func (cc *ClientConn) dispatch(data []byte) error {
	cmd := data[0]
	data = data[1:]
	log.Debug(cc.connectionId, cmd, string(data))
	cc.lastCmd = hack.String(data)

	token := cc.server.GetToken()

	cc.server.GetRWlock().RLock()
	defer func() {
		cc.server.GetRWlock().RUnlock()
		cc.server.ReleaseToken(token)
	}()

	cc.server.IncCounter(protocol.MYSQL_COMMAND(cmd).String())

	switch protocol.MYSQL_COMMAND(cmd) {
	case protocol.COM_QUIT:
		cc.ctx.Close()
		cc.Close()
		return nil
	case protocol.COM_QUERY:
		return cc.handleQuery(hack.String(data))
	case protocol.COM_PING:
		return cc.writeOK()
	case protocol.COM_INIT_DB:
		log.Debug(cmd, hack.String(data))
		if err := cc.useDB(hack.String(data)); err != nil {
			return errors.Trace(err)
		}

		return cc.writeOK()
	case protocol.COM_FIELD_LIST:
		return cc.handleFieldList(hack.String(data))
	case protocol.COM_STMT_PREPARE:
		return cc.handleStmtPrepare(hack.String(data))
	case protocol.COM_STMT_EXECUTE:
		return cc.handleStmtExecute(data)
	case protocol.COM_STMT_CLOSE:
		return cc.handleStmtClose(data)
	case protocol.COM_STMT_SEND_LONG_DATA:
		return cc.handleStmtSendLongData(data)
	case protocol.COM_STMT_RESET:
		return cc.handleStmtReset(data)
	default:
		msg := fmt.Sprintf("command %d not supported now", cmd)
		return protocol.NewError(protocol.ER_UNKNOWN_ERROR, msg)
	}

	return nil
}

func (cc *ClientConn) useDB(db string) (err error) {
	_, err = cc.ctx.Execute("use " + db)
	return err
}

func (cc *ClientConn) flush() error {
	return cc.pkg.Flush()
}

func (cc *ClientConn) writeOK() error {
	data := cc.alloc.AllocBytesWithLen(4, 32)
	data = append(data, protocol.OK_HEADER)
	data = append(data, PutLengthEncodedInt(uint64(cc.ctx.AffectedRows()))...)
	data = append(data, PutLengthEncodedInt(uint64(cc.ctx.LastInsertID()))...)
	if cc.capability&protocol.CLIENT_PROTOCOL_41 > 0 {
		data = append(data, byte(cc.ctx.Status()), byte(cc.ctx.Status()>>8))
		data = append(data, 0, 0)
	}

	err := cc.writePacket(data)
	if err != nil {
		return errors.Trace(err)
	}

	return errors.Trace(cc.flush())
}

func (cc *ClientConn) writeError(e error) error {
	var m *protocol.SqlError
	var ok bool
	if m, ok = e.(*protocol.SqlError); !ok {
		m = protocol.NewError(protocol.ER_UNKNOWN_ERROR, e.Error())
	}

	data := make([]byte, 4, 16+len(m.Message))
	data = append(data, protocol.ERR_HEADER)
	data = append(data, byte(m.Code), byte(m.Code>>8))
	if cc.capability&protocol.CLIENT_PROTOCOL_41 > 0 {
		data = append(data, '#')
		data = append(data, m.State...)
	}

	data = append(data, m.Message...)

	err := cc.writePacket(data)
	if err != nil {
		return errors.Trace(err)
	}
	return errors.Trace(cc.flush())
}

func (cc *ClientConn) writeEOF(status uint16) error {
	data := cc.alloc.AllocBytesWithLen(4, 9)

	data = append(data, protocol.EOF_HEADER)
	if cc.capability&protocol.CLIENT_PROTOCOL_41 > 0 {
		data = append(data, 0, 0)
		data = append(data, byte(status), byte(status>>8))
	}

	err := cc.writePacket(data)
	return errors.Trace(err)
}

func (cc *ClientConn) handleQuery(sql string) (err error) {
	rs, err := cc.ctx.Execute(sql)
	if err != nil {
		return err
	}
	if rs != nil {
		cc.writeResultset(rs, false)
	} else {
		cc.writeOK()
	}
	return
}

func (cc *ClientConn) writeFieldList(status uint16, fs []*ColumnInfo) error {
	data := make([]byte, 4, 1024)
	for _, v := range fs {
		data = data[0:4]
		data = append(data, v.Dump(cc.alloc)...)
		if err := cc.writePacket(data); err != nil {
			return err
		}
	}
	if err := cc.writeEOF(cc.ctx.Status()); err != nil {
		return err
	}
	return errors.Trace(cc.flush())
}

func (cc *ClientConn) handleFieldList(sql string) (err error) {
	columns, err := cc.ctx.FieldList(sql, "")
	if err != nil {
		return
	}
	return cc.writeFieldList(cc.ctx.Status(), columns)
}
