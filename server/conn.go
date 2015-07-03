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

//client <-> proxy
type Conn struct {
	pkg          *protocol.PacketIO
	c            net.Conn
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

func (c *Conn) String() string {
	return fmt.Sprintf("conn: %s, status: %d, charset: %s, user: %s, lastInsertId: %d",
		c.c.RemoteAddr(), c.ctx.Status(), c.charset, c.user, c.ctx.LastInsertID(),
	)
}

func (c *Conn) Handshake() error {
	if err := c.writeInitialHandshake(); err != nil {
		return errors.Trace(err)
	}

	if err := c.readHandshakeResponse(); err != nil {
		c.writeError(err)
		return errors.Trace(err)
	}

	err := c.writeOK()
	c.pkg.Sequence = 0

	return err
}

func (c *Conn) Close() error {
	c.c.Close()
	return nil
}

func (c *Conn) writeInitialHandshake() error {
	data := make([]byte, 4, 128)

	//min version 10
	data = append(data, 10)
	//server version[00]
	data = append(data, protocol.ServerVersion...)
	data = append(data, 0)
	//connection id
	data = append(data, byte(c.connectionId), byte(c.connectionId>>8), byte(c.connectionId>>16), byte(c.connectionId>>24))
	//auth-plugin-data-part-1
	data = append(data, c.salt[0:8]...)
	//filter [00]
	data = append(data, 0)
	//capability flag lower 2 bytes, using default capability here
	data = append(data, byte(DEFAULT_CAPABILITY), byte(DEFAULT_CAPABILITY>>8))
	//charset, utf-8 default
	data = append(data, uint8(protocol.DEFAULT_COLLATION_ID))
	//status
	data = append(data, byte(c.ctx.Status()), byte(c.ctx.Status()>>8))
	//below 13 byte may not be used
	//capability flag upper 2 bytes, using default capability here
	data = append(data, byte(DEFAULT_CAPABILITY>>16), byte(DEFAULT_CAPABILITY>>24))
	//filter [0x15], for wireshark dump, value is 0x15
	data = append(data, 0x15)
	//reserved 10 [00]
	data = append(data, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0)
	//auth-plugin-data-part-2
	data = append(data, c.salt[8:]...)
	//filter [00]
	data = append(data, 0)
	err := c.writePacket(data)
	if err != nil {
		return err
	}
	return c.flush()
}

func (c *Conn) readPacket() ([]byte, error) {
	return c.pkg.ReadPacket()
}

func (c *Conn) writePacket(data []byte) error {
	return c.pkg.WritePacket(data)
}

func (c *Conn) readHandshakeResponse() error {
	data, err := c.readPacket()

	if err != nil {
		return errors.Trace(err)
	}

	pos := 0
	//capability
	c.capability = binary.LittleEndian.Uint32(data[:4])
	pos += 4
	//skip max packet size
	pos += 4
	//charset, skip, if you want to use another charset, use set names
	c.collation = protocol.CollationId(data[pos])
	pos++
	//skip reserved 23[00]
	pos += 23
	//user name
	c.user = string(data[pos : pos+bytes.IndexByte(data[pos:], 0)])
	pos += len(c.user) + 1
	//auth length and auth
	authLen := int(data[pos])
	pos++
	auth := data[pos : pos+authLen]
	checkAuth := protocol.CalcPassword(c.salt, []byte(c.server.CfgGetPwd(c.user)))
	if !bytes.Equal(auth, checkAuth) && !c.server.SkipAuth() {
		return errors.Trace(protocol.NewDefaultError(protocol.ER_ACCESS_DENIED_ERROR, c.c.RemoteAddr().String(), c.user, "Yes"))
	}

	pos += authLen
	if c.capability|protocol.CLIENT_CONNECT_WITH_DB > 0 {
		if len(data[pos:]) == 0 {
			return nil
		}

		db := string(data[pos : pos+bytes.IndexByte(data[pos:], 0)])
		if err := c.useDB(db); err != nil {
			return errors.Trace(err)
		}
	}

	return nil
}

func (c *Conn) Run() {
	defer func() {
		r := recover()
		if err, ok := r.(error); ok {
			const size = 4096
			buf := make([]byte, size)
			buf = buf[:runtime.Stack(buf, false)]

			log.Errorf("lastCmd %s, %v, %s", c.lastCmd, err, buf)
		}

		c.Close()
	}()

	for {
		c.alloc.Reset()
		data, err := c.readPacket()
		if err != nil {
			if err.Error() != io.EOF.Error() {
				log.Info(err)
			}
			return
		}
		log.Debug("packet length", len(data))

		if err := c.dispatch(data); err != nil {
			log.Errorf("dispatch error %s, %s", errors.ErrorStack(err), c)
			if err != protocol.ErrBadConn { //todo: fix this
				c.writeError(err)
			}
		}

		c.pkg.Sequence = 0
	}
}

func (c *Conn) dispatch(data []byte) error {
	cmd := data[0]
	data = data[1:]
	log.Debug(c.connectionId, cmd, string(data))
	c.lastCmd = hack.String(data)

	token := c.server.GetToken()

	c.server.GetRWlock().RLock()
	defer func() {
		c.server.GetRWlock().RUnlock()
		c.server.ReleaseToken(token)
	}()

	c.server.IncCounter(protocol.MYSQL_COMMAND(cmd).String())

	switch protocol.MYSQL_COMMAND(cmd) {
	case protocol.COM_QUIT:
		c.server.driver.CloseCtx(c.ctx)
		c.Close()
		return nil
	case protocol.COM_QUERY:
		return c.handleQuery(hack.String(data))
	case protocol.COM_PING:
		return c.writeOK()
	case protocol.COM_INIT_DB:
		log.Debug(cmd, hack.String(data))
		if err := c.useDB(hack.String(data)); err != nil {
			return errors.Trace(err)
		}

		return c.writeOK()
	case protocol.COM_FIELD_LIST:
		return c.handleFieldList(hack.String(data))
	case protocol.COM_STMT_PREPARE:
		return c.handleStmtPrepare(hack.String(data))
	case protocol.COM_STMT_EXECUTE:
		return c.handleStmtExecute(hack.String(data))
	case protocol.COM_STMT_CLOSE:
		return c.handleStmtClose(hack.String(data))
	case protocol.COM_STMT_SEND_LONG_DATA:
		return c.handleStmtSendLongData(hack.String(data))
	case protocol.COM_STMT_RESET:
		return c.handleStmtReset(hack.String(data))
	default:
		msg := fmt.Sprintf("command %d not supported now", cmd)
		return protocol.NewError(protocol.ER_UNKNOWN_ERROR, msg)
	}

	return nil
}

func (c *Conn) useDB(db string) (err error) {
	_, err = c.server.driver.Execute("use "+db, c.ctx)
	return err
}

func (c *Conn) flush() error {
	return c.pkg.Flush()
}

func (c *Conn) writeOK() error {
	data := c.alloc.AllocBytesWithLen(4, 32)
	data = append(data, protocol.OK_HEADER)
	data = append(data, protocol.PutLengthEncodedInt(uint64(c.ctx.AffectedRows()))...)
	data = append(data, protocol.PutLengthEncodedInt(uint64(c.ctx.LastInsertID()))...)
	if c.capability&protocol.CLIENT_PROTOCOL_41 > 0 {
		data = append(data, byte(c.ctx.Status()), byte(c.ctx.Status()>>8))
		data = append(data, 0, 0)
	}

	err := c.writePacket(data)
	if err != nil {
		return errors.Trace(err)
	}

	return errors.Trace(c.flush())
}

func (c *Conn) writeError(e error) error {
	var m *protocol.SqlError
	var ok bool
	if m, ok = e.(*protocol.SqlError); !ok {
		m = protocol.NewError(protocol.ER_UNKNOWN_ERROR, e.Error())
	}

	data := make([]byte, 4, 16+len(m.Message))
	data = append(data, protocol.ERR_HEADER)
	data = append(data, byte(m.Code), byte(m.Code>>8))
	if c.capability&protocol.CLIENT_PROTOCOL_41 > 0 {
		data = append(data, '#')
		data = append(data, m.State...)
	}

	data = append(data, m.Message...)

	err := c.writePacket(data)
	if err != nil {
		return errors.Trace(err)
	}
	return errors.Trace(c.flush())
}

func (c *Conn) writeEOF(status uint16) error {
	data := c.alloc.AllocBytesWithLen(4, 9)

	data = append(data, protocol.EOF_HEADER)
	if c.capability&protocol.CLIENT_PROTOCOL_41 > 0 {
		data = append(data, 0, 0)
		data = append(data, byte(status), byte(status>>8))
	}

	err := c.writePacket(data)
	return errors.Trace(err)
}

func (c *Conn) handleQuery(sql string) (err error) {
	rs, err := c.server.driver.Execute(sql, c.ctx)
	if err != nil {
		return err
	}
	if rs != nil {
		c.writeResultset(rs)
	} else {
		c.writeOK()
	}
	return
}

func (c *Conn) writeFieldList(status uint16, fs []*ColumnInfo) error {
	data := make([]byte, 4, 1024)
	for _, v := range fs {
		data = data[0:4]
		data = append(data, v.Dump(c.alloc)...)
		if err := c.writePacket(data); err != nil {
			return err
		}
	}
	if err := c.writeEOF(c.ctx.Status()); err != nil {
		return err
	}
	return errors.Trace(c.flush())
}

func (c *Conn) handleFieldList(sql string) (err error) {
	columns := c.server.driver.FieldList(sql, c.ctx)
	return c.writeFieldList(c.ctx.Status(), columns)
}

func (c *Conn) handleStmtPrepare(sql string) (err error) {
	return c.writeError(nil)
}

func (c *Conn) handleStmtExecute(sql string) (err error) {
	return c.writeError(nil)
}

func (c *Conn) handleStmtClose(sql string) (err error) {
	return c.writeError(nil)
}

func (c *Conn) handleStmtSendLongData(sql string) (err error) {
	return c.writeError(nil)
}

func (c *Conn) handleStmtReset(sql string) (err error) {
	return c.writeError(nil)
}
