package server

import (
	"bytes"
	"crypto/sha1"
	"encoding/binary"
	"fmt"
	"io"
	"net"
	"runtime"

	"github.com/juju/errors"
	"github.com/ngaut/arena"
	"github.com/ngaut/log"
	"github.com/pingcap/mp/hack"
	. "github.com/pingcap/mysqldef"
	"github.com/reborndb/go/errors2"
)

var DefaultCapability uint32 = ClientLongPassword | ClientLongFlag |
	ClientConnectWithDB | ClientProtocol41 |
	ClientTransactions | ClientSecureConnection | ClientFoundRows

type ClientConn struct {
	pkg          *PacketIO
	conn         net.Conn
	server       *Server
	capability   uint32
	connectionId uint32
	collation    uint8
	charset      string
	user         string
	dbname       string
	salt         []byte
	alloc        arena.ArenaAllocator
	lastCmd      string
	ctx          IContext
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
	data := cc.alloc.AllocBytesWithLen(4, 32)
	data = append(data, OKHeader)
	data = append(data, 0, 0)
	if cc.capability&ClientProtocol41 > 0 {
		data = append(data, dumpUint16(ServerStatusAutocommit)...)
		data = append(data, 0, 0)
	}

	err := cc.writePacket(data)
	cc.pkg.Sequence = 0
	if err != nil {
		return errors.Trace(err)
	}

	return errors.Trace(cc.flush())
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
	data = append(data, ServerVersion...)
	data = append(data, 0)
	//connection id
	data = append(data, byte(cc.connectionId), byte(cc.connectionId>>8), byte(cc.connectionId>>16), byte(cc.connectionId>>24))
	//auth-plugin-data-part-1
	data = append(data, cc.salt[0:8]...)
	//filter [00]
	data = append(data, 0)
	//capability flag lower 2 bytes, using default capability here
	data = append(data, byte(DefaultCapability), byte(DefaultCapability>>8))
	//charset, utf-8 default
	data = append(data, uint8(DefaultCollationID))
	//status
	data = append(data, dumpUint16(ServerStatusAutocommit)...)
	//below 13 byte may not be used
	//capability flag upper 2 bytes, using default capability here
	data = append(data, byte(DefaultCapability>>16), byte(DefaultCapability>>24))
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

func calcPassword(scramble, password []byte) []byte {
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
	cc.collation = data[pos]
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
	checkAuth := calcPassword(cc.salt, []byte(cc.server.CfgGetPwd(cc.user)))
	if !bytes.Equal(auth, checkAuth) && !cc.server.SkipAuth() {
		return errors.Trace(NewDefaultError(ER_ACCESS_DENIED_ERROR, cc.conn.RemoteAddr().String(), cc.user, "Yes"))
	}

	pos += authLen
	if cc.capability|ClientConnectWithDB > 0 {
		if len(data[pos:]) == 0 {
			return nil
		}

		cc.dbname = string(data[pos : pos+bytes.IndexByte(data[pos:], 0)])
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
			if errors2.ErrorNotEqual(err, io.EOF) {
				log.Info(err)
			}
			return
		}

		if err := cc.dispatch(data); err != nil {
			log.Errorf("dispatch error %s, %s", errors.ErrorStack(err), cc)
			if err != ErrBadConn { //todo: fix this
				cc.writeError(err)
			}
		}

		cc.pkg.Sequence = 0
	}
}

func (cc *ClientConn) dispatch(data []byte) error {
	cmd := data[0]
	data = data[1:]
	if len(data) > 256 {
		log.Debug(cc.connectionId, cmd, string(data[:256])+"...")
	} else {
		log.Debug(cc.connectionId, cmd, string(data))
	}
	cc.lastCmd = hack.String(data)

	token := cc.server.GetToken()

	cc.server.GetRWlock().RLock()
	defer func() {
		cc.server.GetRWlock().RUnlock()
		cc.server.ReleaseToken(token)
	}()

	switch cmd {
	case ComQuit:
		cc.ctx.Close()
		cc.Close()
		return nil
	case ComQuery:
		return cc.handleQuery(hack.String(data))
	case ComPing:
		return cc.writeOK()
	case ComInitDB:
		log.Debug("init db", hack.String(data))
		if err := cc.useDB(hack.String(data)); err != nil {
			return errors.Trace(err)
		}

		return cc.writeOK()
	case ComFieldList:
		return cc.handleFieldList(hack.String(data))
	case ComStmtPrepare:
		return cc.handleStmtPrepare(hack.String(data))
	case ComStmtExecute:
		return cc.handleStmtExecute(data)
	case ComStmtClose:
		return cc.handleStmtClose(data)
	case ComStmtSendLongData:
		return cc.handleStmtSendLongData(data)
	case ComStmtReset:
		return cc.handleStmtReset(data)
	default:
		msg := fmt.Sprintf("command %d not supported now", cmd)
		return NewError(ER_UNKNOWN_ERROR, msg)
	}

	return nil
}

func (cc *ClientConn) useDB(db string) (err error) {
	_, err = cc.ctx.Execute("use " + db)
	if err != nil {
		return errors.Trace(err)
	}
	cc.dbname = db
	return
}

func (cc *ClientConn) flush() error {
	return cc.pkg.Flush()
}

func (cc *ClientConn) writeOK() error {
	data := cc.alloc.AllocBytesWithLen(4, 32)
	data = append(data, OKHeader)
	data = append(data, dumpLengthEncodedInt(uint64(cc.ctx.AffectedRows()))...)
	data = append(data, dumpLengthEncodedInt(uint64(cc.ctx.LastInsertID()))...)
	if cc.capability&ClientProtocol41 > 0 {
		data = append(data, dumpUint16(cc.ctx.Status())...)
		data = append(data, dumpUint16(cc.ctx.WarningCount())...)
	}

	err := cc.writePacket(data)
	if err != nil {
		return errors.Trace(err)
	}

	return errors.Trace(cc.flush())
}

func (cc *ClientConn) writeError(e error) error {
	var m *SqlError
	var ok bool
	if m, ok = e.(*SqlError); !ok {
		m = NewError(ER_UNKNOWN_ERROR, e.Error())
	}

	data := make([]byte, 4, 16+len(m.Message))
	data = append(data, ErrHeader)
	data = append(data, byte(m.Code), byte(m.Code>>8))
	if cc.capability&ClientProtocol41 > 0 {
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

func (cc *ClientConn) writeEOF() error {
	data := cc.alloc.AllocBytesWithLen(4, 9)

	data = append(data, EOFHeader)
	if cc.capability&ClientProtocol41 > 0 {
		data = append(data, dumpUint16(cc.ctx.WarningCount())...)
		data = append(data, dumpUint16(cc.ctx.Status())...)
	}

	err := cc.writePacket(data)
	return errors.Trace(err)
}

func (cc *ClientConn) handleQuery(sql string) (err error) {
	rs, err := cc.ctx.Execute(sql)
	if err != nil {
		return errors.Trace(err)
	}
	if rs != nil {
		err = cc.writeResultset(rs, false)
	} else {
		err = cc.writeOK()
	}
	return errors.Trace(err)
}

func (cc *ClientConn) handleFieldList(sql string) (err error) {
	columns, err := cc.ctx.FieldList(sql, "")
	if err != nil {
		return
	}
	data := make([]byte, 4, 1024)
	for _, v := range columns {
		data = data[0:4]
		data = append(data, v.Dump(cc.alloc)...)
		if err := cc.writePacket(data); err != nil {
			return err
		}
	}
	if err := cc.writeEOF(); err != nil {
		return err
	}
	return errors.Trace(cc.flush())
}

func (cc *ClientConn) writeResultset(rs *ResultSet, binary bool) error {
	columnLen := dumpLengthEncodedInt(uint64(len(rs.Columns)))
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

	if err := cc.writeEOF(); err != nil {
		return errors.Trace(err)
	}
	for _, row := range rs.Rows {
		data = data[0:4]
		if binary {
			rowData, err := dumpRowValuesBinary(cc.alloc, rs.Columns, row)
			if err != nil {
				return errors.Trace(err)
			}
			data = append(data, rowData...)
		} else {
			for i, value := range row {
				if value == nil {
					data = append(data, 0xfb)
					continue
				}
				valData, err := dumpTextValue(rs.Columns[i].Type, value)
				if err != nil {
					return errors.Trace(err)
				}
				data = append(data, dumpLengthEncodedString(valData, cc.alloc)...)
			}
		}

		if err := cc.writePacket(data); err != nil {
			return errors.Trace(err)
		}
	}

	err := cc.writeEOF()
	if err != nil {
		return errors.Trace(err)
	}

	return errors.Trace(cc.flush())
}
