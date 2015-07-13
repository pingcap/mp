package server

import (
	"crypto/rand"
	"io"
	"net"
	"sync"
	"sync/atomic"

	"github.com/juju/errors"
	"github.com/ngaut/arena"
	stats "github.com/ngaut/gostats"
	"github.com/ngaut/log"
	"github.com/ngaut/tokenlimiter"
	"github.com/pingcap/mp/etc"
	"github.com/pingcap/mp/protocol"
)

var (
	baseConnId uint32 = 10000
)

type Server struct {
	cfg               *etc.Config
	driver            IDriver
	listener          net.Listener
	rwlock            *sync.RWMutex
	concurrentLimiter *tokenlimiter.TokenLimiter
	counter           *stats.Counters
	clients           map[uint32]*ClientConn
}

func (s *Server) IncCounter(key string) {
	s.counter.Add(key, 1)
}

func (s *Server) DecCounter(key string) {
	s.counter.Add(key, -1)
}

func (s *Server) GetToken() *tokenlimiter.Token {
	return s.concurrentLimiter.Get()
}

func (s *Server) ReleaseToken(token *tokenlimiter.Token) {
	s.concurrentLimiter.Put(token)
}

func (s *Server) newConn(conn net.Conn) (cc *ClientConn, err error) {
	log.Info("newConn", conn.RemoteAddr().String())
	cc = &ClientConn{
		conn:         conn,
		pkg:          NewPacketIO(conn),
		server:       s,
		connectionId: atomic.AddUint32(&baseConnId, 1),
		collation:    protocol.DEFAULT_COLLATION_ID,
		charset:      protocol.DEFAULT_CHARSET,
		alloc:        arena.NewArenaAllocator(32 * 1024),
	}
	cc.salt = make([]byte, 20)
	io.ReadFull(rand.Reader, cc.salt)
	for i, b := range cc.salt {
		if b == 0 {
			cc.salt[i] = '0'
		}
	}
	return
}

func (s *Server) GetRWlock() *sync.RWMutex {
	return s.rwlock
}

func (s *Server) SkipAuth() bool {
	return s.cfg.SkipAuth
}

func (s *Server) CfgGetPwd(user string) string {
	return s.cfg.Password //TODO support multiple users
}

func NewServer(cfg *etc.Config, driver IDriver) (*Server, error) {
	log.Warningf("%#v", cfg)
	s := &Server{
		cfg:               cfg,
		driver:            driver,
		concurrentLimiter: tokenlimiter.NewTokenLimiter(100),
		counter:           stats.NewCounters(""),
		rwlock:            &sync.RWMutex{},
		clients:           make(map[uint32]*ClientConn),
	}

	var err error
	s.listener, err = net.Listen("tcp", s.cfg.Addr)
	if err != nil {
		return nil, errors.Trace(err)
	}

	log.Infof("Server run MySql Protocol Listen at [%s]", s.cfg.Addr)
	return s, nil
}

func (s *Server) Run() error {
	for {
		conn, err := s.listener.Accept()
		if err != nil {
			log.Errorf("accept error %s", err.Error())
			return err
		}

		go s.onConn(conn)
	}

	return nil
}

func (s *Server) Close() {
	s.rwlock.Lock()
	defer s.rwlock.Unlock()

	if s.listener != nil {
		s.listener.Close()
		s.listener = nil
	}
}

func (s *Server) onConn(c net.Conn) {
	conn, err := s.newConn(c)
	if err != nil {
		log.Errorf("newConn error %s", errors.ErrorStack(err))
		return
	}
	if err := conn.Handshake(); err != nil {
		log.Errorf("handshake error %s", errors.ErrorStack(err))
		c.Close()
		return
	}
	conn.ctx, err = s.driver.OpenCtx(conn.capability, uint8(conn.collation), conn.dbname)
	if err != nil {
		log.Errorf("open ctx error %s", errors.ErrorStack(err))
		c.Close()
		return
	}

	const key = "connections"

	s.IncCounter(key)
	defer func() {
		s.DecCounter(key)
		log.Infof("close %s", conn)
	}()

	s.rwlock.Lock()
	s.clients[conn.connectionId] = conn
	s.rwlock.Unlock()

	conn.Run()
}
