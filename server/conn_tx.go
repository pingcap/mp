package server

import (
	"github.com/ngaut/log"
	"github.com/pingcap/mp/protocol"
)

func (c *Conn) inTransaction() bool {
	return c.status&protocol.SERVER_STATUS_IN_TRANS > 0
}

func (c *Conn) isAutoCommit() bool {
	return c.status&protocol.SERVER_STATUS_AUTOCOMMIT > 0
}

func (c *Conn) handleBegin() error {
	log.Debug("handle begin")
	c.status |= protocol.SERVER_STATUS_IN_TRANS

	return c.writeOkFlush()
}

func (c *Conn) handleCommit() (err error) {
	log.Warning("commit")
	if err := c.commit(); err != nil {
		return err
	}

	return c.writeOkFlush()
}

func (c *Conn) handleRollback() (err error) {
	log.Warning("rollback")
	if err := c.rollback(); err != nil {
		return err
	}

	return c.writeOkFlush()
}

func (c *Conn) commit() (err error) {
	c.status &= ^protocol.SERVER_STATUS_IN_TRANS

	return
}

func (c *Conn) rollback() (err error) {
	c.status &= ^protocol.SERVER_STATUS_IN_TRANS

	return
}

//if status is in_trans, need
//else if status is not autocommit, need
//else no need
//todo: rename this function
func (c *Conn) needBeginTx() bool {
	return c.inTransaction() || !c.isAutoCommit()
}
