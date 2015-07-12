package server

import (
	"errors"

	jerrors "github.com/juju/errors"
	. "gopkg.in/check.v1"
)

var _ = Suite(&testUtilSuite{})

type testUtilSuite struct {
}

func (s *testUtilSuite) TestErrorEuqal(c *C) {
	e1 := errors.New("test error")
	c.Assert(e1, NotNil)

	e2 := jerrors.Trace(e1)
	c.Assert(e2, NotNil)

	e3 := jerrors.Trace(e2)
	c.Assert(e3, NotNil)

	c.Assert(jerrors.Cause(e2), Equals, e1)
	c.Assert(jerrors.Cause(e3), Equals, e1)
	c.Assert(jerrors.Cause(e2), Equals, jerrors.Cause(e3))

	e4 := jerrors.New("test error")
	c.Assert(jerrors.Cause(e4), Not(Equals), e1)

	e5 := jerrors.Errorf("test error")
	c.Assert(jerrors.Cause(e5), Not(Equals), e1)

	c.Assert(ErrorEqual(e1, e2), Equals, true)
	c.Assert(ErrorEqual(e1, e3), Equals, true)
	c.Assert(ErrorEqual(e1, e4), Equals, true)
	c.Assert(ErrorEqual(e1, e5), Equals, true)

	var e6 error

	c.Assert(ErrorEqual(nil, nil), Equals, true)
	c.Assert(ErrorNotEqual(e1, e6), Equals, true)
}
