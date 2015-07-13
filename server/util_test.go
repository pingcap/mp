package server

import (
	"errors"

	jerrors "github.com/juju/errors"
	. "gopkg.in/check.v1"
)

var _ = Suite(&testUtilSuite{})

type testUtilSuite struct {
}
