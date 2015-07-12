package server

type causer interface {
	Cause() error
}

func ErrorEqual(err1, err2 error) bool {
	if e, ok := err1.(causer); ok {
		err1 = e.Cause()
	}
	if e, ok := err2.(causer); ok {
		err2 = e.Cause()
	}
	if err1 == err2 {
		return true
	}

	if err1 == nil || err2 == nil {
		return err1 == err2
	}

	return err1.Error() == err2.Error()
}

func ErrorNotEqual(err1, err2 error) bool {
	return !ErrorEqual(err1, err2)
}
