package distrlock

import "errors"

var ErrTimeout = errors.New("timeout")
var ErrLockUpdate = errors.New("lock update error")

type ErrDistrLock struct {
	Msg string
	Err error
}

func NewDistrLockError(msg string, err error) *ErrDistrLock {
	return &ErrDistrLock{
		Msg: msg,
		Err: err,
	}
}

func (e *ErrDistrLock) Error() string {
	return e.Msg
}

func (e *ErrDistrLock) Unwrap() error {
	return e.Err
}
