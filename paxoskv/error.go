package paxoskv

import (
	"errors"
	"fmt"
)

var (
	HigherBalErr    = errors.New("seen a higher ballot")
	AlreadyPrepared = errors.New("already prepared")
	FakeErr         = errors.New("fake error")
)

type UncommittedErr struct {
	Column int64
}

func (e *UncommittedErr) Error() string {
	return fmt.Sprintf("not committed: %d", e.Column)
}
