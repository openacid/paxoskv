package paxoskv

import (
	"fmt"
	"log"
	"os"

	"github.com/kr/pretty"
	"github.com/openacid/paxoskv/goid"
)

var (
	defaultLogger = log.New(os.Stderr, fmt.Sprintf("??: "), log.Ltime|log.Lmicroseconds|log.Lshortfile|log.Lmsgprefix)
)

type loggerGetter interface {
	getLogger() *log.Logger
}

func dd(gg loggerGetter, f string, args ...interface{}) {
	p := fmt.Sprintf("%s ", goid.ID())
	getLogger(gg).Output(2, p+pretty.Sprintf(f, args...))
}

func bug(gg loggerGetter, f string, args ...interface{}) {
	p := fmt.Sprintf("%s ", goid.ID())
	getLogger(gg).Output(2, p+pretty.Sprintf(f, args...))
	panic("bug")
}

func getLogger(gg loggerGetter) *log.Logger {
	if gg == nil {
		return defaultLogger
	} else {
		lg := gg.getLogger()
		if lg == nil {
			lg = defaultLogger
		}
		return lg
	}
}
