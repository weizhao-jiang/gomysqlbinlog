package logx

import (
	"bufio"
	"fmt"
	"log"
	"os"
	"time"
)

type LOGLEVEL uint

var loglv = DEBUG_LEVEL

const (
	DEBUG_LEVEL LOGLEVEL = 5
	INFO_LEVEL  LOGLEVEL = 4
	WARN_LEVEL  LOGLEVEL = 3
	ERROR_LEVEL LOGLEVEL = 2
	PANIC_LEVEL LOGLEVEL = 1
	OUPUT_LEVEL LOGLEVEL = 0
)

func init() {
	log.SetFlags(log.Lshortfile)
}

func SetOutput(filepath string) {
	file, err := os.OpenFile(filepath, os.O_APPEND|os.O_CREATE|os.O_RDWR, 0644)
	if err != nil {
		Panic("failed to open log file.%s", err.Error())
	}
	logwbuf := bufio.NewWriter(file)
	log.SetOutput(logwbuf)
}

func SetLevel(lv LOGLEVEL) {
	loglv = lv
}

func template(loglevel LOGLEVEL, format string, args ...interface{}) {
	lvStr := "DEBUG"
	switch loglevel {
	case DEBUG_LEVEL:
		lvStr = "DEBU"
	case INFO_LEVEL:
		lvStr = "INFO"
	case WARN_LEVEL:
		lvStr = "WARN"
	case ERROR_LEVEL:
		lvStr = "ERRO"
	case PANIC_LEVEL:
		lvStr = "PANIC"
	}
	if loglevel == OUPUT_LEVEL {
		fmt.Printf(format, args...)
	} else if loglevel <= loglv {
		log.SetPrefix(time.Now().Format("2006-01-02 15:04:05.000") + " [" + lvStr + "] ")
		log.Output(3, fmt.Sprintf(format, args...))
	}

}

func Output(format string, args ...interface{}) {
	template(OUPUT_LEVEL, format, args...)
}

func OutputMark(format string, args ...interface{}) {
	template(OUPUT_LEVEL, format, args...)
}

func DebugF(format string, args ...interface{}) {
	template(DEBUG_LEVEL, format, args...)
}

func Debug(args ...interface{}) {
	template(DEBUG_LEVEL, "%v", args...)
}

func InfoF(format string, args ...interface{}) {
	template(INFO_LEVEL, format, args...)
}

func Info(args ...interface{}) {
	template(INFO_LEVEL, "%v", args...)
}

func WarnF(format string, args ...interface{}) {
	template(WARN_LEVEL, format, args...)
}

func Warn(args ...interface{}) {
	template(WARN_LEVEL, "%v", args...)
}

func ErrorF(format string, args ...interface{}) {
	template(ERROR_LEVEL, format, args...)
}

func Error(args ...interface{}) {
	template(ERROR_LEVEL, "%v", args...)
}

func Panic(args ...interface{}) {
	template(PANIC_LEVEL, "%v", args...)
	os.Exit(1)
	//log.Panicf(time.Now().Format("2006-01-02 15:04:05.000"),
	//	"PANIC",
	//	fmt.Sprint(args...),
	//)
}

func PanicF(format string, args ...interface{}) {
	template(PANIC_LEVEL, format, args...)
	os.Exit(1)
	//log.Panicf(time.Now().Format("2006-01-02 15:04:05.000"),
	//	"PANIC",
	//	fmt.Sprint(args...),
	//)
}
