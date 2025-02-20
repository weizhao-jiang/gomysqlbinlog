package options_handler

import (
	"flag"
	"fmt"
	"gomysqlbinlog/utils/logx"
	"os"
	"strconv"
	"time"
)

type Options struct {
	FileName      string
	StartPos      *uint
	StopPos       *uint
	StartDatetime *time.Time
	StopDatetime  *time.Time
	IncludeGtids  *string
	SkipGtids     bool
	ToSQL         bool
	ToBase64      bool
	ToBinary      bool // no support now ,todo
	ToRollback    bool
	DELIMITER     string
}

func getEnv(keyName, defVal string) string {
	if val, ok := os.LookupEnv("GO_BINLOG_" + keyName); ok {
		return val
	}
	return defVal
}

func getEnvBool(key string, defaultVal bool) bool {
	if envVal, ok := os.LookupEnv("GO_BINLOG_" + key); ok {
		envBool, err := strconv.ParseBool(envVal)
		if err == nil {
			return envBool
		}
	}
	return defaultVal
}

func InitOptions() *Options {
	var timeLocalx *time.Location
	timeLocalx, _ = time.LoadLocation("Asia/Shanghai")

	start_pos := flag.String("start-pos", getEnv("START_POS", "-1"), "Start reading the binlog at position N. Applies to the first binlog passed on the command line.")
	stop_pos := flag.String("stop-pos", getEnv("STOP_POS", "-1"), "Stop reading the binlog at position N. Applies to the last binlog passed on the command line.")
	start_datetime := flag.String("start-datetime", getEnv("START_DATETIME", "1970-01-01 00:00:00"), "Start reading the binlog at first event having a datetime equal or posterior to the argument.")
	stop_datetime := flag.String("stop-datetime", getEnv("STOP_DATETIME", "1970-01-01 00:00:00"), "Stop reading the binlog at first event having a datetime equal or posterior to the argument.")
	include_gtids := flag.String("include-gtids", getEnv("INCLUDE_GTIDS", ""), "Print events whose Global Transaction Identifiers were provided.")
	sql := flag.Bool("sql", getEnvBool("SQL", false), "Print raw sql.")
	base64 := flag.Bool("base64", getEnvBool("BASE64", false), "Print sql in base64 format.")
	rollback := flag.Bool("rollback", getEnvBool("ROLLBACK", false), "Print sql in rollback way.")
	binaryx := flag.Bool("binary", getEnvBool("BINARY", false), "Print sql in binary way.")
	version := flag.Bool("version", getEnvBool("VERSION", false), "gobinlog2sql version")
	logLevel := flag.Int("log-level", int(logx.PANIC_LEVEL), "output log level, 5:debug 4:info 3:warn 2:error 1:panic")
	skipGtids := flag.Bool("skip-gtids", getEnvBool("SKIP_GTIDS", false), "Do not preserve Global Transaction Identifiers; insteadmake the server execute the transactions as if they were new.")

	flag.Parse()
	if *version {
		fmt.Println("v0.1")
		return nil
	}

	op := Options{}
	if start_pos != nil {
		if *start_pos != "-1" {
			if Start_pos, err := strconv.ParseUint(*start_pos, 10, 64); err == nil {
				op.StartPos = new(uint)
				*op.StartPos = uint(Start_pos)
			} else {
				logx.PanicF("invalid start_pos :%v , %s ", *start_pos, err.Error())
			}
		}

	}

	if stop_pos != nil {
		if *stop_pos != "-1" {
			if Stop_pos, err := strconv.ParseUint(*stop_pos, 10, 64); err == nil {
				op.StopPos = new(uint)
				*op.StopPos = uint(Stop_pos)
			} else {
				logx.PanicF("invalid stop_pos :%v , %s ", *stop_pos, err.Error())
			}
		}

	}

	if start_datetime != nil {
		if *start_datetime != "1970-01-01 00:00:00" {
			if Start_datetime, err := time.ParseInLocation("2006-01-02 15:04:05", *start_datetime, timeLocalx); err == nil {
				op.StartDatetime = new(time.Time)
				*op.StartDatetime = Start_datetime
			} else {
				logx.PanicF("invalid start_datetime :%v , %s ", *start_datetime, err.Error())
			}
		}

	}

	if stop_datetime != nil {
		if *stop_datetime != "1970-01-01 00:00:00" {
			if Stop_datetime, err := time.ParseInLocation("2006-01-02 15:04:05", *stop_datetime, timeLocalx); err == nil {
				op.StopDatetime = new(time.Time)
				*op.StopDatetime = Stop_datetime
			} else {
				logx.PanicF("invalid stop_datetime :%v , %s ", *stop_datetime, err.Error())
			}
		}

	}

	if include_gtids != nil {
		if *include_gtids != "" {
			op.IncludeGtids = new(string)
			*op.IncludeGtids = *include_gtids
		}

	}

	if logLevel != nil {
		logx.SetLevel(logx.LOGLEVEL(*logLevel))
	}

	if len(flag.Args()) != 0 {
		op.FileName = flag.Args()[0]
	}

	op.ToSQL = *sql
	op.ToBase64 = *base64
	op.ToRollback = *rollback
	op.ToBinary = *binaryx
	op.SkipGtids = *skipGtids
	op.DELIMITER = "/*!*/;"

	// debug
	// op.FileName = "/opt/tmp/mysql-bin.000097"
	// op.FileName = "/opt/tmp/centos-bin.003028_5.5"
	// op.FileName = "/var/lib/mysql/mysql-bin.000838"
	// logx.SetLevel(logx.DEBUG_LEVEL)
	// op.StartPos = new(uint)
	// op.StopPos = new(uint)
	// *op.StartPos = 960
	// *op.StopPos = 1042
	// op.ToSQL = true
	return &op
}
