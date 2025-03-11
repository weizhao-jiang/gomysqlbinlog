package main

import (
	"fmt"
	"gomysqlbinlog/event_types"
	"gomysqlbinlog/events"
	"gomysqlbinlog/options_handler"
	"gomysqlbinlog/utils"
	"gomysqlbinlog/utils/logx"
	"testing"
)

func TestMain(t *testing.T) {

	options := options_handler.InitOptions()
	options.ToSQL = true
	// logx.SetLevel(logx.DEBUG_LEVEL)

	options.FileName = "/var/lib/mysql/mysql-bin.000989"
	if options == nil {
		return
	}
	r := utils.FileReaders{Filename: options.FileName}
	defer r.Close()
	err := r.Init()
	if err != nil {
		logx.Panic(err)
	}

	// 判断是否为 realaylog
	data := r.Read(event_types.BINLOG_FLAG_SIZE)
	if data == nil {
		logx.Panic("failed to parse binlog.")
	}
	if fmt.Sprint(data) == fmt.Sprint([]byte(event_types.BINLOG_FLAG)) {
		logx.Debug("binlogType: normal binlog")
	} else {
		logx.Debug("binlogType: relaylog")
	}

	isFiltering := false
	// counter := 0
	for {
		filter := events.ReadTrxEvents(&r, options)

		if len(filter.TrxEventList) == 0 {
			logx.Info("trx event is empty.")
			break
		}
		isMatched, isFinish := filter.DoFilter(options, &isFiltering)
		if !isMatched && !isFinish {
			continue
		}

		if isMatched {
			ep := events.EventParser{Options: options}
			for idx, ev := range filter.TrxEventList {
				ep.Init(&ev, true)
				nextEvType := -1
				if idx+1 <= len(filter.TrxEventList)-1 {
					nextEvType = int(filter.TrxEventList[idx+1].EvHeader.Event_data.Event_type)

				}
				err := ep.Parse(nextEvType)
				if err != nil {
					logx.Error(err)
				}

			}
		}

		if isFinish {
			logx.Output("DELIMITER ;\n# Filter finish.\n")
			return
		}
	}
	logx.Output("DELIMITER ;\n# End of log file\n")
}
