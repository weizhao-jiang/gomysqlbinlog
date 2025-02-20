package events

import (
	"encoding/base64"
	"fmt"
	"gomysqlbinlog/binlog_header"
	"gomysqlbinlog/event_types"
	"gomysqlbinlog/options_handler"
	"gomysqlbinlog/utils"
	"gomysqlbinlog/utils/event_ops"
	"gomysqlbinlog/utils/logx"
	"hash/crc32"
)

var HasChecksum bool

// 读取一个完整不包含检验信息的event
// 跳过 PREVIOUS_GTIDS_LOG_EVENT 、 FORMAT_DESCRIPTION_EVENT 这两种event
// PREVIOUS_GTIDS_LOG_EVENT 、 FORMAT_DESCRIPTION_EVENT 只会在binlog文件切换/新建的时候出现一次
func Read_event(r *utils.FileReaders, op *options_handler.Options) *binlog_header.Events {

	var Isreturn bool
	ev := event_ops.EventDetailReader{}

	for {
		Isreturn = true
		var eb *binlog_header.Events = new(binlog_header.Events)
		eb.StartPos = int(r.CurrPos())
		header_data := r.Read(event_types.EVENT_HEAD_SIZE)
		if header_data == nil {
			logx.InfoF("binlog end.")
			break
		}

		// 解析binlog event 头部
		header := binlog_header.EventHeader{}
		header.Init(header_data)
		eb.EvHeader = header

		// 开始解析binlog event
		event_data := r.Read(uint(header.Event_data.Event_size - event_types.EVENT_HEAD_SIZE))
		eb.EvBdata = append([]byte{}, append(header_data, event_data...)...)
		eb.StopPos = int(r.CurrPos())

		//计算校验值
		if HasChecksum {
			eb.CheckSum = int(ev.Read_uint_try(eb.EvBdata[len(eb.EvBdata)-4:], "little"))
		}

		switch int(header.Event_data.Event_type) {
		case event_types.FORMAT_DESCRIPTION_EVENT:
			format_desc := binlog_header.Format_desc_event{}
			format_desc.New(event_data)
			if format_desc.Checksum > 0 {
				HasChecksum = true
				eb.CheckSum = int(ev.Read_uint_try(eb.EvBdata[len(eb.EvBdata)-4:], "little"))
			}
			Isreturn = false
			logx.Output("DELIMITER %s\n", op.DELIMITER)

			eb.EvName = "FORMAT_DESCRIPTION_EVENT"
			eb.PrintEvHeaderInfo(fmt.Sprintf("Start: binlog v%d, server v%s created %s\n",
				format_desc.Binlog_version,
				format_desc.Mysql_version,
				header.Event_data.TimestampStr,
			))

			if op.ToBase64 {
				logx.Output("BINLOG '%s'%v\n", base64.StdEncoding.EncodeToString(eb.EvBdata), op.DELIMITER)
			}

		case event_types.PREVIOUS_GTIDS_LOG_EVENT:
			x := binlog_header.Pre_gtid_event{}
			x.Init(event_data)
			eb.EvName = "PREVIOUS_GTIDS_LOG_EVENT"
			eb.PrintEvHeaderInfo()
			logx.OutputMark("# %s\n", x.ToString())
			Isreturn = false
		}
		// 打印event结束信息
		logx.DebugF("READ EVENT FINISH. EVENT TYPE:%v(%v) %v --> %v  size:%v bytes. HasChecksum:%v",
			header.Event_data.Event_type,
			Isreturn,
			eb.StartPos,
			eb.StopPos,
			eb.StopPos-eb.StartPos,
			HasChecksum,
		)

		if Isreturn {
			if HasChecksum {
				logx.DebugF("CRC32:%d CHECKSUM:%d",
					crc32.ChecksumIEEE(eb.EvBdata[:len(eb.EvBdata)-4]),
					eb.CheckSum,
				)
			}
			return eb
		}
	}
	return nil
}
