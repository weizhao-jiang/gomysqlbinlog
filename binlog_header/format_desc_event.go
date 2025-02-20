package binlog_header

import (
	"fmt"
	"gomysqlbinlog/utils/event_ops"
	"gomysqlbinlog/utils/logx"
	"strings"
)

/*
	@mysql-8.0.40/libbinlogevents/src/control_events.cpp:191:Format_description_event
	https://dev.mysql.com/doc/dev/mysql-server/latest/page_protocol_replication_binlog_event.html#sect_protocol_replication_event_format_desc
*/

type Format_desc_event struct {
	event_ops.EventDetailReader
	Offset              int
	Binlog_version      int
	Mysql_version       string
	Create_timestamp    uint
	Event_header_length uint
	Event_post_header   []byte
	Checksum_alg        uint
	Checksum            uint
}

func (f *Format_desc_event) New(bdata []byte) {
	f.Init(bdata)
	f.Offset = 0
	f.Binlog_version = int(f.Read_uint(2))
	logx.DebugF("Binlog_version: %v", f.Binlog_version)

	f.Mysql_version = string(f.Read(50))
	f.Mysql_version = strings.ReplaceAll(f.Mysql_version, "\x00", "")
	logx.DebugF("Mysql_version: %v", f.Mysql_version)

	f.Create_timestamp = uint(f.Read_uint(4))
	logx.DebugF("Create_timestamp: %v", f.Create_timestamp)

	f.Event_header_length = f.Read_uint(1)
	logx.DebugF("Event_header_length: %v (Should always equal 19)", f.Event_header_length)

	/*
		@mysql-8.0.40/libbinlogevents/src/control_events.cpp:191:Format_description_event
		   if (common_header_len < LOG_EVENT_HEADER_LEN)
		     READER_THROW("Invalid Format_description common header length");

		   available_bytes = READER_CALL(available_to_read);
		   if (available_bytes == 0)
		     READER_THROW("Invalid Format_description common header length");

		   calc_server_version_split();
		   if ((ver_calc = get_product_version()) >= checksum_version_product) {
		      the last bytes are the checksum alg desc and value (or value's room)
		     available_bytes -= BINLOG_CHECKSUM_ALG_DESC_LEN;
		   }

		   number_of_event_types = available_bytes;
		   READER_TRY_CALL(assign, &post_header_len, number_of_event_types);
	*/

	if f.Mysql_version[:1] == "5" {
		f.Event_post_header = f.Read(38)
	} else if f.Mysql_version[:1] == "8" {
		f.Event_post_header = f.Read(41)
	} else if f.Mysql_version[:4] == "8.4." {
		f.Event_post_header = f.Read(43) // FOR MYSQL 8.4.x
	}
	logx.DebugF("Event_post_header:%v", func() string {
		tm := make([]string, len(f.Event_post_header))
		for k, v := range f.Event_post_header {
			tm[k] = fmt.Sprintf("%x", v)
		}
		return fmt.Sprintf("%v", tm)
	}())

	// @mysql-8.0.40/libbinlogevents/include/binlog_event.h event校验值
	f.Checksum_alg = f.Read_uint(1)

	logx.DebugF("checksum algorithm: %v (0:off 1:crc32)", f.Checksum_alg)
	if f.Checksum_alg == 1 {
		f.Checksum = f.Read_uint(4)
		logx.DebugF("checksum value: %v", f.Checksum)
	}
}
