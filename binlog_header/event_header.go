package binlog_header

import (
	"fmt"
	"gomysqlbinlog/utils/event_ops"
	"gomysqlbinlog/utils/logx"
	"time"
)

/*

	https://dev.mysql.com/doc/dev/mysql-server/latest/page_protocol_replication_binlog_event.html#sect_protocol_replication_binlog_event_header
	https://dev.mysql.com/doc/dev/mysql-server/latest/classmysql_1_1binlog_1_1event_1_1Log__event__header.html
-----------------------------------------------------------------------------------------------------------------------------------------------------
| type                              | name       | description 																						|
-----------------------------------------------------------------------------------------------------------------------------------------------------
| 4                                 | timestamp  | seconds since unix epoch                                                                         |
| 1                                 | event_type | See mysql::binlog::event::Log_event_type                                                         |
| 4                                 | server-id  | server-id of the originating mysql-server. Used to filter out events in circular replication     |
| 4                                 | event-size | size of the event (header, post-header, body)                                                    |
| if binlog-version > 1 4 else 0    | log-pos    | position of the next event                                                                       |
| if binlog-version > 1 2 else 0    | flags      | See Binlog Event Header Flags                                                                    |
----------------------------------------------------------------------------------------------------------------------------------------------------

*/

type EventHeader struct {
	event_ops.EventDetailReader
	Event_data event_data
}

type event_data struct {
	Timestamp    uint32
	TimestampStr string
	Event_type   uint8
	Server_id    uint32
	Event_size   uint32
	Log_pos      uint32
	Flags        uint16
}

func (eh *EventHeader) Init(bdata []byte) {
	eh.EventDetailReader.Init(bdata)
	eh.Parse()
	// eh.ToString()
}

func (eh *EventHeader) Parse() {
	/*# https://dev.mysql.com/doc/dev/mysql-server/latest/page_protocol_replication_binlog_event.html
	# libbinlogevents/src/binlog_event.cpp:204:Log_event_header
	---------------------- ---------------------------------------------------
	|  timestamp     |     4 bytes    |    seconds since unix epoch          |
	|  event_type    |     1 byte     |    event类型                          |
	|  server_id     |     4 bytes    |    执行这个event的server_id            |
	|  event_size    |     4 bytes    |    这个event大小(含event_header)       |
	|  log_pos       |     4 bytes    |    距离下一个event的位置                |
	|  flags         |     2 bytes    |    flags                             |
	---------------------- ---------------------------------------------------
	*/
	eh.Event_data.Timestamp = uint32(eh.Read_uint(4))
	eh.Event_data.TimestampStr = time.Unix(int64(eh.Event_data.Timestamp), 0).Format("2006-01-02 15:04:05")
	eh.Event_data.Event_type = uint8(eh.Read_uint(1))
	eh.Event_data.Server_id = uint32(eh.Read_uint(4))
	eh.Event_data.Event_size = uint32(eh.Read_uint(4))
	eh.Event_data.Log_pos = uint32(eh.Read_uint(4))
	eh.Event_data.Flags = uint16(eh.Read_uint(2))
}

func (eh *EventHeader) ToString() string {
	output := fmt.Sprintf("Timestamp:%v ,Event_type:%v ,Server_id:%v ,Event_size:%v ,Log_pos:%v ,Flags:%v",
		eh.Event_data.Timestamp,
		eh.Event_data.Event_type,
		eh.Event_data.Server_id,
		eh.Event_data.Event_size,
		eh.Event_data.Log_pos,
		eh.Event_data.Flags,
	)
	logx.Debug(output)
	return output
}
