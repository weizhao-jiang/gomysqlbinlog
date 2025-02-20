package gtid

import (
	"gomysqlbinlog/binlog_header"
	"gomysqlbinlog/event_types"
	"gomysqlbinlog/utils/event_ops"
	"gomysqlbinlog/utils/logx"
)

// 处理GTID 与 匿名GTID事件
type GTID_EVENT struct {
	event_ops.EventDetailReader
	EvHeader                   binlog_header.EventHeader
	Event_type                 uint
	GTID_FLAGS                 uint8
	SID                        string
	GNO                        uint64
	Lt_type                    uint8
	Last_committed             uint64
	Sequence_number            uint64
	Has_commit_timestamps      bool
	Immediate_commit_timestamp int64
	Original_commit_timestamp  uint64
	Transaction_length         uint64
	Immediate_server_version   int64
	Original_server_version    int64
}

func (g *GTID_EVENT) Init(bdata []byte) {
	g.EventDetailReader.Init(bdata)
	g.EvHeader.Init(g.Read(event_types.EVENT_HEAD_SIZE))
	g.Parser()
	g.ToString()
}

func (g *GTID_EVENT) Parser() {
	/*
		https://dev.mysql.com/doc/dev/mysql-server/latest/classmysql_1_1binlog_1_1event_1_1Gtid__event.html
		mysql-8.0.40/libbinlogevents/src/control_events.cpp:418 Gtid_event::Gtid_event

		----------------------------------------------------------------------------------
		|  对象         |              大小  byte       |               描述               |
		----------------------------------------------------------------------------------
			GTID_FLAGS                  1
			SID                         16                            server uuid
			GNO                         8                             gtid
			lt_type                     1                             logical clock timestamp typecode
			last_committed              8 if lt_type == 2 else 0
			sequence_number             8 if lt_type == 2 else 0
			immediate_commit_timestamp  8
			original_commit_timestamp   0/8
			transaction_length          1-9
			immediate_server_version    4
			original_server_version     0/4
		----------------------------------------------------------------------------------
	*/

	g.GTID_FLAGS = uint8(g.Read_uint(1))
	g.SID = g.Read_UUID(16)
	g.GNO = uint64(g.Read_uint(8))
	g.Lt_type = uint8(g.Read_uint(1))

	/*
		获取逻辑时钟信息，先检查一下长度是否能读，避免缓冲区溢出
		Fetch the logical clocks. Check the length before reading, to
		avoid out of buffer reads.
	*/
	if int(g.EventDetailReader.Offset)+event_types.LOGICAL_TIMESTAMP_TYPECODE_LENGTH >= len(g.Bdata) {
		return
	}
	if g.Lt_type == event_types.LOGICAL_TIMESTAMP_TYPECODE {
		g.Last_committed = uint64(g.Read_uint(8))
		g.Sequence_number = uint64(g.Read_uint(8))
	}

	/*
	   Fetch the timestamps used to monitor replication lags with respect to
	   the immediate master and the server that originated this transaction.
	   Check that the timestamps exist before reading. Note that a master
	   older than MySQL-5.8 will NOT send these timestamps. We should be
	   able to ignore these fields in this case.
	*/
	// 获取用于监视相对于直接主服务器和发起此事务的服务器的复制延迟的时间戳。读取前检查时间戳是否存在。
	// 请注意，MySQL-5.8之前的主机将不会发送这些时间戳。在这种情况下，我们应该能够忽略这些字段。

	// 对于匿名事务是不存在的
	if int(g.EventDetailReader.Offset)+event_types.IMMEDIATE_COMMIT_TIMESTAMP_LENGTH > len(g.Bdata) {
		g.Has_commit_timestamps = false
		return
	}

	g.Immediate_commit_timestamp = int64(g.Read_uint(7))
	if (g.Immediate_commit_timestamp & (1 << event_types.ENCODED_COMMIT_TIMESTAMP_LENGTH)) != 0 {
		g.Original_commit_timestamp = uint64(g.Read_uint(7))
		g.Immediate_commit_timestamp &= ^(1 << event_types.ENCODED_COMMIT_TIMESTAMP_LENGTH)
	}

	g.Transaction_length = uint64(g.Read_net_int())
	g.Immediate_server_version = int64(g.Read_uint(4))

	if (g.Immediate_server_version & (1 << event_types.ENCODED_SERVER_VERSION_LENGTH)) != 0 {
		g.Immediate_server_version &= ^(1 << event_types.ENCODED_SERVER_VERSION_LENGTH)
		g.Original_server_version = int64(g.Read_uint(4))
	} else {
		g.Original_server_version = g.Immediate_server_version
	}
}

func (g *GTID_EVENT) ToString() {
	var event_type_string string = "GTID_LOG_EVENT"
	if g.Event_type == event_types.ANONYMOUS_GTID_LOG_EVENT {
		event_type_string = "ANONYMOUS_GTID_LOG_EVENT"
	}
	logx.DebugF(`GTID_EVENT INFORMATION:
EVENT_TYPE                 :%v
GTID_FLAGS                 :%v
SID(server_uuid)           :%v
GNO(GTID)                  :%v
Lt_type                    :%v
Last_committed             :%v
Sequence_number            :%v          
Immediate_commit_timestamp :%v
Original_commit_timestamp  :%v
Transaction_length         :%v
Immediate_server_version   :%v
Original_server_version    :%v`,
		event_type_string,
		g.GTID_FLAGS,
		g.SID,
		g.GNO,
		g.Lt_type,
		g.Last_committed,
		g.Sequence_number,
		g.Immediate_commit_timestamp,
		g.Original_commit_timestamp,
		g.Transaction_length,
		g.Immediate_server_version,
		g.Original_server_version,
	)
}
