package events

import (
	"gomysqlbinlog/event_types"
	"gomysqlbinlog/utils/event_ops"
	"gomysqlbinlog/utils/logx"
)

// https://dev.mysql.com/doc/dev/mysql-server/8.0.38/classbinary__log_1_1Query__event.html
type QueryEvent struct {
	event_ops.EventDetailReader
	QueryEventHeader
	QueryEventBody
}

type QueryEventHeader struct {
	Thread_id       uint32 // 4
	Query_exec_time uint32 // 4
	Db_len          uint8  // 1
	Error_code      uint16 // 2
	Status_vars_len uint16 // 2
}

type QueryEventBody struct {
	Status_vars []byte
	M_db        string // Status_vars_len + 1(b'\x00')
	M_query     []byte // len(event) - 4 + 4 + 1 + 2 + 2 + 1
	QueryEventStatusVar
}

type QueryEventStatusVar struct {
	Q_FLAGS2_CODE                     *uint             // Q_FLAGS2_CODE == 0
	Q_SQL_MODE_CODE                   *uint             // Q_SQL_MODE_CODE == 1
	Q_CATALOG_CODE                    *uint             // Q_CATALOG_CODE == 2
	Q_AUTO_INCREMENT                  [2]uint           // Q_AUTO_INCREMENT == 3
	Q_CHARSET_CODE                    [3]uint           // Q_CHARSET_CODE == 4
	Q_TIME_ZONE_CODE                  *string           // Q_TIME_ZONE_CODE == 5
	Q_CATALOG_NZ_CODE                 *string           // Q_CATALOG_NZ_CODE == 6
	Q_LC_TIME_NAMES_CODE              *uint             // Q_LC_TIME_NAMES_CODE == 7
	Q_CHARSET_DATABASE_CODE           *uint             // Q_CHARSET_DATABASE_CODE == 8
	Q_TABLE_MAP_FOR_UPDATE_CODE       *uint             // Q_TABLE_MAP_FOR_UPDATE_CODE == 9
	Q_MASTER_DATA_WRITTEN_CODE        *uint             // Q_MASTER_DATA_WRITTEN_CODE == 10
	Q_INVOKER                         map[string]string // Q_INVOKER == 11
	Q_UPDATED_DB_NAMES                []string          // Q_UPDATED_DB_NAMES == 12
	Q_MICROSECONDS                    *uint             // Q_MICROSECONDS == 13
	Q_COMMIT_TS                       *uint             // Q_COMMIT_TS == 14
	Q_COMMIT_TS2                      *uint             // Q_COMMIT_TS2 == 15
	Q_EXPLICIT_DEFAULTS_FOR_TIMESTAMP *bool             // Q_EXPLICIT_DEFAULTS_FOR_TIMESTAMP
	Q_DDL_LOGGED_WITH_XID             *uint             // Q_DDL_LOGGED_WITH_XID
	Q_DEFAULT_COLLATION_FOR_UTF8MB4   *uint             // Q_DEFAULT_COLLATION_FOR_UTF8MB4
	Q_SQL_REQUIRE_PRIMARY_KEY         *uint             // Q_SQL_REQUIRE_PRIMARY_KEY
	Q_DEFAULT_TABLE_ENCRYPTION        *uint             // Q_DEFAULT_TABLE_ENCRYPTION

}

func (q *QueryEvent) Init(bdata []byte) {
	q.EventDetailReader.Init(bdata)
	// 跳过event header
	q.Read(event_types.EVENT_HEAD_SIZE)

	q.QueryEventHeader.Thread_id = uint32(q.Read_uint(4))
	q.QueryEventHeader.Query_exec_time = uint32(q.Read_uint(4))
	q.QueryEventHeader.Db_len = uint8(q.Read_uint(1))
	q.QueryEventHeader.Error_code = uint16(q.Read_uint(2))
	q.QueryEventHeader.Status_vars_len = uint16(q.Read_uint(2))

	// 读取dbname Status_vars
	q.QueryEventBody.Status_vars = q.Read(uint(q.QueryEventHeader.Status_vars_len))
	q.QueryEventBody.M_db = q.Read_String(uint(q.QueryEventHeader.Db_len))

	//b'\x00'  分隔符
	q.Read(1)

	//SQL Query 如果执行SQL为DDL，M_query则显示DDL语句
	q.QueryEventBody.M_query = q.Read(uint(len(bdata) - int(q.Offset)))

	// 打印信息 debug
	logx.DebugF(`
Thread_id        :%v
Query_exec_time  :%v
Db_len           :%v
Dbname           :%v
Error_code       :%v
Status_vars_len  :%v
Status_vars      :%v
M_query          :%v`,
		q.Thread_id,
		q.Query_exec_time,
		q.Db_len,
		q.M_db,
		q.Error_code,
		q.Status_vars_len,
		q.Status_vars,
		string(q.QueryEventBody.M_query))

	// 解析 Status_vars
	ev := event_ops.EventDetailReader{}
	ev.Init(q.QueryEventBody.Status_vars)
	for {
		data := ev.Read(1)
		if data == nil {
			break
		}
		identifier := ev.Read_uint_try(data, "little")
		if identifier == 0 {
			// Q_FLAGS2_CODE == 0  4 byte bitfield
			q.QueryEventStatusVar.Q_FLAGS2_CODE = new(uint)
			*q.QueryEventStatusVar.Q_FLAGS2_CODE = ev.Read_uint(4)
		} else if identifier == 1 {
			// Q_SQL_MODE_CODE == 1   8 byte bitfield
			q.QueryEventStatusVar.Q_SQL_MODE_CODE = new(uint)
			*q.QueryEventStatusVar.Q_SQL_MODE_CODE = ev.Read_uint(8)

		} else if identifier == 2 {
			// 	The status variable Q_CATALOG_CODE == 2 existed in MySQL 5.0.x, where 0<=x<=3. It was identical to Q_CATALOG_CODE, except that the string had a trailing '\0'. The '\0' was removed in 5.0.4 since it was redundant (the string length is stored before the string). The Q_CATALOG_CODE will never be written by a new master, but can still be understood by a new slave.
			continue
			//q.QueryEventStatusVar.Q_CATALOG_CODE = ev.Read_uint(1)

		} else if identifier == 3 {
			// Q_AUTO_INCREMENT == 3    two 2 byte unsigned integers
			q.QueryEventStatusVar.Q_AUTO_INCREMENT[0] = ev.Read_uint(2)
			q.QueryEventStatusVar.Q_AUTO_INCREMENT[1] = ev.Read_uint(2)

		} else if identifier == 4 {
			// Q_CHARSET_CODE == 4     three 2 byte unsigned integers
			q.QueryEventStatusVar.Q_CHARSET_CODE[0] = ev.Read_uint(2)
			q.QueryEventStatusVar.Q_CHARSET_CODE[1] = ev.Read_uint(2)
			q.QueryEventStatusVar.Q_CHARSET_CODE[2] = ev.Read_uint(2)

		} else if identifier == 5 {
			// Q_TIME_ZONE_CODE == 5     Variable-length string: the length in bytes (1 byte) followed by the characters (at most 255 bytes).
			q.QueryEventStatusVar.Q_TIME_ZONE_CODE = new(string)
			*q.QueryEventStatusVar.Q_TIME_ZONE_CODE = ev.Read_String(ev.Read_uint(1))

		} else if identifier == 6 {
			// Q_CATALOG_NZ_CODE == 6     Variable-length string: the length in bytes (1 byte) followed by the characters (at most 255 bytes)
			q.QueryEventStatusVar.Q_CATALOG_NZ_CODE = new(string)
			*q.QueryEventStatusVar.Q_CATALOG_NZ_CODE = ev.Read_String(ev.Read_uint(1))

		} else if identifier == 7 {
			// Q_LC_TIME_NAMES_CODE == 7    2 byte integer
			q.QueryEventStatusVar.Q_LC_TIME_NAMES_CODE = new(uint)
			*q.QueryEventStatusVar.Q_LC_TIME_NAMES_CODE = ev.Read_uint(2)

		} else if identifier == 8 {
			// Q_CHARSET_DATABASE_CODE == 8    2 byte integer
			q.QueryEventStatusVar.Q_CHARSET_DATABASE_CODE = new(uint)
			*q.QueryEventStatusVar.Q_CHARSET_DATABASE_CODE = ev.Read_uint(2)

		} else if identifier == 9 {
			// Q_TABLE_MAP_FOR_UPDATE_CODE == 9    8 byte integer
			q.QueryEventStatusVar.Q_TABLE_MAP_FOR_UPDATE_CODE = new(uint)
			*q.QueryEventStatusVar.Q_TABLE_MAP_FOR_UPDATE_CODE = ev.Read_uint(8)

		} else if identifier == 10 {
			continue
			// Q_MASTER_DATA_WRITTEN_CODE == 9    4 byte bitfield
			q.QueryEventStatusVar.Q_MASTER_DATA_WRITTEN_CODE = new(uint)
			*q.QueryEventStatusVar.Q_MASTER_DATA_WRITTEN_CODE = ev.Read_uint(4)

		} else if identifier == 11 {
			// Q_INVOKER == 11      2 Variable-length strings: the length in bytes (1 byte) followed by characters (user), again followed by length in bytes (1 byte) followed by characters(host)
			// 2 可变长度字符串：以字节为单位的长度（1 个字节），后跟字符（用户），再次以字节为单位的长度（1 个字节），后跟字符（主机）
			q.QueryEventStatusVar.Q_INVOKER = make(map[string]string, 2)
			q.QueryEventStatusVar.Q_INVOKER["user"] = ev.Read_String(ev.Read_uint(1))
			q.QueryEventStatusVar.Q_INVOKER["host"] = ev.Read_String(ev.Read_uint(1))

		} else if identifier == 12 {
			// Q_UPDATED_DB_NAMES == 12      1 byte character, and a 2-D array
			nameLen := ev.Read_uint(1)
			if nameLen > event_types.MAX_DBS_IN_EVENT_MTS {
				nameLen = event_types.OVER_MAX_DBS_IN_EVENT_MTS
			} else {
				for i := 0; i < int(nameLen); i++ {
					tm := ev.Read_Until_End(byte(0x00))
					if tm == nil {
						break
					}
					q.QueryEventStatusVar.Q_UPDATED_DB_NAMES = append(q.QueryEventStatusVar.Q_UPDATED_DB_NAMES, string(tm))
				}
			}

		} else if identifier == 13 {
			// Q_MICROSECONDS == 13      3 byte unsigned integers
			q.QueryEventStatusVar.Q_MICROSECONDS = new(uint)
			*q.QueryEventStatusVar.Q_MICROSECONDS = ev.Read_uint(3)
		} else if identifier == 14 {
			// Q_COMMIT_TS ???
			// 0
			q.QueryEventStatusVar.Q_COMMIT_TS = new(uint)
			*q.QueryEventStatusVar.Q_COMMIT_TS = 0
		} else if identifier == 15 {
			// Q_COMMIT_TS2 ???
			// 0
			q.QueryEventStatusVar.Q_COMMIT_TS2 = new(uint)
			*q.QueryEventStatusVar.Q_COMMIT_TS2 = 0
		} else if identifier == 16 {
			// Q_EXPLICIT_DEFAULTS_FOR_TIMESTAMP 1 byte boolean
			q.QueryEventStatusVar.Q_EXPLICIT_DEFAULTS_FOR_TIMESTAMP = new(bool)
			*q.QueryEventStatusVar.Q_EXPLICIT_DEFAULTS_FOR_TIMESTAMP = (ev.Read_uint(1) == 1)

		} else if identifier == 17 {
			// Q_DDL_LOGGED_WITH_XID 8 byte integer
			q.QueryEventStatusVar.Q_DDL_LOGGED_WITH_XID = new(uint)
			*q.QueryEventStatusVar.Q_DDL_LOGGED_WITH_XID = ev.Read_uint(8)

		} else if identifier == 18 {
			// Q_DEFAULT_COLLATION_FOR_UTF8MB4 2 byte integer
			q.QueryEventStatusVar.Q_DEFAULT_COLLATION_FOR_UTF8MB4 = new(uint)
			*q.QueryEventStatusVar.Q_DEFAULT_COLLATION_FOR_UTF8MB4 = ev.Read_uint(2)

		} else if identifier == 19 {
			// Q_SQL_REQUIRE_PRIMARY_KEY 2 byte integer
			q.QueryEventStatusVar.Q_SQL_REQUIRE_PRIMARY_KEY = new(uint)
			*q.QueryEventStatusVar.Q_SQL_REQUIRE_PRIMARY_KEY = ev.Read_uint(2)

		} else if identifier == 20 {
			// Q_DEFAULT_TABLE_ENCRYPTION 2 byte integer
			q.QueryEventStatusVar.Q_DEFAULT_TABLE_ENCRYPTION = new(uint)
			*q.QueryEventStatusVar.Q_DEFAULT_TABLE_ENCRYPTION = ev.Read_uint(2)

		} else {
			logx.DebugF("(TODO) new identifier: %v", identifier)
		}
	}
}
