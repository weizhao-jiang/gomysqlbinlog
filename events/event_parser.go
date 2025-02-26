package events

import (
	"encoding/base64"
	"fmt"
	"gomysqlbinlog/binlog_header"
	"gomysqlbinlog/event_types"
	"gomysqlbinlog/events/dml_event"
	"gomysqlbinlog/events/gtid"
	"gomysqlbinlog/options_handler"
	"gomysqlbinlog/utils"
	"gomysqlbinlog/utils/event_ops"
	"gomysqlbinlog/utils/logx"
	"hash/crc32"
	"time"
)

type EventParser struct {
	event_ops.EventDetailReader //仅用于字节转换
	TabMap                      *dml_event.TablemapEvent
	Options                     *options_handler.Options
	Ev                          binlog_header.Events
	EventB64List                [][]byte
}

func (ep *EventParser) Init(ev *binlog_header.Events, HasChecksum bool) {
	if len(ev.EvBdata) == 0 || len(ev.EvBdata) <= 19 {
		logx.Panic("event bytes is nil.")
	}
	if ev == nil {
		logx.Panic("event is nil.")
	}
	ep.Ev = *ev
	if HasChecksum {
		ep.Ev.EvBdata = ep.Ev.EvBdata[:len(ep.Ev.EvBdata)-4]
	}
}

func (ep *EventParser) Parse(nextEvType int) error {
	switch ep.Ev.EvHeader.Event_data.Event_type {
	case event_types.ANONYMOUS_GTID_LOG_EVENT:
		ep.Ev.PrintEvHeaderInfo()
		logx.Output("SET @@SESSION.GTID_NEXT= 'AUTOMATIC' %s\n", ep.Options.DELIMITER)

	case event_types.GTID_LOG_EVENT:
		x := gtid.GTID_EVENT{}
		x.Init(ep.Ev.EvBdata)
		ep.Ev.PrintEvHeaderInfo()
		if ep.Options.SkipGtids {
			logx.Output("SET @@SESSION.GTID_NEXT='AUTOMATIC' %s\n", ep.Options.DELIMITER)
		} else {
			logx.Output("/*!80001 SET @@session.original_commit_timestamp=%d*/ %s\n", x.Original_commit_timestamp, ep.Options.DELIMITER)
			logx.Output("/*!80014 SET @@session.original_server_version=%d*/ %s\n", x.Original_server_version, ep.Options.DELIMITER)
			logx.Output("/*!80014 SET @@session.immediate_server_version=%d*/ %s\n", x.Immediate_server_version, ep.Options.DELIMITER)
			logx.Output("SET @@SESSION.GTID_NEXT='%s:%d' %s\n", x.SID, x.GNO, ep.Options.DELIMITER)
		}

	case event_types.XID_EVENT:
		x := event_ops.EventDetailReader{}
		ep.Ev.PrintEvHeaderInfo()
		logx.Output("COMMIT /* XID %v */ %s\n", x.Read_uint_try(ep.Ev.EvBdata[19:19+8], ""), ep.Options.DELIMITER)

	case event_types.QUERY_EVENT: // DDL 不做回滚(不好实现 =……=)
		y := event_ops.EventDetailReader{}
		x := QueryEvent{}
		x.Init(ep.Ev.EvBdata)
		extraMsg := fmt.Sprintf("   thread_id=%d    exec_time=%d    error_code=%d", x.Thread_id, x.Query_exec_time, x.Error_code)
		ep.Ev.PrintEvHeaderInfo(extraMsg)
		querytime := time.Unix(int64(y.Read_uint_try(ep.Ev.EvBdata[:4], "")), 0)
		logx.Output("SET TIMESTAMP=%v /* QUERY TIME %v */ %s\n",
			y.Read_uint_try(ep.Ev.EvBdata[:4], ""),
			querytime.Format("2006-01-02 15:04:05"),
			ep.Options.DELIMITER,
		)
		if x.Q_SQL_MODE_CODE != nil {
			logx.Output("SET @@session.sql_mode=%v %s\n", *x.QueryEventStatusVar.Q_SQL_MODE_CODE, ep.Options.DELIMITER)
		}
		if len(x.Q_AUTO_INCREMENT) != 0 {
			logx.Output("SET @@session.auto_increment_increment=%v %s\n", x.Q_AUTO_INCREMENT[0], ep.Options.DELIMITER)
		}
		if len(x.Q_CHARSET_CODE) >= 3 {
			logx.Output("SET @@session.character_set_client=%v, @@session.collation_connection=%v, @@session.collation_server=%v %s\n",
				x.Q_CHARSET_CODE[0],
				x.Q_CHARSET_CODE[1],
				x.Q_CHARSET_CODE[2],
				ep.Options.DELIMITER,
			)
		}
		if x.Q_DEFAULT_COLLATION_FOR_UTF8MB4 != nil {
			logx.Output("/*!80011 SET @@session.default_collation_for_utf8mb4=%v */%s\n", *x.Q_DEFAULT_COLLATION_FOR_UTF8MB4, ep.Options.DELIMITER)
		}
		if len(x.M_db) != 0 {
			logx.Output("USE %v %s\n", x.M_db, ep.Options.DELIMITER)
		} else if len(x.Q_UPDATED_DB_NAMES) != 0 {
			logx.Output("USE %v %s\n", x.Q_UPDATED_DB_NAMES, ep.Options.DELIMITER)
		}

		if len(x.M_query) >= 19 {
			if string(x.M_query[len(x.M_query)-17:]) == "START TRANSACTION" {
				logx.Output("START TRANSACTION %s \n", ep.Options.DELIMITER)
				// if string(x.M_query[len(x.M_query)-19:]) == "START TRANSACTION" {
				// 	outputer += "START TRANSACTION " + ep.DELIMITER + "\n"
				// }
			} else {
				logx.Output("%v %s\n", string(x.M_query), ep.Options.DELIMITER)
			}
		} else {
			// 兼容MySQL 5.5
			if string(x.M_query) == "B" {
				logx.Output("BEGIN %s\n", ep.Options.DELIMITER)
			} else {
				logx.Output("%v %s\n", string(x.M_query), ep.Options.DELIMITER)
			}
		}

		if x.Q_DDL_LOGGED_WITH_XID != nil {
			logx.Output("COMMIT /* XID %v */ %s\n", *x.Q_DDL_LOGGED_WITH_XID, ep.Options.DELIMITER)
		}

	case event_types.WRITE_ROWS_EVENT, event_types.UPDATE_ROWS_EVENT, event_types.DELETE_ROWS_EVENT:

		if ep.TabMap == nil {
			logx.Panic("tableMap events is missing!")
		}
		re := dml_event.RowEvent{}
		re.Init(ep.Ev.EvBdata, ep.TabMap)
		ep.Ev.PrintEvHeaderInfo(fmt.Sprintf("table id %d", re.RowEventHeader.Table_id))

		if ep.Options.ToSQL {
			for _, v := range re.Get_sql(ep.Options) {
				logx.Output("-- %s /* %v */ %s\n", v, ep.Ev.EvName, ep.Options.DELIMITER)
			}
		} else if ep.Options.ToBase64 {
			ep.EventB64List = append(ep.EventB64List, re.Get_bin(ep.Options))

			if nextEvType != event_types.WRITE_ROWS_EVENT && nextEvType != event_types.UPDATE_ROWS_EVENT && nextEvType != event_types.DELETE_ROWS_EVENT {
				logx.Output("BINLOG '\n")
				for _, v := range ep.EventB64List {
					logx.Output("%s\n", base64.StdEncoding.EncodeToString(v))
				}
				logx.Output("'%s\n", ep.Options.DELIMITER)
				ep.EventB64List = nil
			}
		}

	case event_types.TABLE_MAP_EVENT:
		tp := dml_event.TablemapEvent{}
		tp.Init(ep.Ev.EvBdata, ep.EventDetailReader.Offset)

		ep.Ev.PrintEvHeaderInfo(fmt.Sprintf("Table_map:`%s`.`%s` mapped to %d", tp.Dbname, tp.Table_name, tp.Table_id))
		ep.TabMap = &tp
		if ep.Options.ToBase64 {
			checksum := crc32.ChecksumIEEE(tp.EventDetailReader.Bdata)
			checksumB, errx := utils.Uint32Tobytes(checksum)
			if errx != nil {
				logx.ErrorF("get checksum failed. %s", errx.Error())

			}
			ep.EventB64List = append(ep.EventB64List, append(tp.EventDetailReader.Bdata, checksumB...))
		}
	}
	return nil
}
