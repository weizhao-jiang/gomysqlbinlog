package events

import (
	"gomysqlbinlog/event_types"
	"gomysqlbinlog/events/gtid"
	"gomysqlbinlog/options_handler"
	"gomysqlbinlog/utils"
	"gomysqlbinlog/utils/event_ops"
	"gomysqlbinlog/utils/logx"
	"fmt"
	"strconv"
)

/*
	低版本(如5.5)不一定有GTI,这里不考虑这种情况

	DDL正常事务顺序:
	1. Gtid 或者 anonymous_gtid
	2. Query (第一个Query.M_query 不是 BEGIN 的情况下是DDL)

	低版本MySQL(5.5) DDL正常事务顺序:
	2. Query (第一个Query.M_query 不是 B 的情况下是DDL)

	DML正常事务顺序1:
	1. Gtid 或者 anonymous_gtid
	2. Query
	3. Table_map
	4. Write_rows Update_rows
	3. Table_map
	4. Write_rows Update_rows (3、4或者4(load data infile ...)无限重复，每一条DML语句执行，重复一次.又或者33334444都可能)
	5. Xid (commit)

	低版本MySQL(5.5) DML正常事务顺序1:
	2. Query (第一个Query.M_query == B)
	3. Table_map
	4. Write_rows Update_rows
	3. Table_map
	4. Write_rows Update_rows (3、4或者4(load data infile ...)无限重复，每一条DML语句执行，重复一次.又或者33334444都可能)
	5. Xid (commit)

	DML正常事务顺序2:
	1. Gtid 或者 anonymous_gtid
	2. Query (第一个Query.M_query == BEGIN)
	3. (2 无限重复)
	4. Xid (commit)

	低版本MySQL(5.5) DML正常事务顺序2:
	1. Query (第一个Query.M_query == B)
	2. (2 无限重复)
	3. Xid (commit)


	其他操作事件
	Rotate (flush logs)
	...
*/

// 读取一个事务内的所有event
func ReadTrxEvents(r *utils.FileReaders, o *options_handler.Options) *Filter {
	var TRX_END bool = false

	var eventFlagOrder []string = make([]string, 0)
	var eventFlagList map[string]bool = make(map[string]bool, 0)
	dr := event_ops.EventDetailReader{}
	var filter *Filter = new(Filter)

	for !TRX_END {
		if filter.TrxBeginPos == 0 {
			filter.TrxBeginPos = uint(r.CurrPos())
		}
		ev := Read_event(r, o)
		if ev == nil {
			// 事务不完整或者不存在任何事务
			filter.TrxEventList = nil
			break
		}
		if ev.EvBdata == nil {
			// 事务不完整或者不存在任何事务
			filter.TrxEventList = nil
			break
		}

		rowsEventName := ""
		eventType := dr.Read_uint_try(ev.EvBdata[4:5], "big")
		switch eventType {
		case event_types.GTID_LOG_EVENT, event_types.ANONYMOUS_GTID_LOG_EVENT:
			if eventType == event_types.GTID_LOG_EVENT {
				rowsEventName = "GTID_LOG_EVENT"
			} else {
				rowsEventName = "ANONYMOUS_GTID_LOG_EVENT"
			}
			// GTID 事务开始
			x := gtid.GTID_EVENT{Event_type: uint(eventType)}
			x.Init(ev.EvBdata)
			// 标记顺序
			eventFlagList["GTID_EVENT"] = true
			eventFlagOrder = append(eventFlagOrder, "GTID_EVENT")

			// 记录过滤信息
			filter.TrxEventList = nil
			filter.TrxGTID = fmt.Sprintf("%s:%d", x.SID, x.GNO)
			filter.TrxBeginPos = uint(ev.StartPos)
			filter.TrxBeginTime = uint64(ev.EvHeader.Event_data.Timestamp)

		case event_types.ROWS_QUERY_LOG_EVENT:
			// DDL、DML操作
			// binlog_rows_query_log_events 将原始SQL也记录到binlog里面
			rowsEventName = "ROWS_QUERY_LOG_EVENT"
			// 标记顺序
			eventFlagList["ROWS_QUERY_LOG_EVENT"] = true
			eventFlagOrder = append(eventFlagOrder, "ROWS_QUERY_LOG_EVENT")

		case event_types.QUERY_EVENT:
			// 'BEGIN'/'B' --  DML
			rowsEventName = "QUERY_EVENT"
			M_query := string(ev.EvBdata[len(ev.EvBdata)-9 : len(ev.EvBdata)-4])
			dmlFlag := "BEGIN"
			lastEventName := ""
			if len(eventFlagOrder) == 0 {
				dmlFlag = "B"
				M_query = string(ev.EvBdata[len(ev.EvBdata)-5 : len(ev.EvBdata)-4])
			} else {
				lastEventName = eventFlagOrder[len(eventFlagOrder)-1]
			}

			// MySQL5.6 开始支持GTID，则5.5的都是QUERY_EVENT 开头的
			if M_query != dmlFlag {
				// DDL
				// DDL event M_query 位于事件末端，记录实际执行的DDL语句或者BEGIN(DML开始事务)
				// DDL event 到此结束
				// 如果连续出现QUERY EVENT， 则进入 DML正常事务顺序2
				if lastEventName == "QUERY_EVENT" {
					eventFlagOrder = append(eventFlagOrder, "+1")
				} else if string(lastEventName[0]) == "+" {
					x, err := strconv.Atoi(lastEventName[1:])
					if err == nil {
						eventFlagOrder[len(eventFlagOrder)-1] = fmt.Sprintf("+%d", x+1)
					}

				} else {
					// 否则才是DDL操作
					TRX_END = true
					filter.TrxEndTime = uint64(ev.EvHeader.Event_data.Timestamp)
					eventFlagOrder = append(eventFlagOrder, rowsEventName)
				}
			} else {
				// DML event 继续
				TRX_END = false
				eventFlagOrder = append(eventFlagOrder, rowsEventName)
			}

			// 标记顺序
			eventFlagList[rowsEventName] = true

		case event_types.TABLE_MAP_EVENT:
			//tme := dml_event.TablemapEvent{}
			//tme.Init(eventBdata, uint(r.CurrPos()-int64(len(eventBdata))))
			rowsEventName = "TABLE_MAP_EVENT"
			// 标记顺序
			eventFlagList["TABLE_MAP_EVENT"] = true
			eventFlagOrder = append(eventFlagOrder, "TABLE_MAP_EVENT")

		case event_types.DELETE_ROWS_EVENT_V1,
			event_types.DELETE_ROWS_EVENT,
			event_types.UPDATE_ROWS_EVENT,
			event_types.UPDATE_ROWS_EVENT_V1,
			event_types.WRITE_ROWS_EVENT,
			event_types.WRITE_ROWS_EVENT_V1:

			switch uint(eventType) {
			case event_types.DELETE_ROWS_EVENT_V1:
				rowsEventName = "DELETE_ROWS_EVENT_V1"
			case event_types.DELETE_ROWS_EVENT:
				rowsEventName = "DELETE_ROWS_EVENT"
			case event_types.UPDATE_ROWS_EVENT:
				rowsEventName = "UPDATE_ROWS_EVENT"
			case event_types.UPDATE_ROWS_EVENT_V1:
				rowsEventName = "UPDATE_ROWS_EVENT_V1"
			case event_types.WRITE_ROWS_EVENT:
				rowsEventName = "WRITE_ROWS_EVENT"
			case event_types.WRITE_ROWS_EVENT_V1:
				rowsEventName = "WRITE_ROWS_EVENT_V1"
			}

			if string(eventFlagOrder[len(eventFlagOrder)-1][0]) != "+" {
				switch eventFlagOrder[len(eventFlagOrder)-1] {
				case "WRITE_ROWS_EVENT", "WRITE_ROWS_EVENT_V1":
				case "DELETE_ROWS_EVENT_V1", "DELETE_ROWS_EVENT":
				case "UPDATE_ROWS_EVENT", "UPDATE_ROWS_EVENT_V1":
				case "TABLE_MAP_EVENT":
					// to do
				default:
					logx.ErrorF("Strange events, skipped it.%v", eventFlagOrder[len(eventFlagOrder)-1][:6])
					continue
				}
			}
			// 标记顺序
			eventFlagList["ROWS_EVENT"] = true
			if rowsEventName == eventFlagOrder[len(eventFlagOrder)-1] {
				eventFlagOrder = append(eventFlagOrder, "+1")
			} else {
				if string(eventFlagOrder[len(eventFlagOrder)-1][0]) == "+" {
					x, err := strconv.Atoi(eventFlagOrder[len(eventFlagOrder)-1][1:])
					if err == nil {
						eventFlagOrder[len(eventFlagOrder)-1] = fmt.Sprintf("+%d", x+1)
					}
				} else {
					eventFlagOrder = append(eventFlagOrder, rowsEventName)
				}
			}

		case event_types.XID_EVENT:
			if len(filter.TrxEventList) > 1 && eventFlagList["GTID_EVENT"] && eventFlagList["QUERY_EVENT"] && eventFlagList["TABLE_MAP_EVENT"] && eventFlagList["ROWS_EVENT"] {
				TRX_END = true
			} else if len(filter.TrxEventList) > 1 &&
				eventFlagList["GTID_EVENT"] && eventFlagList["QUERY_EVENT"] && eventFlagList["TABLE_MAP_EVENT"] && eventFlagList["ROWS_QUERY_LOG_EVENT"] && eventFlagList["ROWS_EVENT"] {
				TRX_END = true
			} else if len(filter.TrxEventList) > 1 &&
				eventFlagList["GTID_EVENT"] && eventFlagList["QUERY_EVENT"] && string(eventFlagOrder[len(eventFlagOrder)-1][0]) == "+" {
				TRX_END = true
			} else if len(filter.TrxEventList) > 1 &&
				eventFlagList["QUERY_EVENT"] && string(eventFlagOrder[len(eventFlagOrder)-1][0]) == "+" {
				// 没有GTID的情况 MySQL5.5版本
				TRX_END = true
			} else {
				filter.TrxEventList = nil
			}

			rowsEventName = "XID_EVENT"
			if TRX_END {
				filter.TrxEndTime = uint64(ev.EvHeader.Event_data.Timestamp)
				// logx.DebugF("READ TRX EVENT FINISH. offset:%d --> %d", filter.TrxBeginPos, filter.TrxEndPos)
				eventFlagOrder = append(eventFlagOrder, rowsEventName)
			} else {
				// 事务未结束，未知情况
				// eventFlagOrder = make([]string, 0)
				// eventFlagList = make(map[string]bool, 0)
				logx.Panic("TRX EVENT NOT FINISH yet!")
			}
		case event_types.INTVAR_EVENT:
			x := IntvarEvent{}
			x.Init(ev.EvBdata)
			x.ToString()
			continue
		default:
			logx.WarnF("SKIP EVENT TYPE:%d", eventType)
			continue
		}

		logx.DebugF("[%v] event add. size: %d", rowsEventName, len(ev.EvBdata))
		ev.EvName = rowsEventName
		filter.TrxEventList = append(filter.TrxEventList, *ev)
		logx.DebugF("TRX EVENT ORDER: %v", eventFlagOrder)
	}
	if filter.TrxEventList == nil {
		logx.DebugF("Empty transaction events.")
	} else {
		logx.DebugF("READ TRX EVENT FINISH. offset:%d --> %d ,len:%d bytes", filter.TrxBeginPos, r.CurrPos(), len(filter.TrxEventList))
	}
	filter.TrxEndPos = uint(r.CurrPos())
	return filter
}
