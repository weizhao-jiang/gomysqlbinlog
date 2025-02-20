package events

import (
	"gomysqlbinlog/event_types"
	"gomysqlbinlog/utils/event_ops"
	"gomysqlbinlog/utils/logx"
)

/*
	@mysql-8.0.40/libbinlogevents/include/statement_events.h
	An Intvar_event will be created just before a Query_event,
  	if the query uses one of the variables LAST_INSERT_ID or INSERT_ID.
  	Each Intvar_event holds the value of one of these variables.

	------------------------------------------------
	Name  | Format                   | Description
	------------------------------------------------
	type  | 1 byte enumeration       |   One byte identifying the type of variable stored.  Currently, two identifiers are supported: LAST_INSERT_ID_EVENT == 1 and INSERT_ID_EVENT == 2
	val   | 8 byte unsigned integer  | The value of the variable.
	-------------------------------------------------
*/

// Int_event_type
const (
	INVALID_INT_EVENT    = 0
	LAST_INSERT_ID_EVENT = 1
	INSERT_ID_EVENT      = 2
)

type IntvarEvent struct {
	event_ops.EventDetailReader
	ValType     int
	ValTypeName string
	Val         int
}

func get_var_type_string(valType int) string {
	switch valType {
	case INVALID_INT_EVENT:
		return "INVALID_INT_EVENT"
	case LAST_INSERT_ID_EVENT:
		return "LAST_INSERT_ID_EVENT"
	case INSERT_ID_EVENT:
		return "INSERT_ID_EVENT"
	default:
		return "UNKNOWN"
	}
}

func (ie *IntvarEvent) Init(bdata []byte) {
	// 跳过header
	ie.EventDetailReader.Init(bdata)
	ie.Read(event_types.EVENT_HEAD_SIZE)
	ie.ValType = int(ie.Read_int(1))
	ie.ValTypeName = get_var_type_string(ie.ValType)
	ie.Val = int(ie.Read_uint(8))
}

func (ie *IntvarEvent) ToString() {
	logx.DebugF("ValTypeName: %s, Val:%d", ie.ValTypeName, ie.Val)
}
