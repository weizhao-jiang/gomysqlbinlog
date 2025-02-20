package events

import (
	"gomysqlbinlog/binlog_header"
	"gomysqlbinlog/event_types"
	"gomysqlbinlog/options_handler"
	"gomysqlbinlog/utils/event_ops"
	"gomysqlbinlog/utils/logx"
)

type Filter struct {
	event_ops.EventDetailReader
	TrxEventList []binlog_header.Events
	TrxGTID      string
	TrxBeginTime uint64
	TrxEndTime   uint64
	TrxBeginPos  uint
	TrxEndPos    uint
}

func (f *Filter) preFilter(op *options_handler.Options) (IsMatched bool) {
	if op.StartDatetime != nil && op.StartPos != nil {
		if (op.StartDatetime.Unix() >= int64(f.TrxBeginTime) && op.StartDatetime.Unix() <= int64(f.TrxEndTime)) &&
			(*op.StartPos >= f.TrxBeginPos && *op.StartPos <= f.TrxEndPos) {
			return true
		} else {
			return false
		}
	} else {
		if op.StartDatetime != nil {
			if op.StartDatetime.Unix() >= int64(f.TrxBeginTime) && op.StartDatetime.Unix() <= int64(f.TrxEndTime) {
				return true
			} else {
				return false
			}
		}
		if op.StartPos != nil {
			if *op.StartPos >= f.TrxBeginPos && *op.StartPos <= f.TrxEndPos {
				return true
			} else {
				return false
			}
		}
		return true
	}

}

func (f *Filter) BeginEventCutter(op *options_handler.Options) {
	// 当前事务匹配之后，对当前事务上边界进行裁剪
	var borderMatch bool
	if op.StartDatetime == nil && op.StartPos == nil {
		return
	}
	if op.StartPos != nil {
		if *op.StartPos == f.TrxBeginPos {
			return
		}
	}
	if op.StartDatetime != nil {
		if op.StartDatetime.Unix() == int64(f.TrxBeginTime) {
			return
		}
	}

	for evIdx, ev := range f.TrxEventList {
		if op.StopDatetime != nil {
			if int64(ev.EvHeader.Event_data.Timestamp) == op.StartDatetime.Unix() {
				borderMatch = true
			}
		}
		if op.StartPos != nil {

			if *op.StartPos >= uint(ev.StartPos) && *op.StartPos < uint(ev.StopPos) {
				borderMatch = true
			}
		}
		if borderMatch {
			idxOffset := 0
			for {
				rowsEv := true
				switch f.TrxEventList[evIdx-idxOffset].EvHeader.Event_data.Event_type {
				case event_types.WRITE_ROWS_EVENT,
					event_types.WRITE_ROWS_EVENT_V1,
					event_types.UPDATE_ROWS_EVENT,
					event_types.UPDATE_ROWS_EVENT_V1,
					event_types.DELETE_ROWS_EVENT,
					event_types.DELETE_ROWS_EVENT_V1:
					rowsEv = true
					idxOffset++
					if evIdx-idxOffset < 0 {
						logx.Panic("Invalid trx events!")
					}
				default:
					rowsEv = false
				}

				if !rowsEv {
					break
				}
			}
			f.TrxEventList = f.TrxEventList[evIdx-idxOffset:]
			return
		}
	}
}

func (f *Filter) EndEventCutter(op *options_handler.Options) (borderMatch bool) {
	// 当前事务匹配之后，判断一下当前事务末端是否满足下边界，如果满足，则对事务进行裁剪
	if op.StopDatetime == nil && op.StopPos == nil {
		return
	}
	if op.StopPos != nil {
		if *op.StopPos >= f.TrxEndPos {
			return
		}
	}
	if op.StopDatetime != nil {
		if op.StopDatetime.Unix() >= int64(f.TrxEndTime) {
			return
		}
	}
	for evIdx, ev := range f.TrxEventList {
		if op.StopDatetime != nil && op.StopPos != nil {
			if int64(ev.EvHeader.Event_data.Timestamp) >= op.StopDatetime.Unix() || uint(ev.StopPos) >= *op.StopPos {
				borderMatch = true
			}
		} else {
			if op.StopDatetime != nil {
				if int64(ev.EvHeader.Event_data.Timestamp) >= op.StopDatetime.Unix() {
					borderMatch = true
				}
			}
			if op.StopPos != nil {
				if uint(ev.StopPos) >= *op.StopPos {
					borderMatch = true
				}
			}
			// 结束时间与结束位置都为空
		}

		if borderMatch {
			f.TrxEventList = f.TrxEventList[:evIdx+1]
			// if evIdx+1 <= len(f.TrxEventList) {
			// 	f.TrxEventList = f.TrxEventList[:evIdx+1]
			// } else {
			// 	f.TrxEventList = f.TrxEventList[:evIdx]
			// }

			return
		}
	}
	return
}

func (f *Filter) aftFilter(op *options_handler.Options) (IsMatched bool) {
	if op.StopDatetime != nil && op.StopPos != nil {
		if int64(f.TrxEndTime) <= op.StopDatetime.Unix() && f.TrxEndPos <= *op.StopPos {
			return true
		} else {
			return false
		}
	} else {
		if op.StopDatetime != nil {
			if int64(f.TrxEndTime) <= op.StopDatetime.Unix() {
				return true
			} else {
				return false
			}
		}
		if op.StopPos != nil {
			if f.TrxEndPos <= *op.StopPos {
				return true
			} else {
				return false
			}
		}
		// 结束时间与结束位置都为空
		return true
	}

}

func (f *Filter) DoFilter(op *options_handler.Options, Filtering *bool) (IsMatched, IsFinish bool) {
	// 如指定了GTID，则优先匹配GTID
	if op.IncludeGtids != nil {
		if *op.IncludeGtids == f.TrxGTID {
			// 匹配一次就返回
			return true, true
		} else {
			return false, false
		}

	}

	if !*Filtering {
		if f.preFilter(op) {
			// 如果匹配成功，则进入匹配阶段
			*Filtering = true
			f.BeginEventCutter(op)
			if f.EndEventCutter(op) {
				// 下边界匹配，则匹配完成
				return true, true
			}
			// 如下边界没有匹配，则进入匹配且匹配未结束
			return true, false
		} else {
			// 如果没有匹配，则继续寻找
			if op.StopPos != nil {
				if f.TrxBeginPos > *op.StopPos {
					return false, true
				}
			}
			if op.StopDatetime != nil {
				if int64(f.TrxBeginTime) > op.StopDatetime.Unix() {
					return false, true
				}
			}

			return false, false
		}
	}

	if f.aftFilter(op) {
		if f.EndEventCutter(op) {
			// 下边界匹配，则匹配完成
			return true, true
		}
		return true, false
	} else {
		// 如果没有匹配，则匹配结束
		return false, true
	}

}
