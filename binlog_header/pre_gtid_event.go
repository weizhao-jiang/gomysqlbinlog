package binlog_header

import (
	"fmt"
	"gomysqlbinlog/utils/event_ops"
	"gomysqlbinlog/utils/logx"
)

/*
  这个event 隐藏得太深了 -……-
  @mysql-8.0.40/sql/log_event.h:3989
  @class Previous_gtids_log_event

  This is the subclass of Previous_gtids_event and Log_event
  It is used to record the gtid_executed in the last binary log file,
  for ex after flush logs, or at the starting of the binary log file

  @internal
  The inheritance structure is as follows

        Binary_log_event
               ^
               |
               |
B_l:Previous_gtids_event   Log_event
                \         /
                 \       /
                  \     /
                   \   /
         Previous_gtids_log_event

  B_l: Namespace Binary_log
*/

/*
Previous_gtids_log_event
@mysql-8.0.40/sql/log_event.cc 13587
  PROPAGATE_REPORTED_ERROR_INT(
      target->add_gtid_encoding(buf, buf_size + add_size, &end_pos));
*/

/*
@mysql-8.0.40/sql/rpl_gtid_set.cc:1296:add_gtid_encoding

+-------+--------+----------+-------------+--------------+---------------+-------------------------
|sid_num|sid1    |interv_num|interv1_start|interv1_end   |interv2_start  |interv2_end |sid2    |...
|8 bytes|16 bytes|8 bytes   |8 bytes      |8 bytes       |8 bytes        |8 bytes     |16 bytes|...
+-------+--------+----------+-------------+--------------+---------------+-------------------------

例子
16ef99f0-1fd8-4686-a2f8-9f19bd5eff02:1-2,5-99,111-999;ffd096fb-96e7-46d1-9d99-2195cdaf6c8f:1-99
sid_num=2 //两个uuid
uuid:16ef99f0-1fd8-4686-a2f8-9f19bd5eff02 ffd096fb-96e7-46d1-9d99-2195cdaf6c8f
interval_num:第一个uuid有3个interval，第二个uuid只有一个interval
interval:1-2,5-99,111-99    1-99

gtid_number	8 bytes
gtid_list	gtid_number*gtid_info

	gtid_info:
		server_uuid
		group_gno_number  8 bytes (多少对连续的gno)
			start_gno 8 bytes
			stop_gno  8 bytes
*/

type Pre_gtid_event struct {
	event_ops.EventDetailReader
	GTID_counter uint
	GTID_entity  []Pre_gtid_event_body
}

type Pre_gtid_event_body struct {
	UUID           string
	Interv_counter uint
	Interv_entity  []Pre_gtid_event_intervals
}

type Pre_gtid_event_intervals struct {
	// the second (numeric) component of a GTID, is an alias of binary_log::gtids::gno_t
	Interv_start uint
	Interv_end   uint
}

func (pe *Pre_gtid_event) Init(bdata []byte) {
	pe.EventDetailReader.Init(bdata)
	pe.Parse()
	pe.ToString()
}

func (pe *Pre_gtid_event) Parse() error {
	pe.GTID_counter = pe.Read_uint(8)
	for x := uint(1); x <= pe.GTID_counter; x++ {
		peb := Pre_gtid_event_body{}
		peb.UUID = pe.Read_UUID(16)
		peb.Interv_counter = pe.Read_uint(8)
		for y := uint(1); y <= peb.Interv_counter; y++ {
			pei := Pre_gtid_event_intervals{}
			pei.Interv_start = pe.Read_uint(8)
			pei.Interv_end = pe.Read_uint(8) - 1
			peb.Interv_entity = append(peb.Interv_entity, pei)
		}
		pe.GTID_entity = append(pe.GTID_entity, peb)
	}
	return nil
}

func (pe *Pre_gtid_event) ToString() string {
	output := ""
	for _, v := range pe.GTID_entity {
		if output == "" {
			output = v.UUID
		} else {
			output = output + v.UUID + ";"
		}
		for _, v1 := range v.Interv_entity {
			output = output + fmt.Sprintf(":%d-%d", v1.Interv_start, v1.Interv_end)
		}
	}
	logx.DebugF("Pre_gtid_event: %s", output)
	return output
}
