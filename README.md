## 介绍
**************
用纯go语言解析MySQL binlog文件(没有使用任何第三方库)，学习binlog文件内部结构，binlog events组成以及数据恢复。支持MySQL 5.5/5.6/5.7/8.0


## 使用
```bash
Usage of ./gomysqlbinlog:
  -base64
        Print sql in base64 format.
  -binary
        Print sql in binary way.(Not support now.)
  -include-gtids string
        Print events whose Global Transaction Identifiers were provided.
  -log-level int
        output log level, 5:debug 4:info 3:warn 2:error 1:panic (default 1)
  -rollback
        Print sql in rollback way.
  -skip-gtids
        Do not preserve Global Transaction Identifiers; insteadmake the server execute the transactions as if they were new.
  -sql
        Print raw sql.
  -start-datetime string
        Start reading the binlog at first event having a datetime equal or posterior to the argument. (default "1970-01-01 00:00:00")
  -start-pos string
        Start reading the binlog at position N. Applies to the first binlog passed on the command line. (default "-1")
  -stop-datetime string
        Stop reading the binlog at first event having a datetime equal or posterior to the argument. (default "1970-01-01 00:00:00")
  -stop-pos string
        Stop reading the binlog at position N. Applies to the last binlog passed on the command line. (default "-1")
  -version
        gobinlog2sql version
```


#### 使用例子
```bash
# 解析成sql输出
gomysqlbinlog --sql mysql-bin.000810
# 解析为base64 输出
gomysqlbinlog --base64 mysql-bin.000810
# 指定开始、结束位置(会破坏事务)
gomysqlbinlog --start-pos 99 --stop-pos 9999 --sql mysql-bin.000810
# 指定开始、结束时间(会破坏事务)
gomysqlbinlog --start-datetime "2025-02-20 08:00:00"  --stop-datetime "2025-02-20 12:00:00" --sql mysql-bin.000810
# 重新生成GTID，而非使用原GTID
gomysqlbinlog --skip-gtids --start-datetime "2025-02-20 08:00:00"  --stop-datetime "2025-02-20 12:00:00" --sql mysql-bin.000810
```


#### 输出实例
```bash
root@test# ./gomysqlbinlog --sql  --log-level 5 /mysql-bin
DELIMITER /*!*/;
# at 4
#2024-11-14 17:22:50 server id 5223  end_log_pos 126 CRC32 dd96da5e      FORMAT_DESCRIPTION_EVENT Start: binlog v4, server v8.0.40-0ubuntu0.22.04.1 created 2024-11-14 17:22:50

# at 126
#2024-11-14 17:22:50 server id 5223  end_log_pos 197 CRC32 1087a1b6      PREVIOUS_GTIDS_LOG_EVENT 
# ac0890ba-af9f-11ee-a694-525400f0b85f:1-72002137
# at 197
#2024-11-14 17:23:45 server id 5223  end_log_pos 276 CRC32 b2a6cde7      GTID_LOG_EVENT 
/*!80001 SET @@session.original_commit_timestamp=0*/ /*!*/;
/*!80014 SET @@session.original_server_version=80040*/ /*!*/;
/*!80014 SET @@session.immediate_server_version=80040*/ /*!*/;
SET @@SESSION.GTID_NEXT='ac0890ba-af9f-11ee-a694-525400f0b85f:72002138' /*!*/;
# at 276
#2024-11-14 17:23:45 server id 5223  end_log_pos 351 CRC32 f91d9f00      QUERY_EVENT 
SET TIMESTAMP=1731576225 /* QUERY TIME 2024-11-14 17:23:45 */ /*!*/;
SET @@session.sql_mode=1168113696 /*!*/;
SET @@session.auto_increment_increment=0 /*!*/;
SET @@session.character_set_client=255, @@session.collation_connection=255, @@session.collation_server=255 /*!*/;
/*!80011 SET @@session.default_collation_for_utf8mb4=255 *//*!*/;
USE test /*!*/;
BEGIN /*!*/;
# at 351
#2024-11-14 17:23:45 server id 5223  end_log_pos 401 CRC32 7bfa585a      TABLE_MAP_EVENT Table_map:`test`.`x222` mapped to 110
# at 401
#2024-11-14 17:23:45 server id 5223  end_log_pos 461 CRC32 e55d4948      WRITE_ROWS_EVENT table id 110
-- INSERT INTO `test`.`x222` VALUES (1) /* WRITE_ROWS_EVENT */ /*!*/;
-- INSERT INTO `test`.`x222` VALUES (2) /* WRITE_ROWS_EVENT */ /*!*/;
-- INSERT INTO `test`.`x222` VALUES (3) /* WRITE_ROWS_EVENT */ /*!*/;
-- INSERT INTO `test`.`x222` VALUES (4) /* WRITE_ROWS_EVENT */ /*!*/;
-- INSERT INTO `test`.`x222` VALUES (5) /* WRITE_ROWS_EVENT */ /*!*/;
# at 461
#2024-11-14 17:23:45 server id 5223  end_log_pos 492 CRC32 c277cb40      XID_EVENT 
COMMIT /* XID 65 */ /*!*/;
DELIMITER ;
# End of log file
```


#### DEBUG 模式，可以看到每个event的分析流程
```bash
root@test# ./gomysqlbinlog --sql  /mysql-bin
2025-02-20 14:46:13.230 [DEBU] main.go:30: binlogType: normal binlog
2025-02-20 14:46:13.230 [DEBU] format_desc_event.go:31: Binlog_version: 4
2025-02-20 14:46:13.230 [DEBU] format_desc_event.go:35: Mysql_version: 8.0.40-0ubuntu0.22.04.1
2025-02-20 14:46:13.230 [DEBU] format_desc_event.go:38: Create_timestamp: 0
2025-02-20 14:46:13.230 [DEBU] format_desc_event.go:41: Event_header_length: 19 (Should always equal 19)
2025-02-20 14:46:13.230 [DEBU] format_desc_event.go:69: Event_post_header:[0 d 0 8 0 0 0 0 4 0 4 0 0 0 62 0 4 1a 8 0 0 0 8 8 8 2 0 0 0 a a a 2a 2a 0 12 34 0 a 28 0]
2025-02-20 14:46:13.230 [DEBU] format_desc_event.go:80: checksum algorithm: 1 (0:off 1:crc32)
2025-02-20 14:46:13.230 [DEBU] format_desc_event.go:83: checksum value: 3717651038
DELIMITER /*!*/;
# at 4
#2024-11-14 17:22:50 server id 5223  end_log_pos 126 CRC32 dd96da5e      FORMAT_DESCRIPTION_EVENT Start: binlog v4, server v8.0.40-0ubuntu0.22.04.1 created 2024-11-14 17:22:50

2025-02-20 14:46:13.230 [DEBU] read_event.go:81: READ EVENT FINISH. EVENT TYPE:15(false) 4 --> 126  size:122 bytes. HasChecksum:true
2025-02-20 14:46:13.230 [DEBU] pre_gtid_event.go:120: Pre_gtid_event: ac0890ba-af9f-11ee-a694-525400f0b85f:1-72002137
# at 126
#2024-11-14 17:22:50 server id 5223  end_log_pos 197 CRC32 1087a1b6      PREVIOUS_GTIDS_LOG_EVENT 
2025-02-20 14:46:13.230 [DEBU] pre_gtid_event.go:120: Pre_gtid_event: ac0890ba-af9f-11ee-a694-525400f0b85f:1-72002137
# ac0890ba-af9f-11ee-a694-525400f0b85f:1-72002137
2025-02-20 14:46:13.230 [DEBU] read_event.go:81: READ EVENT FINISH. EVENT TYPE:35(false) 126 --> 197  size:71 bytes. HasChecksum:true
2025-02-20 14:46:13.230 [DEBU] read_event.go:81: READ EVENT FINISH. EVENT TYPE:33(true) 197 --> 276  size:79 bytes. HasChecksum:true
2025-02-20 14:46:13.230 [DEBU] read_event.go:92: CRC32:2997276135 CHECKSUM:2997276135
2025-02-20 14:46:13.230 [DEBU] gtid_event.go:114: GTID_EVENT INFORMATION:
EVENT_TYPE                 :GTID_LOG_EVENT
GTID_FLAGS                 :0
SID(server_uuid)           :ac0890ba-af9f-11ee-a694-525400f0b85f
GNO(GTID)                  :72002138
Lt_type                    :2
Last_committed             :0
Sequence_number            :1          
Immediate_commit_timestamp :1731576225198215
Original_commit_timestamp  :0
Transaction_length         :295
Immediate_server_version   :80040
Original_server_version    :80040
2025-02-20 14:46:13.231 [DEBU] read_trx_event.go:250: [GTID_LOG_EVENT] event add. size: 79
2025-02-20 14:46:13.231 [DEBU] read_trx_event.go:253: TRX EVENT ORDER: [GTID_EVENT]
2025-02-20 14:46:13.231 [DEBU] read_event.go:81: READ EVENT FINISH. EVENT TYPE:2(true) 276 --> 351  size:75 bytes. HasChecksum:true
2025-02-20 14:46:13.231 [DEBU] read_event.go:92: CRC32:4179468032 CHECKSUM:4179468032
2025-02-20 14:46:13.231 [DEBU] read_trx_event.go:250: [QUERY_EVENT] event add. size: 75
2025-02-20 14:46:13.231 [DEBU] read_trx_event.go:253: TRX EVENT ORDER: [GTID_EVENT QUERY_EVENT]
2025-02-20 14:46:13.231 [DEBU] read_event.go:81: READ EVENT FINISH. EVENT TYPE:19(true) 351 --> 401  size:50 bytes. HasChecksum:true
2025-02-20 14:46:13.231 [DEBU] read_event.go:92: CRC32:2080004186 CHECKSUM:2080004186
2025-02-20 14:46:13.231 [DEBU] read_trx_event.go:250: [TABLE_MAP_EVENT] event add. size: 50
2025-02-20 14:46:13.231 [DEBU] read_trx_event.go:253: TRX EVENT ORDER: [GTID_EVENT QUERY_EVENT TABLE_MAP_EVENT]
2025-02-20 14:46:13.231 [DEBU] read_event.go:81: READ EVENT FINISH. EVENT TYPE:30(true) 401 --> 461  size:60 bytes. HasChecksum:true
2025-02-20 14:46:13.231 [DEBU] read_event.go:92: CRC32:3848096072 CHECKSUM:3848096072
2025-02-20 14:46:13.231 [DEBU] read_trx_event.go:250: [WRITE_ROWS_EVENT] event add. size: 60
2025-02-20 14:46:13.231 [DEBU] read_trx_event.go:253: TRX EVENT ORDER: [GTID_EVENT QUERY_EVENT TABLE_MAP_EVENT WRITE_ROWS_EVENT]
2025-02-20 14:46:13.231 [DEBU] read_event.go:81: READ EVENT FINISH. EVENT TYPE:16(true) 461 --> 492  size:31 bytes. HasChecksum:true
2025-02-20 14:46:13.231 [DEBU] read_event.go:92: CRC32:3262630720 CHECKSUM:3262630720
2025-02-20 14:46:13.231 [DEBU] read_trx_event.go:250: [XID_EVENT] event add. size: 31
2025-02-20 14:46:13.231 [DEBU] read_trx_event.go:253: TRX EVENT ORDER: [GTID_EVENT QUERY_EVENT TABLE_MAP_EVENT WRITE_ROWS_EVENT XID_EVENT]
2025-02-20 14:46:13.231 [DEBU] read_trx_event.go:258: READ TRX EVENT FINISH. offset:197 --> 492 ,len:5 bytes
2025-02-20 14:46:13.231 [DEBU] gtid_event.go:114: GTID_EVENT INFORMATION:
EVENT_TYPE                 :GTID_LOG_EVENT
GTID_FLAGS                 :0
SID(server_uuid)           :ac0890ba-af9f-11ee-a694-525400f0b85f
GNO(GTID)                  :72002138
Lt_type                    :2
Last_committed             :0
Sequence_number            :1          
Immediate_commit_timestamp :1731576225198215
Original_commit_timestamp  :0
Transaction_length         :295
Immediate_server_version   :80040
Original_server_version    :80040
# at 197
#2024-11-14 17:23:45 server id 5223  end_log_pos 276 CRC32 b2a6cde7      GTID_LOG_EVENT 
/*!80001 SET @@session.original_commit_timestamp=0*/ /*!*/;
/*!80014 SET @@session.original_server_version=80040*/ /*!*/;
/*!80014 SET @@session.immediate_server_version=80040*/ /*!*/;
SET @@SESSION.GTID_NEXT='ac0890ba-af9f-11ee-a694-525400f0b85f:72002138' /*!*/;
2025-02-20 14:46:13.231 [DEBU] query_event.go:78: 
Thread_id        :9
Query_exec_time  :0
Db_len           :4
Dbname           :test
Error_code       :0
Status_vars_len  :29
Status_vars      :[0 0 0 0 0 1 32 0 160 69 0 0 0 0 6 3 115 116 100 4 255 0 255 0 255 0 18 255 0]
M_query          :BEGIN
# at 276
#2024-11-14 17:23:45 server id 5223  end_log_pos 351 CRC32 f91d9f00      QUERY_EVENT 
SET TIMESTAMP=1731576225 /* QUERY TIME 2024-11-14 17:23:45 */ /*!*/;
SET @@session.sql_mode=1168113696 /*!*/;
SET @@session.auto_increment_increment=0 /*!*/;
SET @@session.character_set_client=255, @@session.collation_connection=255, @@session.collation_server=255 /*!*/;
/*!80011 SET @@session.default_collation_for_utf8mb4=255 *//*!*/;
USE test /*!*/;
BEGIN /*!*/;
2025-02-20 14:46:13.231 [DEBU] tablemap_event.go:119: 
offset         : 46
size           : 46
TABLE_ID       : 110
FLAGS          : 1
DBNAME         : test
TABLENAME      : x222
COLUMN COUNT   : 1
column type    : [3]
metadata length: 0
metadata       : []
null_bits      : 1(1)
null_bits_list : [1 0 0 0 0 0 0 0]
null_bit_bool  : [true]
opt            : [{Meta_type:1 Meta_len:1 Meta_value:[0]}]
2025-02-20 14:46:13.231 [DEBU] tablemap_event.go:160: signed_list(True:unsigned) [0 0 0 0 0 0 0 0]
# at 351
#2024-11-14 17:23:45 server id 5223  end_log_pos 401 CRC32 7bfa585a      TABLE_MAP_EVENT Table_map:`test`.`x222` mapped to 110
2025-02-20 14:46:13.231 [DEBU] rows_event.go:555: Table id:110
2025-02-20 14:46:13.231 [DEBU] rows_event.go:557: Flags:1
2025-02-20 14:46:13.231 [DEBU] rows_event.go:561: Extra_row_length:2
2025-02-20 14:46:13.231 [DEBU] rows_event.go:586: Width:1
2025-02-20 14:46:13.231 [DEBU] rows_event.go:590: Cols:1111111111111111111111111111111111111111111111111111111111111111
2025-02-20 14:46:13.231 [DEBU] rows_event.go:603: Columns_before_image:255
2025-02-20 14:46:13.231 [DEBU] rows_event.go:609: Columns_after_image:255
2025-02-20 14:46:13.231 [DEBU] rows_event.go:612: DATA SIZE:25
2025-02-20 14:46:13.231 [DEBU] rows_event.go:107: start_offset:12
2025-02-20 14:46:13.231 [DEBU] rows_event.go:115: [Read_row]:nullbits:0 rw.Cols:1111111111111111111111111111111111111111111111111111111111111111
2025-02-20 14:46:13.231 [DEBU] rows_event.go:743: ### @0=1 /* LONG meta=0 nullable=1 is_null=0 */
2025-02-20 14:46:13.231 [DEBU] rows_event.go:536: stop_offset:17
2025-02-20 14:46:13.231 [DEBU] rows_event.go:107: start_offset:17
2025-02-20 14:46:13.231 [DEBU] rows_event.go:115: [Read_row]:nullbits:0 rw.Cols:1111111111111111111111111111111111111111111111111111111111111111
2025-02-20 14:46:13.231 [DEBU] rows_event.go:743: ### @0=2 /* LONG meta=0 nullable=1 is_null=0 */
2025-02-20 14:46:13.231 [DEBU] rows_event.go:536: stop_offset:22
2025-02-20 14:46:13.231 [DEBU] rows_event.go:107: start_offset:22
2025-02-20 14:46:13.231 [DEBU] rows_event.go:115: [Read_row]:nullbits:0 rw.Cols:1111111111111111111111111111111111111111111111111111111111111111
2025-02-20 14:46:13.231 [DEBU] rows_event.go:743: ### @0=3 /* LONG meta=0 nullable=1 is_null=0 */
2025-02-20 14:46:13.231 [DEBU] rows_event.go:536: stop_offset:27
2025-02-20 14:46:13.231 [DEBU] rows_event.go:107: start_offset:27
2025-02-20 14:46:13.231 [DEBU] rows_event.go:115: [Read_row]:nullbits:0 rw.Cols:1111111111111111111111111111111111111111111111111111111111111111
2025-02-20 14:46:13.231 [DEBU] rows_event.go:743: ### @0=4 /* LONG meta=0 nullable=1 is_null=0 */
2025-02-20 14:46:13.231 [DEBU] rows_event.go:536: stop_offset:32
2025-02-20 14:46:13.231 [DEBU] rows_event.go:107: start_offset:32
2025-02-20 14:46:13.231 [DEBU] rows_event.go:115: [Read_row]:nullbits:0 rw.Cols:1111111111111111111111111111111111111111111111111111111111111111
2025-02-20 14:46:13.231 [DEBU] rows_event.go:743: ### @0=5 /* LONG meta=0 nullable=1 is_null=0 */
2025-02-20 14:46:13.231 [DEBU] rows_event.go:536: stop_offset:37
2025-02-20 14:46:13.231 [DEBU] rows_event.go:107: start_offset:37
# at 401
#2024-11-14 17:23:45 server id 5223  end_log_pos 461 CRC32 e55d4948      WRITE_ROWS_EVENT table id 110
-- INSERT INTO `test`.`x222` VALUES (1) /* WRITE_ROWS_EVENT */ /*!*/;
-- INSERT INTO `test`.`x222` VALUES (2) /* WRITE_ROWS_EVENT */ /*!*/;
-- INSERT INTO `test`.`x222` VALUES (3) /* WRITE_ROWS_EVENT */ /*!*/;
-- INSERT INTO `test`.`x222` VALUES (4) /* WRITE_ROWS_EVENT */ /*!*/;
-- INSERT INTO `test`.`x222` VALUES (5) /* WRITE_ROWS_EVENT */ /*!*/;
# at 461
#2024-11-14 17:23:45 server id 5223  end_log_pos 492 CRC32 c277cb40      XID_EVENT 
COMMIT /* XID 65 */ /*!*/;
2025-02-20 14:46:13.231 [INFO] file_reader.go:35: [/opt/tmp/mysql-bin] read finish.
2025-02-20 14:46:13.231 [INFO] read_event.go:31: binlog end.
2025-02-20 14:46:13.231 [DEBU] read_trx_event.go:256: Empty transaction events.
2025-02-20 14:46:13.231 [INFO] main.go:41: trx event is empty.
DELIMITER ;
# End of log file
```