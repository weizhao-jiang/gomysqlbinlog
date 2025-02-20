## 介绍
**************
用纯go语言解析MySQL binlog文件(没有使用任何第三方库)，学习binlog文件内部结构，binlog events组成以及数据恢复。支持MySQL 5.5/5.6/5.7/8.0


## 使用
```bash
Usage of /tmp/go-build1226212202/b001/exe/main:
  -base64
        Print sql in base64 format.
  -binary
        Print sql in binary way.
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
root@test# ./gomysqlbinlog --sql  /mysql-bin
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
