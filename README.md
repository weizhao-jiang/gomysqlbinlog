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

```
