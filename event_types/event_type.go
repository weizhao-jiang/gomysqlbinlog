package event_types

const (
	UNKNOWN_EVENT            = 0
	START_EVENT_V3           = 1
	QUERY_EVENT              = 2
	STOP_EVENT               = 3
	ROTATE_EVENT             = 4
	INTVAR_EVENT             = 5
	LOAD_EVENT               = 6
	SLAVE_EVENT              = 7
	CREATE_FILE_EVENT        = 8
	APPEND_BLOCK_EVENT       = 9
	EXEC_LOAD_EVENT          = 10
	DELETE_FILE_EVENT        = 11
	NEW_LOAD_EVENT           = 12
	RAND_EVENT               = 13
	USER_VAR_EVENT           = 14
	FORMAT_DESCRIPTION_EVENT = 15
	XID_EVENT                = 16
	BEGIN_LOAD_QUERY_EVENT   = 17
	EXECUTE_LOAD_QUERY_EVENT = 18
	TABLE_MAP_EVENT          = 19
	PRE_GA_WRITE_ROWS_EVENT  = 20
	PRE_GA_UPDATE_ROWS_EVENT = 21
	PRE_GA_DELETE_ROWS_EVENT = 22

	WRITE_ROWS_EVENT_V1  = 23
	UPDATE_ROWS_EVENT_V1 = 24
	DELETE_ROWS_EVENT_V1 = 25

	INCIDENT_EVENT = 26

	HEARTBEAT_LOG_EVENT = 27

	IGNORABLE_LOG_EVENT  = 28
	ROWS_QUERY_LOG_EVENT = 29

	WRITE_ROWS_EVENT  = 30
	UPDATE_ROWS_EVENT = 31
	DELETE_ROWS_EVENT = 32

	GTID_LOG_EVENT           = 33
	ANONYMOUS_GTID_LOG_EVENT = 34

	PREVIOUS_GTIDS_LOG_EVENT = 35 //描述之前的gtid信息(不需要扫描之前的binlog文件)

	TRANSACTION_CONTEXT_EVENT = 36

	VIEW_CHANGE_EVENT = 37

	XA_PREPARE_LOG_EVENT = 38

	// 8.0 新增如下event (所以8.0的读的时候要多读3个)
	PARTIAL_UPDATE_ROWS_EVENT = 39
	TRANSACTION_PAYLOAD_EVENT = 40
	HEARTBEAT_LOG_EVENT_V2    = 41
)

// BINLOG HEADER
const (
	BINLOG_FLAG      = "\xfebin"
	BINLOG_FLAG_SIZE = 4
	EVENT_HEAD_SIZE  = 19
)

// GTID
const (
	LOGICAL_TIMESTAMP_TYPECODE_LENGTH = 1
	IMMEDIATE_COMMIT_TIMESTAMP_LENGTH = 7
	LOGICAL_TIMESTAMP_TYPECODE        = 2
	ENCODED_COMMIT_TIMESTAMP_LENGTH   = 55
	ENCODED_SERVER_VERSION_LENGTH     = 31
)

// @libbinlogevents/include/rows_event.h
// https://dev.mysql.com/doc/dev/mysql-server/latest/classmysql_1_1binlog_1_1event_1_1Table__map__event.html
// 描述和格式都有. (网站那个不全, 源码里面更全. 有些类型没有及时更新到注释文档里面.)
// 符号 只对number类型有效, 还是从左到右, 1bit表示1个number, 1表示无符号, 0表示有符号
const (
	IGNEDNESS = 1

	// 字符集
	DEFAULT_CHARSET = 2

	// 库字符集
	COLUMN_CHARSET = 3

	// 字段名字.(only for binlog_row_metadata=FULL) 字段名长度限制为64字节
	COLUMN_NAME = 4

	// set的值 binlog_row_metadata=FULL
	SET_STR_VALUE = 5

	// enum的值 binlog_row_metadata=FULL
	ENUM_STR_VALUE = 6

	// 空间坐标的 binlog_row_metadata=FULL
	GEOMETRY_TYPE = 7

	// 主键 binlog_row_metadata=FULL
	SIMPLE_PRIMARY_KEY = 8
	// 主键前缀索引  binlog_row_metadata=FULL
	PRIMARY_KEY_WITH_PREFIX      = 9
	ENUM_AND_SET_DEFAULT_CHARSET = 10
	ENUM_AND_SET_COLUMN_CHARSET  = 11

	// 可见字段
	COLUMN_VISIBILITY = 12
)

// @include/field_types.h
const (
	MYSQL_TYPE_DECIMAL     = 0
	MYSQL_TYPE_TINY        = 1
	MYSQL_TYPE_SHORT       = 2
	MYSQL_TYPE_LONG        = 3
	MYSQL_TYPE_FLOAT       = 4
	MYSQL_TYPE_DOUBLE      = 5
	MYSQL_TYPE_NULL        = 6
	MYSQL_TYPE_TIMESTAMP   = 7
	MYSQL_TYPE_LONGLONG    = 8
	MYSQL_TYPE_INT24       = 9
	MYSQL_TYPE_DATE        = 10
	MYSQL_TYPE_TIME        = 11
	MYSQL_TYPE_DATETIME    = 12
	MYSQL_TYPE_YEAR        = 13
	MYSQL_TYPE_NEWDATE     = 14 /**< Internal to MySQL. Not used in protocol */
	MYSQL_TYPE_VARCHAR     = 15
	MYSQL_TYPE_BIT         = 16
	MYSQL_TYPE_TIMESTAMP2  = 17
	MYSQL_TYPE_DATETIME2   = 18 /**< Internal to MySQL. Not used in protocol */
	MYSQL_TYPE_TIME2       = 19 /**< Internal to MySQL. Not used in protocol */
	MYSQL_TYPE_TYPED_ARRAY = 20 /**< Used for replication only */
	MYSQL_TYPE_INVALID     = 243
	MYSQL_TYPE_BOOL        = 244 /**< Currently just a placeholder */
	MYSQL_TYPE_JSON        = 245
	MYSQL_TYPE_NEWDECIMAL  = 246
	MYSQL_TYPE_ENUM        = 247
	MYSQL_TYPE_SET         = 248
	MYSQL_TYPE_TINY_BLOB   = 249
	MYSQL_TYPE_MEDIUM_BLOB = 250
	MYSQL_TYPE_LONG_BLOB   = 251
	MYSQL_TYPE_BLOB        = 252
	MYSQL_TYPE_VAR_STRING  = 253
	MYSQL_TYPE_STRING      = 254
	MYSQL_TYPE_GEOMETRY    = 255
)

// https://github.dev/mysql/mysql-server/tree/trunk/libbinlogevents/include
// statement_events.cpp
const (
	MAX_DBS_IN_EVENT_MTS      = 16
	OVER_MAX_DBS_IN_EVENT_MTS = 255
)
