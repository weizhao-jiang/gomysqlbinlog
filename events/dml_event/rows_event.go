package dml_event

import (
	"fmt"
	"gomysqlbinlog/event_types"
	"gomysqlbinlog/events/json_binary"
	"gomysqlbinlog/options_handler"
	"gomysqlbinlog/utils"
	"gomysqlbinlog/utils/event_ops"
	"gomysqlbinlog/utils/logx"
	"hash/crc32"
	"time"
)

/*
@mysql-8.0.40/libbinlogevents/src/rows_event.cpp:384:Rows_event
@mysql-8.0.40/libbinlogevents/include/rows_event.h
https://dev.mysql.com/doc/dev/mysql-server/latest/classmysql_1_1binlog_1_1event_1_1Rows__event.html
*/

var row_flags []string = []string{
	1: "STMT_END_F",
	2: "NO_FOREIGN_KEY_CHECKS_F",
	4: "RELAXED_UNIQUE_CHECKS_F",
	8: "COMPLETE_ROWS_F",
}

// var enum_extra_row_info_typecode []string = []string{
// 	0: "NDB",
// 	1: "PART",
// }

type RowEvent struct {
	event_ops.EventDetailReader
	tp        *TablemapEvent
	rowsdata  []RowsData
	EventType uint
	RowEventHeader
	RowEventBody
}

type RowEventHeader struct {
	Table_id uint // 6 bytes unsigned integer
	Flags    uint // 2 byte bitfield  Reserved for future use; currently always 0.
}

type RowEventBody struct {
	Width                uint   // packed integer
	Cols                 uint64 // Indicates whether each column is used, one bit per column. For this field, the amount of storage required is INT((width + 7) / 8) bytes.
	Extra_row_length     uint
	Extra_row_type       uint
	Extra_row_info_NDB   ERI_NDB // In case of NDB
	Extra_row_info_ID    ERI_ID  // In case of INSERT/DELETE
	Extra_row_info_U     ERI_U   // In case of UPDATE
	Extra_data           []byte
	Columns_before_image uint   // the amount of storage required for N columns is INT((N + 7) / 8) bytes.
	Columns_after_image  uint   // the amount of storage required for N columns is INT((N + 7) / 8) bytes.
	Row                  []uint // Null_bit_mask(4)|field-1|field-2|field-3|field 4
}

type ERI_NDB struct {
	Ndb_info_length uint
	Ndb_info_format uint
	Ndb_info        []byte
}

type ERI_ID struct {
	Partition_id uint
}

type ERI_U struct {
	Partition_id        uint
	Source_partition_id uint
}

type RowsData struct {
	Offset             []uint
	Data               []*string
	Metadata           []uint
	NextUpdateRowsData *RowsData
}

/*
关于数值类型，源码里面直接全部当有符号去显示了，不过如果是负数，则会使用(%u)加个括号显示无符号的值？？
@mysql-8.0.40/sql/log_event.cc:1582:my_b_write_sint32_and_uint32
  Prints a 32-bit number in both signed and unsigned representation

  @param[in] file              IO cache
  @param[in] si                Signed number
  @param[in] ui                Unsigned number

static void my_b_write_sint32_and_uint32(IO_CACHE *file, int32 si, uint32 ui) {
	my_b_printf(file, "%d", si);
	if (si < 0) my_b_printf(file, " (%u)", ui);
  }
*/

// 根据已经获取到 TablemapEvent 每一列的类型读取整一行数据
// https://dev.mysql.com/doc/dev/mysql-server/latest/classmysql_1_1binlog_1_1event_1_1Table__map__event.html
// Table_map_event column types: numerical identifier and metadata
// @mysql-8.0.40/libbinlogevents/include/rows_event.h
func (rw *RowEvent) Read_row() (start_offset, stop_offset int, rwdata []*string, metadata []uint) {
	// var coln int = 0
	var metaOffset int = 0
	var mdata uint64
	start_offset = int(rw.Offset)
	logx.DebugF("start_offset:%d", start_offset-19)
	// nbdata := rw.Read(uint((rw.Width + 7) / 8))
	nbdata := rw.Read(uint((rw.Width + 7) / 8))

	if nbdata == nil {
		return -999, -999, nil, nil
	}
	nullbits := rw.Read_uint_try(nbdata, "little")
	logx.DebugF("[Read_row]:nullbits:%b rw.Cols:%b", nullbits, rw.Cols)
	rwdata = make([]*string, len(rw.tp.Column_type))
	for k := range rwdata {
		rwdata[k] = new(string)
	}
	for coln, colType := range rw.tp.Column_type {
		mdata = 0
		// 当前行该列没被使用
		if (rw.Cols & (1 << coln)) == 0 {
			rwdata[coln] = nil
			// logx.DebugF("%d is not used.", coln)
		}

		// 当前行该列值为空
		if rw.tp.Null_bits_boolean[coln] && (nullbits&(1<<(coln))) != 0 {
			rwdata[coln] = nil
			logx.DebugF("%d is NULL. %b   %b", coln, nullbits, 1<<(coln))
		}

		// @mysql-8.0.40/sql/log_event.cc:1814:log_event_print_value
		func() {
			switch colType {
			case event_types.MYSQL_TYPE_NEWDECIMAL:
				numCount := rw.Read_uint_try(rw.tp.Metadata[metaOffset:metaOffset+1], "little")
				metaOffset++
				decimalNumCount := rw.Read_uint_try(rw.tp.Metadata[metaOffset:metaOffset+1], "little")
				metaOffset++
				if rwdata[coln] == nil {
					rw.PrintLog(coln, rwdata[coln], "DECIMAL", mdata, rw.tp.Null_bits_list[coln], 1)
					return
				}

				integer_p1_count := int((numCount - decimalNumCount) / 9)
				integer_p2_count := int(numCount-decimalNumCount) - integer_p1_count*9
				integer_size := integer_p1_count*4 + int((integer_p2_count+1)/2)
				decimal_p1_count := int(decimalNumCount / 9)
				decimal_p2_count := int(decimalNumCount) - decimal_p1_count*9
				decimal_size := decimal_p1_count*4 + int((decimal_p2_count+1)/2)
				total_size := integer_size + decimal_size
				DMByteData := rw.Read(uint(total_size))
				p1 := integer_size
				//p2 := decimal_size
				p1_bdata := DMByteData[:p1]
				p2_bdata := DMByteData[p1:]
				p1_data := rw.Read_int_try(p1_bdata, "big")
				p2_data := rw.Read_int_try(p2_bdata, "big")
				//p1_n := (p1 * 8) - 1
				//p2_n := (p2 * 8) - 1
				if p1_data < 0 {
					p1_data = p1_data + int64(1<<(8*p1-1))
				} else {
					p1_data = p1_data - int64(1<<(8*p1-1))
				}
				if p2_data < 0 {
					p2_data = -(p2_data + 1)
				}
				*rwdata[coln] = fmt.Sprintf("%d.%d", p1_data, p2_data)
				rw.PrintLog(coln, rwdata[coln], fmt.Sprintf("DECIMAL(%d.%d)", p1_data, p2_data), mdata, rw.tp.Null_bits_list[coln], 0)

			case event_types.MYSQL_TYPE_TINY:
				if rwdata[coln] == nil {
					rw.PrintLog(coln, rwdata[coln], "TINY", mdata, rw.tp.Null_bits_list[coln], 1)
					return
				}

				data := rw.Read_int(1)
				*rwdata[coln] = fmt.Sprintf("%d", data)
				rw.PrintLog(coln, rwdata[coln], "TINY", mdata, rw.tp.Null_bits_list[coln], 0)

			case event_types.MYSQL_TYPE_SHORT:
				if rwdata[coln] == nil {
					rw.PrintLog(coln, rwdata[coln], "SHORT", mdata, rw.tp.Null_bits_list[coln], 1)
					return
				}

				data := rw.Read_int(2)
				*rwdata[coln] = fmt.Sprintf("%d", data)
				rw.PrintLog(coln, rwdata[coln], "SHORT", mdata, rw.tp.Null_bits_list[coln], 0)

			case event_types.MYSQL_TYPE_LONG:
				if rwdata[coln] == nil {
					rw.PrintLog(coln, rwdata[coln], "LONG", mdata, rw.tp.Null_bits_list[coln], 1)
					return
				}

				data := rw.Read_int(4)
				*rwdata[coln] = fmt.Sprintf("%d", data)
				rw.PrintLog(coln, rwdata[coln], "LONG", mdata, rw.tp.Null_bits_list[coln], 0)

			case event_types.MYSQL_TYPE_FLOAT:
				mdata = rw.Read_uint_try(rw.tp.Metadata[metaOffset:metaOffset+1], "little")
				metaOffset++
				if rwdata[coln] == nil {
					rw.PrintLog(coln, rwdata[coln], "FLOAT", mdata, rw.tp.Null_bits_list[coln], 1)
					return
				}

				data := rw.Read_float(4)
				*rwdata[coln] = fmt.Sprintf("%v", data)
				rw.PrintLog(coln, rwdata[coln], "FLOAT", mdata, rw.tp.Null_bits_list[coln], 0)

			case event_types.MYSQL_TYPE_DOUBLE:
				mdata = rw.Read_uint_try(rw.tp.Metadata[metaOffset:metaOffset+1], "little")
				metaOffset++
				if rwdata[coln] == nil {
					rw.PrintLog(coln, rwdata[coln], "DOUBLE", mdata, rw.tp.Null_bits_list[coln], 1)
					return
				}

				data := rw.Read_float(8)
				*rwdata[coln] = fmt.Sprintf("%f", data)
				rw.PrintLog(coln, rwdata[coln], "DOUBLE", mdata, rw.tp.Null_bits_list[coln], 0)

			case event_types.MYSQL_TYPE_TIMESTAMP:
				if rwdata[coln] == nil {
					rw.PrintLog(coln, rwdata[coln], "TIMESTAMP", mdata, rw.tp.Null_bits_list[coln], 1)
					return
				}

				timestampx := rw.Read_uint(4)
				data := time.Unix(int64(timestampx), 0).Format("2006-01-02 15:04:05")
				*rwdata[coln] = data
				rw.PrintLog(coln, rwdata[coln], "TIMESTAMP", mdata, rw.tp.Null_bits_list[coln], 0)

			case event_types.MYSQL_TYPE_LONGLONG:
				if rwdata[coln] == nil {
					rw.PrintLog(coln, rwdata[coln], "LONGLONG", mdata, rw.tp.Null_bits_list[coln], 1)
					return
				}

				data := rw.Read_int(8)
				*rwdata[coln] = fmt.Sprintf("%d", data)
				rw.PrintLog(coln, rwdata[coln], "LONGLONG", mdata, rw.tp.Null_bits_list[coln], 0)

			case event_types.MYSQL_TYPE_INT24:
				if rwdata[coln] == nil {
					rw.PrintLog(coln, rwdata[coln], "INT24", mdata, rw.tp.Null_bits_list[coln], 1)
					return
				}

				data := rw.Read_int(3)
				*rwdata[coln] = fmt.Sprintf("%d", data)
				rw.PrintLog(coln, rwdata[coln], "INT24", mdata, rw.tp.Null_bits_list[coln], 0)

			case event_types.MYSQL_TYPE_DATE:
				if rwdata[coln] == nil {
					rw.PrintLog(coln, rwdata[coln], "DATE", mdata, rw.tp.Null_bits_list[coln], 1)
					return
				}

				data := rw.Read_uint(3)
				year := (data & ((1 << 15) - 1) << 9) >> 9
				month := (data & ((1 << 4) - 1) << 5) >> 5
				day := (data & ((1 << 5) - 1))
				*rwdata[coln] = fmt.Sprintf("%v-%v-%v", year, month, day)
				rw.PrintLog(coln, rwdata[coln], "DATE", mdata, rw.tp.Null_bits_list[coln], 0)

			case event_types.MYSQL_TYPE_TIME:
				if rwdata[coln] == nil {
					rw.PrintLog(coln, rwdata[coln], "TIME", mdata, rw.tp.Null_bits_list[coln], 1)
					return
				}

				data := rw.Read_uint(3)
				timex := fmt.Sprintf("%v:%v:%v", int(data/10000), int((data%10000)/100), int(data%100))
				*rwdata[coln] = timex
				rw.PrintLog(coln, rwdata[coln], "TIME", mdata, rw.tp.Null_bits_list[coln], 0)

			case event_types.MYSQL_TYPE_TIME2:
				mdata = rw.Read_uint_try(rw.tp.Metadata[metaOffset:metaOffset+1], "little")
				metaOffset++
				if rwdata[coln] == nil {
					rw.PrintLog(coln, rwdata[coln], "TIME2", mdata, rw.tp.Null_bits_list[coln], 1)
					return
				}

				bdata := rw.Read(uint(3 + int((mdata+1)/2)))
				idata := rw.Read_int_try(bdata[:3], "big")
				hour := ((idata & ((1 << 10) - 1) << 12) >> 12)
				minute := (idata & ((1 << 6) - 1) << 6) >> 6
				second := (idata & ((1 << 6) - 1))
				great0 := false
				if idata&(1<<23) > 0 {
					great0 = true
				}
				fraction := int64(0)
				if len(bdata) > 3 {
					fraction = rw.Read_int_try(bdata[3:], "big")
				}
				dt := ""
				if fraction != 0 {
					if great0 {
						dt = fmt.Sprintf("%v:%v:%v", hour, minute, second)
					} else {
						dt = fmt.Sprintf("-%v:%v:%v", hour, minute, second)
					}
				} else {
					if great0 {
						dt = fmt.Sprintf("%v:%v:%v.%v", hour, minute, second, fraction)
					} else {
						dt = fmt.Sprintf("-%v:%v:%v.%v", hour, minute, second, fraction)
					}
				}
				*rwdata[coln] = dt
				rw.PrintLog(coln, rwdata[coln], "TIME2", mdata, rw.tp.Null_bits_list[coln], 0)

			case event_types.MYSQL_TYPE_DATETIME:
				return

			case event_types.MYSQL_TYPE_DATETIME2:
				mdata = rw.Read_uint_try(rw.tp.Metadata[metaOffset:metaOffset+1], "little")
				metaOffset++
				if rwdata[coln] == nil {
					rw.PrintLog(coln, rwdata[coln], "DATETIME2", mdata, rw.tp.Null_bits_list[coln], 1)
					return
				}

				bdata := rw.Read(uint(5 + int((mdata+1)/2)))
				idata := rw.Read_int_try(bdata[:5], "big")
				year_month := ((idata & ((1 << 17) - 1) << 22) >> 22)
				year := int(year_month / 13)
				month := int(year_month % 13)
				day := ((idata & ((1 << 5) - 1) << 17) >> 17)
				hour := ((idata & ((1 << 5) - 1) << 12) >> 12)
				minute := ((idata & ((1 << 6) - 1) << 6) >> 6)
				second := (idata & ((1 << 6) - 1))
				great0 := false
				if idata&(1<<39) > 0 {
					great0 = true
				}
				fraction := int64(0)
				if len(bdata) > 5 {
					fraction = rw.Read_int_try(bdata[5:], "big")
				}
				dt := ""
				if fraction > 0 {
					if fraction != 0 {
						if great0 {
							dt = fmt.Sprintf("%v-%v-%v %v:%v:%v", year, month, day, hour, minute, second)
						} else {
							dt = fmt.Sprintf("-%v-%v-%v %v:%v:%v", year, month, day, hour, minute, second)
						}
					} else {
						if great0 {
							dt = fmt.Sprintf("%v-%v-%v %v:%v:%v.%v", year, month, day, hour, minute, second, fraction)
						} else {
							dt = fmt.Sprintf("-%v-%v-%v %v:%v:%v.%v", year, month, day, hour, minute, second, fraction)
						}
					}
				}
				*rwdata[coln] = dt
				rw.PrintLog(coln, rwdata[coln], "DATETIME2", mdata, rw.tp.Null_bits_list[coln], 0)

			case event_types.MYSQL_TYPE_TIMESTAMP2:
				mdata = rw.Read_uint_try(rw.tp.Metadata[metaOffset:metaOffset+1], "little")
				metaOffset++
				if rwdata[coln] == nil {
					rw.PrintLog(coln, rwdata[coln], "TIMESTAMP2", mdata, rw.tp.Null_bits_list[coln], 1)
					return
				}

				bdata := rw.Read(uint(4 + int((mdata+1)/2)))
				timestampx := rw.Read_int_try(bdata[:4], "big")
				fraction := int64(0)
				if len(bdata) > 4 {
					fraction = rw.Read_int_try(bdata[4:], "big")
				}
				timestampxStr := time.Unix(int64(timestampx), 0).Format("2006-01-02 15:04:05")
				dt := fmt.Sprintf("%v.%v", timestampx, fraction)
				*rwdata[coln] = dt
				rw.PrintLog(coln, rwdata[coln], fmt.Sprintf("%s TIMESTAMP2", timestampxStr), mdata, rw.tp.Null_bits_list[coln], 0)

			case event_types.MYSQL_TYPE_YEAR:
				if rwdata[coln] == nil {
					rw.PrintLog(coln, rwdata[coln], "YEAR", mdata, rw.tp.Null_bits_list[coln], 1)
					return
				}

				data := int(rw.Read(1)[0]) + 1900
				*rwdata[coln] = fmt.Sprintf("%v", data)
				rw.PrintLog(coln, rwdata[coln], "YEAR", mdata, rw.tp.Null_bits_list[coln], 0)

			case event_types.MYSQL_TYPE_STRING:
				mdata = rw.Read_uint_try(rw.tp.Metadata[metaOffset:metaOffset+2], "big")
				metaOffset += 2
				mtype := mdata >> 8
				if rwdata[coln] == nil {
					rw.PrintLog(coln, rwdata[coln], "STRING", mdata, rw.tp.Null_bits_list[coln], 1)
					return
				}

				var data interface{}
				msize := uint64(0)
				if mtype == 247 { // enum
					msize = (mdata & ((1 << 8) - 1))
					if msize >= uint64(1<<8) {
						data = int(rw.Read_uint(2))
					} else {
						data = int(rw.Read_uint(2))
					}
				} else if mtype == 248 { // set
					msize = (mdata & ((1 << 8) - 1))
					data = int(rw.Read_uint(uint((msize + 7) / 8)))
				} else {
					// libbinlogevents/src/binary_log_funcs.cpp
					msize := uint(0)
					mmaxsize := (((mdata >> 4) & 0x300) ^ 0x300) + (mdata & 0x00ff)
					if mmaxsize > 255 {
						msize = rw.Read_uint(2)
					} else {
						msize = rw.Read_uint(1)
					}
					data = rw.Read_String(uint(msize))
				}
				*rwdata[coln] = fmt.Sprintf("'%v'", data)
				rw.PrintLog(coln, rwdata[coln], "STRING", mdata, rw.tp.Null_bits_list[coln], 0)

			case event_types.MYSQL_TYPE_VARCHAR: // varchar/varbinary
				// defer func() {
				// 	if errP := recover(); errP != nil {
				// 		logx.ErrorF("parse type [MYSQL_TYPE_VARCHAR] failed.%v", errP)
				// 	}
				// }()
				if rwdata[coln] == nil {
					rw.PrintLog(coln, rwdata[coln], "VARCHAR", mdata, rw.tp.Null_bits_list[coln], 1)
					return
				}

				mdata = rw.Read_uint_try(rw.tp.Metadata[metaOffset:metaOffset+2], "little")
				metaOffset += 2
				msize := uint(0)
				if mdata > 255 {
					msize = rw.Read_uint(2)
				} else {
					msize = rw.Read_uint(1)
				}

				bdata := rw.Read(msize)
				*rwdata[coln] = fmt.Sprintf("'%s'", string(bdata))
				rw.PrintLog(coln, rwdata[coln], fmt.Sprintf("VARCHAR(%d)", msize), mdata, rw.tp.Null_bits_list[coln], 0)

			case event_types.MYSQL_TYPE_BIT:
				mdata = rw.Read_uint_try(rw.tp.Metadata[metaOffset:metaOffset+2], "little")
				metaOffset += 2
				if rwdata[coln] == nil {
					rw.PrintLog(coln, rwdata[coln], "BIT", mdata, rw.tp.Null_bits_list[coln], 1)
					return
				}

				msize := uint((mdata + 7) / 8)
				data := rw.Read_uint(msize)
				*rwdata[coln] = fmt.Sprintf("%d", data)
				rw.PrintLog(coln, rwdata[coln], "BIT", mdata, rw.tp.Null_bits_list[coln], 0)

			case event_types.MYSQL_TYPE_JSON:
				mdata = rw.Read_uint_try(rw.tp.Metadata[metaOffset:metaOffset+1], "little")
				metaOffset++
				if rwdata[coln] == nil {
					rw.PrintLog(coln, rwdata[coln], "JSON", mdata, rw.tp.Null_bits_list[coln], 1)
					return
				}

				msize := rw.Read_uint(uint(mdata))
				_tdata := rw.Read(msize)
				jbi := json_binary.JsonBinaryImpl{
					JsonType:          int(rw.Read_int_try(_tdata[:1], "little")),
					EventDetailReader: event_ops.EventDetailReader{Bdata: _tdata[1:]},
				}
				if e := jbi.Init(); e != nil {
					logx.ErrorF("fail to parse json binary: %v", e)
				}
				resString := jbi.ToString()
				*rwdata[coln] = fmt.Sprintf("\"%s\"", resString)
				rw.PrintLog(coln, rwdata[coln], "JSON", mdata, rw.tp.Null_bits_list[coln], 0)

			case event_types.MYSQL_TYPE_BLOB:
				/*
					4:longblob/longtext
					3:mediumblob/mediumtext
					2:blob/text
					1:tinyblob/tinytext
				*/
				mdata = rw.Read_uint_try(rw.tp.Metadata[metaOffset-1:metaOffset], "")
				metaOffset++
				if rwdata[coln] == nil {
					rw.PrintLog(coln, rwdata[coln], "BLOG", mdata, rw.tp.Null_bits_list[coln], 1)
					return
				}

				blobSize := rw.Read_uint(uint(mdata))
				if blobSize == 0 {
					logx.ErrorF("read mysql blob size failed.")
					rwdata[coln] = nil
					return
				}
				data := rw.Read_String(uint(blobSize))
				*rwdata[coln] = data
				rw.PrintLog(coln, rwdata[coln], "BLOG", mdata, rw.tp.Null_bits_list[coln], 0)

			case event_types.MYSQL_TYPE_GEOMETRY:
				mdata = rw.Read_uint_try(rw.tp.Metadata[metaOffset-1:metaOffset], "")
				metaOffset++
				if rwdata[coln] == nil {
					rw.PrintLog(coln, rwdata[coln], "GEOMETRY", mdata, rw.tp.Null_bits_list[coln], 1)
					return
				}

				msize := rw.Read_uint(uint(mdata))
				data := rw.Read(msize)
				data2 := make([]string, len(data))
				for k, v := range data {
					data2[k] = fmt.Sprintf("0x%x", v)
				}
				*rwdata[coln] = fmt.Sprint(data2)
				rw.PrintLog(coln, rwdata[coln], "GEOMETRY", mdata, rw.tp.Null_bits_list[coln], 0)
			}
		}()
		metadata = append(metadata, uint(mdata))
	}

	stop_offset = int(rw.Offset)
	logx.DebugF("stop_offset:%d", stop_offset-19)
	return
}

func (rw *RowEvent) Init(bdata []byte, tableMap *TablemapEvent) {
	rw.EventDetailReader.Init(bdata)
	rw.tp = tableMap
	rw.EventType = uint(bdata[4])
	rw.Prepare()
}

func (rw *RowEvent) Prepare() {
	// 跳过event header
	rw.Read(event_types.EVENT_HEAD_SIZE)

	// 开始解析 row event
	/* Master is of an intermediate source tree before 5.1.4. Id is 4 bytes */

	rw.Table_id = rw.Read_uint(6)
	logx.DebugF("Table id:%v", rw.Table_id)
	rw.Flags = rw.Read_uint(2)
	logx.DebugF("Flags:%v", rw.Flags)

	// extra row info
	rw.Extra_row_length = rw.Read_uint(2)
	logx.DebugF("Extra_row_length:%v", rw.Extra_row_length)
	if rw.Extra_row_length > 2 {
		rw.Extra_row_type = rw.Read_uint(1)
		if row_flags[rw.Extra_row_type] == "NDB" {
			rw.Extra_row_info_NDB.Ndb_info_length = rw.Read_uint(1)
			rw.Extra_row_info_NDB.Ndb_info_format = rw.Read_uint(2)
			rw.Extra_row_info_NDB.Ndb_info = rw.Read(rw.Extra_row_info_NDB.Ndb_info_length - 2)
			logx.DebugF("Ndb_info:%v", string(rw.Extra_row_info_NDB.Ndb_info))
		} else if row_flags[rw.Extra_row_type] == "PART" {
			switch rw.EventType {
			case event_types.UPDATE_ROWS_EVENT, event_types.UPDATE_ROWS_EVENT_V1, event_types.PARTIAL_UPDATE_ROWS_EVENT:
				rw.Extra_row_info_U.Partition_id = rw.Read_uint(2)
				rw.Extra_row_info_U.Source_partition_id = rw.Read_uint(2)
				logx.DebugF("Partition_id:%v , Source_partition_id:%v", rw.Extra_row_info_U.Partition_id, rw.Extra_row_info_U.Source_partition_id)
			default:
				rw.Extra_row_info_U.Partition_id = rw.Read_uint(2)
				logx.DebugF("Partition_id:%v", rw.Extra_row_info_U.Partition_id)
			}
		} else {
			rw.Extra_data = rw.Read(rw.Extra_row_length - 3)
			logx.DebugF("Extra_data:%v", string(rw.Extra_data))
		}
	}

	rw.Width = rw.Read_pack_int()
	logx.DebugF("Width:%v", rw.Width)

	// Indicates whether each column is used, one bit per column. 先假设每个字段都用到了？
	rw.Cols = (1 << 64) - 1
	logx.DebugF("Cols:%b", rw.Cols)
	/* 更新前后镜像列是否在使用 ， update
		For WRITE and UPDATE only. Bit-field indicating whether each column is used in the UPDATE_ROWS_EVENT and WRITE_ROWS_EVENT after-image; one bit per column. For this field, the amount of storage required for N columns is INT((N + 7) / 8) bytes.

	         +-------------------------------------------------------+
	         | Event Type | Cols_before_image | Cols_after_image     |
	         +-------------------------------------------------------+
	         |  DELETE    |   Deleted row     |    NULL              |
	         |  INSERT    |   NULL            |    Inserted row      |
	         |  UPDATE    |   Old     row     |    Updated row       |
	         +-------------------------------------------------------+
	*/
	rw.Columns_before_image = rw.Read_uint((rw.Width + 7) / 8)
	logx.DebugF("Columns_before_image:%v", rw.Columns_before_image)
	if rw.EventType == event_types.UPDATE_ROWS_EVENT || rw.EventType == event_types.UPDATE_ROWS_EVENT_V1 {
		rw.Columns_after_image = rw.Read_uint((rw.Width + 7) / 8)
	} else {
		rw.Columns_after_image = rw.Columns_before_image
	}
	logx.DebugF("Columns_after_image:%v", rw.Columns_after_image)

	// 开始读取具体行内容
	logx.DebugF("DATA SIZE:%v", len(rw.Bdata)-int(rw.Offset))
	rw.rowsdata = make([]RowsData, 0)
	for {
		start_offset, stop_offset, rwdata, metadata := rw.Read_row()
		if start_offset == -999 {
			break
		}

		rd := RowsData{}
		switch rw.EventType {
		case event_types.WRITE_ROWS_EVENT, event_types.WRITE_ROWS_EVENT_V1:
			rd.Offset = []uint{uint(start_offset), uint(stop_offset)}
			rd.Data = rwdata
			rd.Metadata = metadata
			rw.rowsdata = append(rw.rowsdata, rd)
		case event_types.DELETE_ROWS_EVENT_V1, event_types.DELETE_ROWS_EVENT: //V1 EVENT是MySQL5.5的版本，这里不考虑
			rd.Offset = []uint{uint(start_offset), uint(stop_offset)}
			rd.Data = rwdata
			rd.Metadata = metadata
			rw.rowsdata = append(rw.rowsdata, rd)
		case event_types.UPDATE_ROWS_EVENT_V1, event_types.UPDATE_ROWS_EVENT, event_types.PARTIAL_UPDATE_ROWS_EVENT:
			// 条件
			rd.Offset = []uint{uint(start_offset), uint(stop_offset)}
			rd.Data = rwdata
			rd.Metadata = metadata

			// 更新值
			rd1 := RowsData{}
			start_offset, stop_offset, rwdata, metadata := rw.Read_row()
			if start_offset == -999 {
				break
			}
			rd1.Offset = []uint{uint(start_offset), uint(stop_offset)}
			rd1.Data = rwdata
			rd1.Metadata = metadata
			rd.NextUpdateRowsData = &rd1

			rw.rowsdata = append(rw.rowsdata, rd)
		}
	}
}

func (rw *RowEvent) Get_sql(op *options_handler.Options) []string {
	var sqlText []string = make([]string, len(rw.rowsdata))
	for k, v := range rw.rowsdata {
		switch rw.EventType {
		case event_types.WRITE_ROWS_EVENT:
			sql := ""
			if op.ToRollback {
				val := rw.tp.Read_column_value(v.Data, v.Metadata, 2)
				sql = fmt.Sprintf("DELETE FROM `%s`.`%s` WHERE (%v)", rw.tp.Dbname, rw.tp.Table_name, val)
			} else {
				val := rw.tp.Read_column_value(v.Data, v.Metadata, 1)
				sql = fmt.Sprintf("INSERT INTO `%s`.`%s` VALUES (%v)", rw.tp.Dbname, rw.tp.Table_name, val)
			}
			sqlText[k] = sql
		case event_types.DELETE_ROWS_EVENT:
			sql := ""
			if op.ToRollback {
				val := rw.tp.Read_column_value(v.Data, v.Metadata, 1)
				sql = fmt.Sprintf("INSERT INTO `%s`.`%s` VALUES (%v)", rw.tp.Dbname, rw.tp.Table_name, val)
			} else {
				val := rw.tp.Read_column_value(v.Data, v.Metadata, 2)
				sql = fmt.Sprintf("DELETE FROM `%s`.`%s` WHERE (%v)", rw.tp.Dbname, rw.tp.Table_name, val)
			}
			sqlText[k] = sql
		case event_types.UPDATE_ROWS_EVENT, event_types.PARTIAL_UPDATE_ROWS_EVENT:
			val1 := rw.tp.Read_column_value(v.Data, v.Metadata, 2)
			val2 := rw.tp.Read_column_value(v.NextUpdateRowsData.Data, v.NextUpdateRowsData.Metadata, 0)
			sql := ""
			if op.ToRollback {
				sql = fmt.Sprintf("UPDATE `%s`.`%s` SET %v WHERE %v", rw.tp.Dbname, rw.tp.Table_name, val1, val2)
			} else {
				sql = fmt.Sprintf("UPDATE `%s`.`%s` SET %v WHERE %v", rw.tp.Dbname, rw.tp.Table_name, val2, val1)
			}
			sqlText[k] = sql

		default:
			logx.ErrorF("unknow row events: %v", rw.EventType)
		}
	}
	return sqlText
}

func (rw *RowEvent) Get_bin(op *options_handler.Options) []byte {
	var eventBdata []byte
	switch rw.EventType {
	case event_types.WRITE_ROWS_EVENT:
		if op.ToRollback {
			rw.EventDetailReader.Bdata[4] = event_types.DELETE_ROWS_EVENT
		}
		eventBdata = rw.EventDetailReader.Bdata

	case event_types.DELETE_ROWS_EVENT:
		if op.ToRollback {
			rw.EventDetailReader.Bdata[4] = event_types.WRITE_ROWS_EVENT
		}
		eventBdata = rw.EventDetailReader.Bdata

	case event_types.UPDATE_ROWS_EVENT, event_types.PARTIAL_UPDATE_ROWS_EVENT:
		for _, v := range rw.rowsdata {
			// update db.tb set (v1) where (v2)
			// 交换一下 v1 、v2 的位置即可
			if op.ToRollback {
				eventBdata = utils.SwapSlicInner(
					[2]uint{v.Offset[0], v.Offset[1]},
					[2]uint{v.NextUpdateRowsData.Offset[0], v.NextUpdateRowsData.Offset[1]},
					rw.EventDetailReader.Bdata,
				)
			} else {
				eventBdata = rw.EventDetailReader.Bdata
			}

			// 一个event内只有一条语句吗？
			break
		}

	default:
		logx.ErrorF("unknow row events: %v", rw.EventType)
	}
	checksumB, errx := utils.Uint32Tobytes(crc32.ChecksumIEEE(eventBdata))
	if errx != nil {
		logx.ErrorF("get checksum of event bytes failed. %s", errx.Error())
		return nil
	}
	fullBdata := append(eventBdata, checksumB...)

	return fullBdata
}

func (rw *RowEvent) PrintLog(coln int, value *string, colType string, meta uint64, nullable uint, isnull int, args ...string) {
	logx.DebugF("### @%v=%v /* %s meta=%v nullable=%d is_null=%d %s*/",
		coln,
		func() string {
			if value == nil {
				return "NULL"
			}
			return *value
		}(),
		colType,
		meta,
		nullable,
		isnull,
		func() string {
			tm := ""
			for _, v := range args {
				tm += v
			}
			return tm
		}(),
	)
}
