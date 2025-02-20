package dml_event

import (
	"fmt"
	"gomysqlbinlog/event_types"
	"gomysqlbinlog/utils/event_ops"
	"gomysqlbinlog/utils/logx"
)

type TablemapEvent struct {
	event_ops.EventDetailReader
	Signed_list         []uint
	Signed_list_boolean bool
	Table_id            uint   // uint 6bytes
	Flags               uint   // uint 2bytes
	Dbname              string // Dbname_length
	Dbname_length       uint
	Table_name          string // Tablename_length
	Tablename_length    uint
	Column_name         []string
	Column_count        uint
	Column_type         []int
	Metadata_length     uint
	Metadata            []byte
	Null_bits           uint
	Null_bits_list      []uint
	Null_bits_boolean   []bool
	Geom_type           []byte
	Opt                 []OptMetaFileds
}

type OptMetaFileds struct {
	Meta_type  uint
	Meta_len   uint
	Meta_value []byte
}

func Reverse(sl []uint) {
	for x, y := 0, len(sl)-1; x < y; x, y = x+1, y-1 {
		sl[x], sl[y] = sl[y], sl[x]
	}
}

// mysql-8.0.40/libbinlogevents/src/rows_event.cpp:34:Table_map_event
func (t *TablemapEvent) Init(eventByteData []byte, offset uint) {
	t.EventDetailReader.Init(eventByteData)
	//var event_type = uint8(eventByteData[4])

	// 跳过event header
	t.EventDetailReader.Read(19)

	// 开始解析 TablemapEvent
	t.Table_id = t.EventDetailReader.Read_uint(6)
	t.Flags = t.EventDetailReader.Read_uint(2)
	t.Dbname_length = t.EventDetailReader.Read_uint(1)
	t.Dbname = t.EventDetailReader.Read_String(t.Dbname_length)
	_ = t.EventDetailReader.Read(1) // \x00结尾

	t.Tablename_length = t.EventDetailReader.Read_uint(1)
	t.Table_name = t.EventDetailReader.Read_String(t.Tablename_length)
	_ = t.EventDetailReader.Read(1) // \x00结尾

	// column
	t.Column_count = t.EventDetailReader.Read_pack_int()
	t.Column_type = make([]int, 0)
	for _, v := range t.EventDetailReader.Read(t.Column_count) {
		t.Column_type = append(t.Column_type, int(uint8(v)))
	}

	// metadata ?
	t.Metadata_length = t.EventDetailReader.Read_pack_int()
	t.Metadata = t.EventDetailReader.Read(t.Metadata_length)

	// null_bits_list 是否可空符号位
	null_bits_list := make([]uint, 0)
	t.Null_bits = t.EventDetailReader.Read_uint((t.Column_count + 7) / 8)
	/*
		@mysql-8.0.40/sql/rpl_record.h:280:Bit_stream_base :: bool get()
		Read the next bit and move the read position one bit forward.
		@return true if the bit was 1, false if the bit was 0.
		每个字节的二进制从右到左的每一位代表每一列是否可空，通过位与运算取出每一列是否可空,
		使用小端字节序读取一个无符号整型，刚好是每个字节从右往左的排列
	*/
	for x := 0; x < 8*int((t.Column_count+7)/8); x++ {
		if t.Null_bits&(1<<x) == 0 {
			null_bits_list = append(null_bits_list, 0)
		} else {
			null_bits_list = append(null_bits_list, 1)
		}
	}

	t.Null_bits_list = null_bits_list
	//copy(t.Null_bits_list, null_bits_list)

	// Null_bits_boolean
	null_bits_boolean := make([]bool, 0)
	for _, nbl := range null_bits_list {
		null_bits_boolean = append(null_bits_boolean, nbl == 1)
	}
	t.Null_bits_boolean = null_bits_boolean[:t.Column_count]

	//logx.DebugF("offset:%d size:%d", offset+t.Offset, len(eventByteData))

	// 8.0 新增opt optional metadata fields (比如是否有符号等信息.)
	// @mysql-8.0.40/src/rows_events.cpp:321:Table_map_event::Optional_metadata_fields::Optional_metadata_fields
	t.Opt = make([]OptMetaFileds, 0)
	for {
		x := t.Read(1)
		if x == nil {
			break
		}
		optx := new(OptMetaFileds)
		optx.Meta_type = uint(x[0])
		optx.Meta_len = t.Read_pack_int()
		optx.Meta_value = t.Read(optx.Meta_len)
		t.Opt = append(t.Opt, *optx)
	}

	logx.DebugF(`
offset         : %d
size           : %d
TABLE_ID       : %v
FLAGS          : %v
DBNAME         : %v
TABLENAME      : %v
COLUMN COUNT   : %v
column type    : %v
metadata length: %v
metadata       : %v
null_bits      : %b(%d)
null_bits_list : %v
null_bit_bool  : %v
opt            : %+v`,
		offset+t.Offset, len(eventByteData),
		t.Table_id,
		t.Flags,
		t.Dbname,
		t.Table_name,
		t.Column_count,
		t.Column_type, //是MYSQL_TYPE_ 非innodb类型
		t.Metadata_length,
		t.Metadata,
		t.Null_bits,
		t.Null_bits,
		t.Null_bits_list,
		t.Null_bits_boolean,
		t.Opt,
	)

	if len(t.Opt) != 0 {
		for _, v := range t.Opt {
			if v.Meta_type == event_types.IGNEDNESS {
				va := t.EventDetailReader.Read_uint_try(v.Meta_value, "little")
				//self.signed_list = [ True if v&(1<<y) else False for y in range(len(x[2])*8) ]
				for b := 0; b < len(v.Meta_value)*8; b++ {
					t.Signed_list = append(t.Signed_list, uint(va&(1<<b)))
				}
				Reverse(t.Signed_list)
				t.Signed_list_boolean = true
				logx.DebugF("signed_list(True:unsigned) %v", t.Signed_list)
			} else if v.Meta_type == event_types.COLUMN_NAME {
				var offset uint = 0
				if t.Column_name == nil {
					t.Column_name = make([]string, 0)
				}

				for {
					if offset >= v.Meta_len {
						break
					}
					namesize := uint(v.Meta_value[offset : offset+1][0])
					offset++
					columnName := fmt.Sprintf("%x", v.Meta_value[offset:offset+namesize])
					offset += namesize
					t.Column_name = append(t.Column_name, columnName)
				}
				logx.DebugF("Column_name:%v", t.Column_name)
			} else if v.Meta_type == event_types.GEOMETRY_TYPE { //空间坐标
				t.Geom_type = v.Meta_value
			}
		}
	}
}

func (t *TablemapEvent) Read_column_name() []string {
	if len(t.Column_name) == 0 {
		res := make([]string, 0)
		for x := 1; x <= int(t.Column_count); x++ {
			res = append(res, fmt.Sprintf("@%v", x))
		}
		return res
	}
	return t.Column_name
}

func (t *TablemapEvent) Read_column_value(rowValue []*string, metadata []uint, kvork int) string {
	var rowValueOffset uint = 0
	cl := t.Read_column_name()
	rdata := ""
	if len(rowValue) == 0 {
		return rdata
	}

	for v := range t.Column_count {
		var keystr string
		var valuestr string
		colType := uint(t.Column_type[v])

		if v > uint(len(cl)) {
			keystr = "NULL"
		} else {
			keystr = cl[v]
		}

		if v > uint(len(rowValue)) {
			valuestr = "NULL"
		} else {
			// for rowValue[rowValueOffset] == nil && rowValueOffset <= uint(len(rowValue)) {
			// 	rowValueOffset++
			// }
			if rowValue[rowValueOffset] == nil {
				valuestr = "NULL"
			} else {
				valuestr = *rowValue[rowValueOffset]
			}
		}

		meta := metadata[v]
		if ((meta>>8) == 247 || (meta>>8) == 248) || (colType == event_types.MYSQL_TYPE_DECIMAL ||
			colType == event_types.MYSQL_TYPE_TINY ||
			colType == event_types.MYSQL_TYPE_SHORT ||
			colType == event_types.MYSQL_TYPE_LONG ||
			colType == event_types.MYSQL_TYPE_FLOAT ||
			colType == event_types.MYSQL_TYPE_DOUBLE ||
			colType == event_types.MYSQL_TYPE_LONGLONG ||
			colType == event_types.MYSQL_TYPE_INT24 ||
			colType == event_types.MYSQL_TYPE_YEAR ||
			colType == event_types.MYSQL_TYPE_NEWDECIMAL ||
			colType == event_types.MYSQL_TYPE_ENUM ||
			colType == event_types.MYSQL_TYPE_SET ||
			colType == event_types.MYSQL_TYPE_BIT ||
			colType == event_types.MYSQL_TYPE_GEOMETRY) {
			if kvork == 0 {
				rdata += fmt.Sprintf("`%v`=%v, ", keystr, valuestr)
			} else if kvork == 1 {
				rdata += fmt.Sprintf("%v, ", valuestr)
			} else if kvork == 2 {
				rdata += fmt.Sprintf("`%v`=%v and ", keystr, valuestr)
			}
		} else {
			if kvork == 0 {
				rdata += fmt.Sprintf("`%v`=%v, ", keystr, valuestr)
			} else if kvork == 1 {
				rdata += fmt.Sprintf("%v, ", valuestr)
			} else if kvork == 2 {
				rdata += fmt.Sprintf("`%v`=%v and ", keystr, valuestr)
			}
		}
		rowValueOffset++
	}
	if kvork < 2 {
		return rdata[:len(rdata)-2]
	}
	return rdata[:len(rdata)-5]
}
