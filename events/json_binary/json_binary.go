package json_binary

import (
	"encoding/binary"
	"fmt"
	"gomysqlbinlog/utils/event_ops"
	"gomysqlbinlog/utils/logx"
	"math"
)

/*
	@mysql-8.0.40/sql-common/json_binary.h
	@mysql-8.0.40/sql-common/json_binary.cc

		-------
        | json |
		-------
			|
		----------------
		| type (1 byte) |
        -----------------
				|
	-----------------------------------------------------------------------
	| element-count | size | key-entry* | value-entry* | key* | value*    |
	-----------------------------------------------------------------------
		|				|		|					|		|
----------------------	|		|					|		|
| uint16 (small json)|	|		|					|		|
| uint32 (large json)|	|		|					|		|
----------------------	|		|					|		|
						|		|					|		|
	----------------------		|					|	----------------
	| uint16 (small json)|		|					|	| utf8mb4-data  |
	| uint32 (large json)|		|					|	----------------
	----------------------		|					|
								|					|
					---------------------------		|
					| key-offset | key-length |		|
					---------------------------		|
						|			|				|
						|			|				|
						|			|		----------------------------------------
	----------------------			|		| type 	｜	offset-or-inlined-value   |
	| uint16 (small json)|			|		----------------------------------------
	| uint32 (large json)|			|		// 此字段保存值存储位置的偏移量，或者如果值足够小可以内联
	----------------------			|		// 或者如果值足够小可以内联（即，如果它是JSON文本或足够小的[u]int) 则保值本身。
						         	|
						         	|
						         	|
							---------
							| uint16 |
							---------








  type ::=
      0x00 |       // small JSON object
      0x01 |       // large JSON object
      0x02 |       // small JSON array
      0x03 |       // large JSON array
      0x04 |       // literal (true/false/null)
      0x05 |       // int16
      0x06 |       // uint16
      0x07 |       // int32
      0x08 |       // uint32
      0x09 |       // int64
      0x0a |       // uint64
      0x0b |       // double
      0x0c |       // utf8mb4 string
      0x0f         // custom data (any MySQL data type)

  value ::=
      object  |
      array   |
      literal |
      number  |
      string  |
      custom-data

*/

const (
	JSON_TYPE_SMALL_OBJECT   = 0x00
	JSON_TYPE_LARGE_OBJECT   = 0x01
	JSON_TYPE_SMALL_ARRAY    = 0x02
	JSON_TYPE_LARGE_ARRAY    = 0x03
	JSON_TYPE_LITERAL        = 0x04
	JSON_TYPE_INT16          = 0x05
	JSON_TYPE_UINT16         = 0x06
	JSON_TYPE_INT32          = 0x07
	JSON_TYPE_UINT32         = 0x08
	JSON_TYPE_INT64          = 0x09
	JSON_TYPE_UINT64         = 0x0a
	JSON_TYPE_DOUBLE         = 0x0b
	JSON_TYPE_UTF8MB4_STRING = 0x0c
	JSON_TYPE_CUSTOM_DATA    = 0x0f

	// object size
	JSON_SMALL_SIZE = 2
	JSON_LARGE_SIZE = 4
)

type JsonBinaryImpl struct {
	event_ops.EventDetailReader
	JsonType int
	JsonBinaryHeaderImpl
}

type JsonBinaryHeaderImpl struct {
	ReadSize       uint
	ElementCounter uint
	Size           uint
	KeyEntry       []KeyEntryImpl
	ValEntry       []ValueEntryImpl
	Key            []string
	Value          []string
}

type KeyEntryImpl struct {
	KeyOffset uint
	KeyLen    uint
}

type ValueEntryImpl struct {
	ValueType     uint
	ValueOffset   uint
	ValueTypeName string
	ValueString   string
}

func (j *JsonBinaryImpl) Init() error {
	if j.JsonType == JSON_TYPE_SMALL_OBJECT || j.JsonType == JSON_TYPE_SMALL_ARRAY {
		j.ReadSize = 2
	} else {
		j.ReadSize = 4
	}
	j.InitElementCounter()
	if err := j.InitSize(); err != nil {
		return err
	}
	j.InitKeyEntry()
	j.InitValueEntry()
	j.InitKey()
	if err := j.InitValue(); err != nil {
		return err
	}
	return nil
}

func (j *JsonBinaryImpl) Read_uint() uint {
	return j.EventDetailReader.Read_uint(j.ReadSize)
}

func (j *JsonBinaryImpl) Read_inline_value(valType uint) (vx string, vTName string) {
	/*
		@mysql-8.0.4/sql-common/json_binary.cc:398:inlined_type
		// 这些类型都是直接存储在 value-entry 内，因为足够小
		// LITERAL 可以当作布尔类型去解析
		  Will a value of the specified type be inlined?
		  @param type  the type to check
		  @param large true if the large storage format is used
		  @return true if the value will be inlined

		static bool inlined_type(uint8 type, bool large) {
			switch (type) {
			  case JSONB_TYPE_LITERAL:
			  case JSONB_TYPE_INT16:
			  case JSONB_TYPE_UINT16:
				return true;
			  case JSONB_TYPE_INT32:
			  case JSONB_TYPE_UINT32:
				return large;
			  default:
				return false;
			}
		  }

		// 开始解析inline-value
		@mysql-8.0.4/sql-common/json_binary.cc:451:attempt_inline_value
	*/

	v := int64(0)
	switch valType {
	case JSON_TYPE_INT16:
		v = int64(j.EventDetailReader.Read_int(2))
		vTName = "JSON_TYPE_INT16"
	case JSON_TYPE_UINT16:
		v = int64(j.EventDetailReader.Read_uint(2))
		vTName = "JSON_TYPE_UINT16"
	case JSON_TYPE_INT32:
		v = int64(j.EventDetailReader.Read_int(4))
		vTName = "JSON_TYPE_INT32"
	case JSON_TYPE_UINT32:
		v = int64(j.EventDetailReader.Read_uint(4))
		vTName = "JSON_TYPE_UINT32"
	case JSON_TYPE_INT64:
		v = int64(j.EventDetailReader.Read_int(8))
		vTName = "JSON_TYPE_INT64"
	case JSON_TYPE_UINT64:
		v = int64(j.EventDetailReader.Read_uint(8))
		vTName = "JSON_TYPE_UINT64"
	}
	return fmt.Sprint(v), vTName
}

func (j *JsonBinaryImpl) Read_varchar(offset int) string {
	/*
		@mysql-8.0.40/sql-common/json_binary.cc:252:append_variable_length
		@mysql-8.0.40/sql-common/json_binary.cc:285:read_variable_length
		Append a length to a String. The number of bytes used to store the length
		uses a variable number of bytes depending on how large the length is. If the
		highest bit in a byte is 1, then the length is continued on the next byte.
		The least significant bits are stored in the first byte.

		------------------------------------------------------
		| flag(1 bit) | 7 bit data (if flag, 8 bit data*128) |
		------------------------------------------------------
		读mysql的varchar的 记录长度的大小, 范围字节数量和大小
		如果第一bit是1 就表示要使用2字节表示:
			后面1字节表示 使用有多少个128字节, 然后加上前面1字节(除了第一bit)的数据(0-127) 就是最终数据

		------------------------------------------------------------------------------------------
		| first bit  | len                                                                       |
		------------------------------------------------------------------------------------------
		| 0          | 第一个字节0-7位                                                            |
		| 1          | (第一个字节0-7位)<<7  + 下一个字节0-7位                                     |
		| 1          | (第一个字节0-7位)<<14 + (下一个字节0-7位)<<7   +        (下一个字节0-7位)    |
		| 1          | 以此类推..........                                                         |
		------------------------------------------------------------------------------------------
		每个字节第一位(第八位)作为标志位，如果为0，则第0-7位为长度整形，如果为1，则需要继续判断下一个字节中的最高位是否为1
		如为1，则0-7位为长度整形，以此类推，直到下一个字节中最高位0，则结束
		// The length shouldn't exceed 32 bits.
		且长度不应超过32位，4个字节

	*/
	varlenSize := 1
	lenx := j.Read_int_try(j.Bdata[offset:offset+varlenSize], "little")
	if flag := (lenx & (1 << 7)); flag == 128 {
		varlenSize = 2
		offset2 := j.Bdata[offset : offset+varlenSize]
		lenx = j.Read_int_try(offset2[0:1], "little")*128 + j.Read_int_try(offset2[1:], "little") - (1 << 7)
	}
	return j.Read_string_try(j.Bdata[offset+varlenSize : offset+varlenSize+int(lenx)])
}

func (j *JsonBinaryImpl) ToString() string {
	/*
		array:
		{[1,2,3,4,5,6,7]}

		object
		{"key":"value"}

		array object混合
		{[{"key":"value"},1,2,{"key":[1,2,4,5]}}
	*/

	var res []string = make([]string, len(j.ValEntry))
	switch j.JsonType {
	case JSON_TYPE_SMALL_ARRAY, JSON_TYPE_LARGE_ARRAY:
		for k, v := range j.ValEntry {
			if v.ValueString != "" {
				res = append(res, v.ValueString)
			} else {
				res = append(res, j.Value[k])
			}
		}
	case JSON_TYPE_SMALL_OBJECT, JSON_TYPE_LARGE_OBJECT:
		for k, v := range j.Key {
			kv := ""
			if j.ValEntry[k].ValueString != "" {
				kv = fmt.Sprintf("\\\"%s\\\":%s", v, j.ValEntry[k].ValueString)
			} else {
				// switch j.ValEntry[k].ValueType {
				// case JSON_TYPE_LARGE_OBJECT, JSON_TYPE_SMALL_OBJECT:
				// 	kv = fmt.Sprintf("\"%s\":{%s}", v, j.Value[k])
				// default:
				kv = fmt.Sprintf("\\\"%s\\\":%s", v, j.Value[k])

			}
			res = append(res, kv)
		}
	}
	return func() string {
		_res := ""
		for _, v := range res {
			if _res == "" {
				if v != "" {
					_res = "{" + v
				}
				continue
			}
			_res = _res + "," + v

		}
		return _res + "}"
	}()
}

func (j *JsonBinaryImpl) InitElementCounter() {
	j.ElementCounter = j.Read_uint()
}

func (j *JsonBinaryImpl) InitSize() error {
	j.Size = j.Read_uint()
	if j.Size != uint(len(j.Bdata)) {
		return fmt.Errorf("invalid json data, size:%v , real size:%v", j.Size, len(j.Bdata))
	}
	return nil
}

func (j *JsonBinaryImpl) InitKeyEntry() {
	if j.JsonType != JSON_TYPE_SMALL_OBJECT && j.JsonType != JSON_TYPE_LARGE_OBJECT {
		j.KeyEntry = nil
		return
	}
	for i := uint(0); i < j.ElementCounter; i++ {
		j.KeyEntry = append(j.KeyEntry, KeyEntryImpl{
			KeyOffset: j.Read_uint(),
			KeyLen:    j.EventDetailReader.Read_uint(2),
		})
	}
}

func (j *JsonBinaryImpl) InitValueEntry() {
	if j.JsonType != JSON_TYPE_SMALL_OBJECT &&
		j.JsonType != JSON_TYPE_LARGE_OBJECT &&
		j.JsonType != JSON_TYPE_SMALL_ARRAY &&
		j.JsonType != JSON_TYPE_LARGE_ARRAY {
		j.ValEntry = nil
		return
	}

	for i := uint(0); i < j.ElementCounter; i++ {
		ve := ValueEntryImpl{}
		ve.ValueType = uint(j.EventDetailReader.Read(1)[0])

		switch ve.ValueType {
		case JSON_TYPE_LARGE_ARRAY, JSON_TYPE_LARGE_OBJECT, JSON_TYPE_SMALL_ARRAY, JSON_TYPE_SMALL_OBJECT:
			ve.ValueTypeName = "JSON_OBJECT_OR_JSON_ARRAY"
			ve.ValueOffset = j.Read_uint()

		case JSON_TYPE_LITERAL:
			/*
				literal ::=
				0x00 |   // JSON null literal
				0x01 |   // JSON true literal
				0x02 |   // JSON false literal
			*/
			ve.ValueTypeName = "bool"
			v := j.Read_uint()
			if v == 1 {
				ve.ValueString = "true"
			} else if v == 2 {
				ve.ValueString = "false"
			} else if v == 0 {
				ve.ValueString = "null"
			}
			ve.ValueTypeName = "literal"

		case JSON_TYPE_INT16, JSON_TYPE_UINT16, JSON_TYPE_INT32, JSON_TYPE_UINT32, JSON_TYPE_INT64, JSON_TYPE_UINT64:
			ve.ValueString, ve.ValueTypeName = j.Read_inline_value(ve.ValueType)

		case JSON_TYPE_DOUBLE:
			ve.ValueTypeName = "doubule"
			ve.ValueString = fmt.Sprint(j.Read_uint())

		case JSON_TYPE_UTF8MB4_STRING:
			ve.ValueTypeName = "string"
			ve.ValueOffset = j.Read_uint()

		}
		j.ValEntry = append(j.ValEntry, ve)
	}

}

func (j *JsonBinaryImpl) InitKey() {
	defer func() {
		if errx := recover(); errx != nil {
			logx.ErrorF("InitKey failed.%v", errx)
		}
	}()
	j.Key = make([]string, len(j.KeyEntry))
	for k, v := range j.KeyEntry {
		keyName := string(j.Bdata[v.KeyOffset : v.KeyOffset+v.KeyLen])
		j.Key[k] = keyName
	}
}

func (j *JsonBinaryImpl) InitValue() error {
	j.Value = make([]string, len(j.ValEntry))
	for k, v := range j.ValEntry {
		switch v.ValueType {
		case JSON_TYPE_UTF8MB4_STRING:
			j.Value[k] = fmt.Sprintf("\\\"%s\\\"", j.Read_varchar(int(v.ValueOffset)))
		case JSON_TYPE_DOUBLE:
			bdata := j.Bdata[v.ValueOffset : v.ValueOffset+8]
			doubleData := math.Float64frombits(binary.LittleEndian.Uint64(bdata))
			j.Value[k] = fmt.Sprintf("%f", doubleData)
		case JSON_TYPE_SMALL_OBJECT, JSON_TYPE_LARGE_OBJECT, JSON_TYPE_SMALL_ARRAY, JSON_TYPE_LARGE_ARRAY:
			jsonSize := j.Read_int_try(j.Bdata[v.ValueOffset+j.ReadSize:v.ValueOffset+j.ReadSize+j.ReadSize], "little")
			x := JsonBinaryImpl{JsonType: int(v.ValueType)}
			if int64(v.ValueOffset)+jsonSize == int64(len(j.Bdata)) {
				x.Bdata = j.Bdata[v.ValueOffset:]
			} else {
				x.Bdata = j.Bdata[v.ValueOffset : int64(v.ValueOffset)+jsonSize]
			}

			if err := x.Init(); err != nil {
				return err
			}
			j.Value[k] = x.ToString()
			/*j.Value[k] = func() string {
				_res := ""
				for _, v := range x.ToString() {
					if _res == "" {
						_res = v
						continue
					}
					_res = _res + "," + v
				}
				return _res
			}()*/

		default:
			// 除此之外都是 inlinedata 了
			j.Value[k] = v.ValueString

		}
	}
	return nil
}
