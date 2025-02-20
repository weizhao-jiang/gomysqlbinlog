package event_ops

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"gomysqlbinlog/utils/logx"
)

type EventDetailReader struct {
	Offset uint
	Bdata  []byte
	//_bdata []byte
}

// 补0
func fillZero(bdata []byte, dataType string, byteOrder string) []byte {
	var aimLen int
	switch dataType {
	case "uint", "int":
		if len(bdata) == 0 || len(bdata) == 1 || len(bdata) == 2 || len(bdata) == 4 || len(bdata) == 8 {
			return bdata
		}
		switch len(bdata) {
		case 3:
			aimLen = 4
		case 5, 6, 7:
			aimLen = 8
		}
	case "float":
		if len(bdata) == 4 || len(bdata) == 8 {
			return bdata
		}
		switch len(bdata) {
		case 1, 2, 3:
			aimLen = 4
		case 5, 6, 7:
			aimLen = 8
		}
	}
	tb := make([]byte, aimLen)
	if byteOrder == "big" {
		// 高字节在低地址，左补 0x00
		copy(tb[aimLen-len(bdata):], bdata)
	} else {
		// 高字节在高地址，右补 0x00
		copy(tb[:len(bdata)], bdata)
	}

	return tb
}

func (c *EventDetailReader) Init(data []byte) {
	c.Offset = 0
	c.Bdata = data
}

func (c *EventDetailReader) Read_uint(n uint) uint {
	data := c.Read(n)
	return uint(c.Read_uint_try(data, "little"))
}

func (c *EventDetailReader) Read_uint_try(bdata []byte, byteOrder string) uint64 {
	var be binary.ByteOrder = binary.LittleEndian
	if byteOrder == "big" {
		be = binary.BigEndian
	}

	if bdata == nil {
		logx.Warn("bytes is nil")
		return 0
	}
	if len(bdata) == 0 || len(bdata) > 8 {
		logx.ErrorF("invalid len of uint bytes: %v", len(bdata))
		return 0
	}

	defer func() {
		if err := recover(); err != nil {
			logx.ErrorF("read uint panic.%v", err)
		}
	}()

	nb := fillZero(bdata, "uint", byteOrder)
	bbuff := new(bytes.Buffer)
	if _, err := bbuff.Write(nb); err != nil {
		logx.ErrorF("bytes buffer write error.%v", err)
		return 0
	}

	switch len(nb) {
	case 1:
		uit := uint8(0)
		if binary.Read(bbuff, be, &uit) == nil {
			return uint64(uit)
		}
	case 2:
		uit := uint16(0)
		if binary.Read(bbuff, be, &uit) == nil {
			return uint64(uit)
		}
	case 4:
		uit := uint32(0)
		if binary.Read(bbuff, be, &uit) == nil {
			return uint64(uit)
		}
	case 8:
		uit := uint64(0)
		if binary.Read(bbuff, be, &uit) == nil {
			return uit
		}
	}
	return 0
}

func (c *EventDetailReader) Read_float(n int) float64 {
	defer func() {
		if err := recover(); err != nil {
			logx.Panic(err)
		}
	}()

	data := c.Read(uint(n))
	if data == nil {
		logx.Warn("bytes is nil.")
		return 0
	}
	return c.Read_float_try(data, "little")
}

func (c *EventDetailReader) Read_float_try(bdata []byte, byteOrder string) float64 {
	var be binary.ByteOrder = binary.LittleEndian
	if byteOrder == "big" {
		be = binary.BigEndian
	}

	if bdata == nil {
		logx.Warn("bytes is nil")
		return 0
	}
	if len(bdata) == 0 || len(bdata) > 8 {
		logx.ErrorF("invalid len of float bytes: %v", len(bdata))
		return 0
	}

	defer func() {
		if err := recover(); err != nil {
			logx.ErrorF("read float panic.%v", err)
		}
	}()

	nb := fillZero(bdata, "float", byteOrder)
	bbuff := new(bytes.Buffer)
	if _, err := bbuff.Write(nb); err != nil {
		logx.ErrorF("bytes buffer write error.%v", err)
		return 0
	}

	switch len(nb) {
	case 4:
		uit := float32(0)
		if binary.Read(bbuff, be, &uit) == nil {
			return float64(uit)
		}
	case 8:
		uit := float64(0)
		if binary.Read(bbuff, be, &uit) == nil {
			return uit
		}
	}
	return 0
}

func (c *EventDetailReader) Read_int(n uint) int64 {
	defer func() {
		if err := recover(); err != nil {
			logx.Panic(err)
		}
	}()
	data := c.Read(n)
	return c.Read_int_try(data, "little")
}

func (c *EventDetailReader) Read_int_try(bdata []byte, byteOrder string) int64 {
	var res int64 = -999
	var res8 int8
	var res16 int16
	var res32 int
	var res32_ int32
	var res64 int64

	var be binary.ByteOrder = binary.LittleEndian
	if byteOrder == "big" {
		be = binary.BigEndian
	}
	defer func() {
		if errx := recover(); errx != nil {
			logx.Panic(errx)
		}
	}()
	if bdata == nil {
		logx.Error("empty bytes.")
		return res
	}
	nb := fillZero(bdata, "int", byteOrder)
	buf := new(bytes.Buffer)
	_, err := buf.Write(nb)
	if err != nil {
		logx.Error(err)
		return res
	}

	switch len(nb) {
	case 1:
		t := int8(0)
		err = binary.Read(buf, be, &t)
		if err == nil {
			return int64(t)
		}
	case 2:
		t := int16(0)
		err = binary.Read(buf, be, &t)
		if err == nil {
			return int64(t)
		}
	case 4:
		t := int32(0)
		err = binary.Read(buf, be, &t)
		if err == nil {
			return int64(t)
		}
	case 8:
		t := int64(0)
		err = binary.Read(buf, be, &t)
		if err == nil {
			return t
		}
	}

	err = binary.Read(buf, be, &res8)
	if err != nil {
		if binary.Read(buf, be, &res16) != nil {
			if binary.Read(buf, be, &res32) != nil {
				if binary.Read(buf, be, &res32_) != nil {
					if binary.Read(buf, be, &res64) != nil {
						logx.ErrorF("all retry failed.%v", err)
					} else {
						res = res64
					}
				} else {
					res = int64(res32_)
				}
			} else {
				res = int64(res32)
			}
		} else {
			res = int64(res16)
		}
	} else {
		res = int64(res8)
	}
	return res
}

func (c *EventDetailReader) Read(n uint) []byte {
	if c.Offset+n > uint(len(c.Bdata)) {
		return nil
	}
	res := c.Bdata[c.Offset:(c.Offset + n)]
	c.Offset += n
	return res
}

func (c *EventDetailReader) Read_pack_int() uint {

	/*
	   #https://dev.mysql.com/doc/dev/mysql-server/latest/classmysql_1_1binlog_1_1event_1_1Binary__log__event.html#packed_integer
	   ---------------------------------------------------------------------------------------------------------
	   First byte   format
	   0-250        The first byte is the number (in the range 0-250), and no more bytes are used.
	   252          Two more bytes are used.   The number is in the range 251-0xffff.
	   253          Three more bytes are used. The number is in the range 0xffff-0xffffff.
	   254          Eight more bytes are used. The number is in the range 0xffffff-0xffffffffffffffff.
	   ---------------------------------------------------------------------------------------------------------
	*/
	return c.Read_net_int()
}

func (c *EventDetailReader) Read_net_int() uint {
	// 1 3 4 9 (不含第一字节)
	data := c.Read_uint(1)
	if data < 251 {
		return data
	} else if data == 251 {
		return c.Read_uint(1)
	} else if data == 252 {
		return c.Read_uint(2)
	} else if data == 253 {
		return c.Read_uint(3)
	} else {
		return c.Read_uint(8)
	}
}

func (c *EventDetailReader) Read_UUID(n uint) string {
	b := c.Read(uint(n))

	defer func() {
		if err := recover(); err != nil {
			logx.Error(err)
		}
	}()
	res := fmt.Sprintf("%x-%x-%x-%x-%x",
		b[0:4],
		b[4:6],
		b[6:8],
		b[8:10],
		b[10:],
	)

	return res
}

func (c *EventDetailReader) Read_String(n uint) string {
	b := c.Read(uint(n))
	return c.Read_string_try(b)
}

func (c *EventDetailReader) Read_string_try(bdata []byte) string {
	defer func() {
		if err := recover(); err != nil {
			logx.Error(err)
		}
	}()
	if bdata == nil {
		logx.ErrorF("bytes to string failed.empty bytes.")
		return ""
	}
	return string(bdata)
}

func (c *EventDetailReader) Read_Until_End(end byte) []byte {
	var res []byte
	defer func() {
		if err := recover(); err != nil {
			logx.Error(err)
		}
	}()
	for {
		b := c.Read(1)
		if b == nil {
			return nil
		}
		if b[0] == end {
			break
		}
		res = append(res, b...)
	}
	return res
}
