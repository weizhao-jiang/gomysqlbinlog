package utils

import (
	"encoding/base64"
	"fmt"
	"gomysqlbinlog/binlog_header"
	"testing"
)

func Read(n uint, offset *uint, bdata []byte) []byte {
	if n+*offset > uint(len(bdata)) {
		return nil
	}
	bPos := *offset
	*offset = *offset + n
	return bdata[bPos : bPos+n]
}

func TestBase64(t *testing.T) {
	var offset *uint = new(uint)
	b64_1 := "ocE1Zx5nFAAAPAAAAM0BAAAAAG4AAAAAAAEAAgAB/wABAAAAAAIAAAAAAwAAAAAEAAAAAAUAAABISV3l"

	bdata, err := base64.StdEncoding.DecodeString(b64_1)
	if err != nil {
		fmt.Println(err)
	}

	header_data := Read(19, offset, bdata)
	for header_data != nil {
		header := binlog_header.EventHeader{}
		header.Init(header_data)
		header.ToString()
		Read(uint(header.Event_data.Event_size-19), offset, bdata)

		header_data = Read(19, offset, bdata)
	}

}

func Test1(t *testing.T) {
	// 定义一个无符号整数
	var num uint32 = 3912761516
	fmt.Println(Uint32Tobytes(num))
}
