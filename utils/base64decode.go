package utils

import (
	"bytes"
	"encoding/base64"
	"encoding/binary"
)

func Base64Decode(b []byte) string {
	var dataStr string
	for x := 0; x < len(b); x += 57 {
		if dataStr == "" {
			dataStr = base64.StdEncoding.EncodeToString(b[x : x+57])
		} else {
			if x+57 > len(b) {
				break
			}
			dataStr = dataStr + "\n" + base64.StdEncoding.EncodeToString(b[x:x+57])
		}
	}
	return dataStr
}

func Uint32Tobytes(n uint32) ([]byte, error) {
	var nb *bytes.Buffer = new(bytes.Buffer)
	e := binary.Write(nb, binary.LittleEndian, n)
	if e != nil {
		return nil, e
	}
	return nb.Bytes(), nil
}
