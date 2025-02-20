package utils

import (
	"bufio"
	"fmt"
	"gomysqlbinlog/utils/logx"
	"os"
)

type FileW struct {
	fs    *os.File
	fsBuf *bufio.Writer
}

func Output(format string, args ...interface{}) {
	fmt.Printf(format, args...)
}

func (f *FileW) Init(filename string) {
	fs, err := os.OpenFile(filename, os.O_APPEND|os.O_WRONLY, 0644)
	if err != nil {
		logx.Panic(err)
	}

	f.fsBuf = bufio.NewWriter(fs)
	f.fs = fs
}

func (f *FileW) WriteToFile(format string, args ...interface{}) {
	_, err := f.fsBuf.WriteString(fmt.Sprintf(format, args...))
	if err != nil {
		fmt.Println(err)
	}
}

func (f *FileW) Close() {
	err := f.fsBuf.Flush()
	if err != nil {
		logx.Error(err)
	}
	err = f.fs.Close()
	if err != nil {
		logx.Error(err)
	}
}
