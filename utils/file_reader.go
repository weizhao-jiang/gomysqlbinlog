package utils

import (
	"bufio"
	"fmt"
	"gomysqlbinlog/utils/logx"
	"io"
	"os"
)

type FileReaders struct {
	Filename  string
	File      *os.File
	BufReader *bufio.Reader
	pos       uint
}

func (r *FileReaders) Init() (err error) {
	if r.Filename == "" {
		return fmt.Errorf("filename is empty..")
	}

	r.File, err = os.Open(r.Filename)
	r.BufReader = bufio.NewReaderSize(r.File, 4096)
	r.pos = 0
	return
}

func (r *FileReaders) Read(lens uint) []byte {
	var data []byte = make([]byte, lens)
	_, err := io.ReadFull(r.BufReader, data[:])
	//io.ReadSeeker.Seek()
	if err != nil {
		if err == io.EOF {
			logx.InfoF("[%s] read finish.", r.Filename)
			return nil
		}
		logx.Error(err)
		return nil
	}
	r.pos += lens
	return data
}

func (r *FileReaders) Close() error {
	r.BufReader.Reset(nil)
	return r.File.Close()
}

// 相对于当前位置偏移多少
func (r *FileReaders) Seek(offset uint) error {
	_, err := r.BufReader.Discard(int(offset))
	r.pos += offset
	return err
}

func (r *FileReaders) CurrPos() int64 {
	//ret, err := r.File.Seek(0, io.SeekCurrent)
	//if err != nil {
	//	fmt.Println(err)
	//}
	return int64(r.pos)
}
