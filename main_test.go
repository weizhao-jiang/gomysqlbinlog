package main

import (
	"encoding/base64"
	"fmt"
	"gomysqlbinlog/events"
	"gomysqlbinlog/options_handler"
	"gomysqlbinlog/utils/logx"
	"testing"
)

func TestMain(t *testing.T) {
	// -- UPDATE `MINI_PROGRAM1`.`test1` SET `@1`=8 and `@2`='77777' WHERE `@1`=8, `@2`='替换🤔' /* UPDATE_ROWS_EVENT */ /*!*/;
	options := options_handler.InitOptions()
	options.ToSQL = true
	logx.SetLevel(logx.DEBUG_LEVEL)
	ep := events.EventParser{Options: options, FileBPos: 0, FileEPos: 99999}
	TrxListBdata := [][]byte{}

	res, err := base64.StdEncoding.DecodeString("gr8LZhNnFAAASgAAAEsIAAAAAHEAAAAAAAEAE01FSVpVX01JTklfUFJPR1JBTTEABXRlc3QxAAIDDwL8AwIBAQACA/z/AL/g094=")
	if err != nil {
		fmt.Println(err)
	}
	TrxListBdata = append(TrxListBdata, res)

	//res1, err1 := base64.StdEncoding.DecodeString("gr8LZh9nFAAAQQAAAIwIAAAAAHEAAAAAAAEAAgAC//8ACAAAAAUANzc3NzcACAAAAAoA5pu/5o2i8J+klIrfqic=")
	//原版
	//res1, err1 := base64.StdEncoding.DecodeString("gr8LZh9nFAAAQQAAAIwIAAAAAHEAAAAAAAEAAgAC//8ACAAAAAUANzc3NzcACAAAAAoA5pu/5o2i8J+klIrfqic=")
	//回滚版
	res1, err1 := base64.StdEncoding.DecodeString("gr8LZh9nFAAAQQAAAIwIAAAAAHEAAAAAAAEAAgAC//8ACAAAAAoA5pu/5o2i8J+klAAIAAAABQA3Nzc3N3CIbqc=")

	if err1 != nil {
		fmt.Println(err1)
	}
	TrxListBdata = append(TrxListBdata, res1)

	for idx, ev := range TrxListBdata {
		if e := ep.Init(&ev, true); e != nil {
			logx.Error(e)
		}
		nextEvType := -1
		if idx+1 <= len(TrxListBdata)-1 {
			if len(TrxListBdata[idx+1]) >= 4 {
				nextEvType = int(ep.Read_uint_try(TrxListBdata[idx+1][4:5], "little"))
			}
		}
		err := ep.Parse(nextEvType)
		if err != nil {
			logx.Error(err)
		}

	}
}
