package binlog_header

import (
	"gomysqlbinlog/utils/logx"
)

type Events struct {
	EvBdata  []byte
	EvName   string
	EvHeader EventHeader
	CheckSum int
	StartPos int
	StopPos  int
}

func (Ev *Events) PrintEvHeaderInfo(infoEtc ...string) {
	logx.OutputMark("# at %d\n", Ev.StartPos)
	logx.OutputMark("#%s server id %d  end_log_pos %d CRC32 %x      %s %s\n",
		Ev.EvHeader.Event_data.TimestampStr,
		Ev.EvHeader.Event_data.Server_id,
		Ev.StopPos,
		Ev.CheckSum,
		Ev.EvName,
		func() string {
			x := ""
			for _, v := range infoEtc {
				x += v
			}
			return x
		}(),
	)
}
