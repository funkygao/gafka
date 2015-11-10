package zk

import (
	"strconv"
	"time"
)

func TimestampToTime(ts string) time.Time {
	sec, _ := strconv.ParseInt(ts, 10, 64)
	if sec > 143761237100 {
		sec /= 1000
	}

	return time.Unix(sec, 0)
}
