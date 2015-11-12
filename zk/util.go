package zk

import (
	"strconv"
	"time"
)

var PanicHandler func(interface{})

func TimestampToTime(ts string) time.Time {
	sec, _ := strconv.ParseInt(ts, 10, 64)
	if sec > 143761237100 {
		sec /= 1000
	}

	return time.Unix(sec, 0)
}

func withRecover(fn func()) {
	defer func() {
		handler := PanicHandler
		if handler != nil {
			if err := recover(); err != nil {
				handler(err)
			}
		}
	}()

	fn()
}
