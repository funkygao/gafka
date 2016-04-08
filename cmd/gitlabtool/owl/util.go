package main

import (
	"time"

	"github.com/funkygao/golib/bjtime"
)

func since(timestamp string) string {
	t, _ := time.Parse(time.RFC3339, timestamp)
	return bjtime.TimeToString(t)
}
