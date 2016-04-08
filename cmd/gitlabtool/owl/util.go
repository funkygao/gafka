package main

import (
	"time"

	"github.com/funkygao/golib/gofmt"
)

func since(timestamp string) string {
	t, _ := time.Parse(time.RFC3339, timestamp)
	return gofmt.PrettySince(t)
}
