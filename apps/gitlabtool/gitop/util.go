package main

import (
	"time"

	"github.com/funkygao/golib/bjtime"
	"github.com/mattn/go-runewidth"
)

func since(timestamp string) string {
	t, _ := time.Parse(time.RFC3339, timestamp)
	return bjtime.TimeToString(t)
}

func reorderEvents() []interface{} {
	evts := make([]interface{}, 0, len(events))
	for i := len(events) - 1; i >= 0; i-- {
		evts = append(evts, events[i])
	}
	return evts
}

func wideStr(str string, width int) string {
	var size int
	for _, r := range str {
		w := runewidth.RuneWidth(r)
		if w == 0 || (w == 2 && runewidth.IsAmbiguousWidth(r)) {
			w = 1
		}
		size += w
	}

	r := ""
	for i := 0; i < width-size; i++ {
		r += " "
	}
	r += str

	return r
}
