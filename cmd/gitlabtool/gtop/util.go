package main

import (
	"time"
	"unicode/utf8"

	"github.com/funkygao/golib/bjtime"
)

func since(timestamp string) string {
	t, _ := time.Parse(time.RFC3339, timestamp)
	return bjtime.TimeToString(t)
}

func wideStr(str string, width int) string {
	var size int
	for _, s := range str {
		_, l := utf8.DecodeRuneInString(string(s))
		size += l
	}

	r := ""
	for i := 0; i < width-size; i++ {
		r += " "
	}
	r += str

	return r
}
