package main

import (
	"bufio"
	"os"
	"time"

	gio "github.com/funkygao/golib/io"
)

const (
	mockFile = "webhook.json"
)

func mockEvents() {
	f, _ := os.Open(mockFile)

	reader := bufio.NewReader(f)
	for {
		l, e := gio.ReadLine(reader)
		if e != nil {
			// EOF
			break
		}

		events = append(events, decode(l))
	}

	time.Sleep(time.Second)
	close(ready)
}
