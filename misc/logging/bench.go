package main

import (
	"time"

	log "github.com/funkygao/log4go"
)

func init() {
	log.DeleteFilter("stdout")

	rotateEnabled, discardWhenDiskFull := true, true
	filer := log.NewFileLogWriter("bench.log", rotateEnabled, discardWhenDiskFull, 0644)
	filer.SetFormat("[%d %T] [%L] (%S) %M")
	filer.SetRotateLines(0)
	filer.SetRotateDaily(true)
	log.AddFilter("file", log.INFO, filer)
}

func main() {
	start := make(chan struct{})
	var n int64
	go func() {
		start <- struct{}{}
		for {
			log.Info("hello %s %d", "world!", n)
			n++
		}
	}()

	<-start
	time.Sleep(time.Second * 10)
	println(n)
	return
}
