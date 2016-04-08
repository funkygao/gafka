package main

import (
	"flag"
)

func init() {
	flag.StringVar(&options.stream, "s", "webhook", "usage")
	flag.StringVar(&options.topic, "t", "30.gitlab_events.v1", "topic")
	flag.Parse()
}

var (
	newEvt chan struct{}
	events []interface{}
)

func main() {
	quit := make(chan struct{})
	newEvt = make(chan struct{}, 10)
	go subLoop(quit)

	runUILoop(quit)
}
