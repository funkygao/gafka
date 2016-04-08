package main

import (
	"flag"
	"fmt"
	"sync"
	"time"
)

func init() {
	flag.StringVar(&options.stream, "s", "webhook", "usage")
	flag.StringVar(&options.topic, "t", "30.gitlab_events.v1", "topic")
	flag.BoolVar(&options.debug, "d", false, "debug")
	flag.BoolVar(&options.mock, "m", false, "mock mode")
	flag.Parse()
}

var (
	lock   sync.Mutex
	newEvt chan struct{}
	events []interface{}
	quit   chan struct{}
	ready  chan struct{}
)

func main() {
	quit = make(chan struct{})
	newEvt = make(chan struct{}, 10)
	ready = make(chan struct{})
	if options.mock {
		go mockEvents()
	} else {
		go subLoop()
	}

	if options.debug {
		time.Sleep(time.Second * 5)
		lock.Lock()
		for _, evt := range events {
			fmt.Printf("%#v\n\n", evt)
		}

		fmt.Printf("TOTAL: %d\n", len(events))
		lock.Unlock()

		return
	}

	runUILoop()
}
