package main

import (
	"flag"
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"sync"
)

func init() {
	flag.StringVar(&options.topic, "t", "30.gitlab_events.v1", "event topic")
	flag.BoolVar(&options.debug, "d", false, "debug")
	flag.BoolVar(&options.mock, "m", false, "mock mode")
	flag.StringVar(&options.project, "p", "", "display only a single project events")
	flag.BoolVar(&options.webhookOnly, "web", false, "webhook only")
	flag.BoolVar(&options.syshookOnly, "sys", false, "system hook only")
	flag.BoolVar(&options.noUI, "plain", false, "without UI mode")
	flag.StringVar(&options.logfile, "l", "", "log file")
	flag.Parse()

	if options.project != "" {
		options.webhookOnly = true
		options.syshookOnly = false
	}
}

var (
	lock    sync.Mutex
	loadedN int
	errCh   chan error
	newEvt  chan interface{}
	events  []interface{}
	quit    chan struct{}
	ready   chan struct{}
)

func main() {
	quit = make(chan struct{})
	newEvt = make(chan interface{}, 10)
	errCh = make(chan error)
	ready = make(chan struct{})
	log.SetFlags(log.Lshortfile | log.LstdFlags)
	if options.logfile == "" {
		if !options.noUI {
			log.SetOutput(ioutil.Discard)
		}
	} else {
		f, err := os.OpenFile(options.logfile, os.O_WRONLY|os.O_CREATE|os.O_APPEND, 0666)
		if err != nil {
			panic(err)
		}
		log.SetOutput(f)
	}

	if options.mock {
		go mockEvents()
	} else {
		go subLoop()
	}

	if options.debug {
		<-ready
		lock.Lock()
		for _, evt := range events {
			fmt.Printf("%#v\n\n", evt)
		}

		fmt.Printf("TOTAL: %d\n", len(events))
		lock.Unlock()

		return
	}

	if options.noUI {
		select {}
	} else {
		runUILoop()
	}

}
