package main

import (
	//tm "github.com/buger/goterm"
	"github.com/nsf/termbox-go"
)

func runUILoop(quit chan struct{}) {
	err := termbox.Init()
	if err != nil {
		panic(err)
	}
	defer termbox.Close()

	termbox.SetInputMode(termbox.InputEsc)
	redrawAll()

	// capture and process events from the CLI
	eventChan := make(chan termbox.Event, 16)
	go handleEvents(eventChan)
	go func() {
		for {
			ev := termbox.PollEvent()
			eventChan <- ev
		}
	}()

	for {
		select {
		case <-newEvt:
			redrawAll()

		case <-quit:
			return
		}
	}

}

func handleEvents(eventChan chan termbox.Event) {
	for ev := range eventChan {
		switch ev.Type {
		case termbox.EventKey:
			switch ev.Key {
			case termbox.KeySpace:
			case termbox.KeyArrowDown:
			case termbox.KeyArrowUp:
			case termbox.KeyEsc:

			}

		case termbox.EventError:
			panic(ev.Err)
		}
	}
}
