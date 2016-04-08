package main

import (
	"github.com/nsf/termbox-go"
)

func runUILoop() {
	err := termbox.Init()
	if err != nil {
		panic(err)
	}
	defer termbox.Close()

	termbox.SetInputMode(termbox.InputEsc)

	drawSplash()
	<-ready
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
			case termbox.Key('b'):
				// page up

			case termbox.KeySpace:
				// page down

			case termbox.KeyEnter:
				// detail page

			case termbox.KeyArrowDown, termbox.Key('j'):
				selectedRow++
				redrawAll()

			case termbox.KeyArrowUp, termbox.Key('k'):
				if selectedRow > 0 {
					selectedRow--
					redrawAll()
				}

			case termbox.KeyEsc, termbox.Key('q'):
				close(quit)
				return

			}

		case termbox.EventError:
			panic(ev.Err)
		}
	}
}
