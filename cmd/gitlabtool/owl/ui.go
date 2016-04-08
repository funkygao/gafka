package main

import (
	"os"

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
			switch ev.Ch {
			case 'j':
				selectedRow++
				redrawAll()

			case 'k':
				if selectedRow > 0 {
					selectedRow--
					redrawAll()
				}

			case 'd':
				// detail page
				if detailView {
					redrawAll()
				} else {
					drawDetail()
				}
				detailView = !detailView

			case 'f':
				// page down

			case 'b':
				// page up

			case 'q':
				if detailView {
					detailView = false
					redrawAll()
				} else {
					termbox.Close()
					os.Exit(0)
				}

			}

		case termbox.EventError:
			panic(ev.Err)

		}
	}
}
