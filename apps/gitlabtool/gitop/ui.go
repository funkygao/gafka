package main

import (
	"fmt"
	"os"
	"time"

	"github.com/nsf/termbox-go"
	"github.com/pkg/browser"
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
		case evt := <-newEvt:
			drawNotify(eventContent(evt))
			time.Sleep(time.Second)
			detailView = false
			dashboardView = false
			projectSummaryView = false
			userSummaryView = false
			redrawAll()

		case err := <-errCh:
			termbox.Close()
			fmt.Println(err)
			return

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
			case termbox.KeyArrowUp:

			case termbox.KeyArrowDown:

			case termbox.KeySpace:
				// page down
				lock.Lock()
				totalN := len(events)
				lock.Unlock()
				if selectedRow >= totalN-1 {
					// the last event
					continue
				}

				if !detailView {
					if selectedRow/pageSize == totalN/pageSize {
						// already the last page, do nothing
						continue
					}

					selectedRow += pageSize
					if selectedRow > totalN-1 {
						// can't be beyond the range
						selectedRow = totalN - 1
					}
					page++

					redrawAll()
				}
				continue

			case termbox.KeyEnter:
				if detailView {
					browser.OpenURL(currentWebHook.Commits[len(currentWebHook.Commits)-selectedCommit-1].Url)
				}
				continue

			case termbox.KeyCtrlD:
				// dashboard page
				if dashboardView {
					redrawAll()
				} else {
					drawDashboardByHour()
				}
				dashboardView = !dashboardView

			case termbox.KeyCtrlP:
				if projectSummaryView {
					redrawAll()
				} else {
					drawDashboardByProject()
				}
				projectSummaryView = !projectSummaryView

			case termbox.KeyCtrlU:
				if userSummaryView {
					redrawAll()
				} else {
					drawDashboardByUser()
				}
				userSummaryView = !userSummaryView

			case termbox.KeyEsc:
				if detailView {
					detailView = false
					redrawAll()
				} else {
					termbox.Close()
					os.Exit(0)
				}
			}

			switch ev.Ch {
			case 'j':
				if detailView {
					if selectedCommit < len(currentWebHook.Commits)-1 {
						selectedCommit++
					} else {
						selectedCommit = 0 // rewind
					}
					drawDetail()
				} else {
					lock.Lock()
					totalN := len(events)
					lock.Unlock()

					if selectedRow < totalN-1 {
						selectedRow++
						if selectedRow%pageSize == 0 {
							page++
						}
						redrawAll()
					}
				}

			case 'K', 'T':
				if mainView {
					page = 0
					selectedRow = 0
					redrawAll()
				}

			case 'G':
				if mainView {
					lock.Lock()
					totalN := len(events)
					lock.Unlock()

					page = totalN / pageSize
					if totalN%pageSize == 0 {
						page--
					}

					selectedRow = totalN - 1
					redrawAll()
				}

			case 'M':
				if !detailView {
					lock.Lock()
					totalN := len(events)
					lock.Unlock()

					if selectedRow/pageSize == totalN/pageSize {
						// already the last page, do nothing
						continue
					}

					selectedRow = page*pageSize + pageSize/2
					redrawAll()
				}

			case 'H':
				if !detailView {
					selectedRow = page * pageSize
					redrawAll()
				}

			case 'L':
				if !detailView {
					lock.Lock()
					totalN := len(events)
					lock.Unlock()

					if selectedRow/pageSize == totalN/pageSize {
						// already the last page, do nothing
						continue
					}

					selectedRow = page*pageSize + pageSize - 1
					redrawAll()
				}

			case 'k':
				if detailView {
					if selectedCommit > 0 {
						selectedCommit--
					} else {
						selectedCommit = len(currentWebHook.Commits) - 1
					}
					drawDetail()
				} else {
					if selectedRow > 0 {
						if selectedRow%pageSize == 0 {
							page--
						}
						selectedRow--
						redrawAll()
					}
				}

			case 'd':
				// detail page
				if detailView {
					redrawAll()
				} else {
					drawDetail()
				}
				detailView = !detailView

			case 'b':
				// page up
				if !detailView {
					selectedRow -= pageSize
					if selectedRow < 0 {
						selectedRow = 0
					} else {
						page--
					}
					redrawAll()
				}

			case 'q':
				if detailView {
					detailView = false
					redrawAll()
				} else if dashboardView {
					dashboardView = false
					redrawAll()
				} else if projectSummaryView {
					projectSummaryView = false
					redrawAll()
				} else if userSummaryView {
					userSummaryView = false
					redrawAll()
				} else {
					termbox.Close()
					os.Exit(0)
				}
				selectedCommit = 0
			}

		case termbox.EventError:
			panic(ev.Err)

		}
	}
}
