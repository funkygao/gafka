package main

import (
	tm "github.com/buger/goterm"
)

func demoTerm() {
	tm.Clear()
	tm.MoveCursor(1, 1)
	for i := 0; i < 10; i++ {
		tm.Println("hello world, haha")
	}
	tm.Println(tm.HighlightRegion("str", 1, 3, tm.GREEN))
	tm.Flush()
}

func runUILoop(quit chan struct{}) {

}
