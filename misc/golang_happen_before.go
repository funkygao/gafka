package main

import (
	"time"
)

var a, b int

func demoHappenBefore() {
	a = 0
	b = 0
	go func() {
		// the 2 instructions might be in disordered by compiler
		a = 1
		b = 2
	}()
	time.Sleep(time.Microsecond)
	println(a, b) // might output: (0 2), (0, 0), (1, 2)
}

func main() {
	for i := 0; i < 200; i++ {
		demoHappenBefore()
	}
}
