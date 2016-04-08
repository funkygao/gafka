package main

import (
	"flag"
)

func init() {
	flag.StringVar(&options.stream, "s", "webhook", "usage")
	flag.Parse()
}

var (
	events []interface{}
)

func main() {
	quit := make(chan struct{})
	go subLoop(quit)

	runUILoop(quit)
}
