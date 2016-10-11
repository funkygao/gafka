package main

import "fmt"

var bar int

func main() {
	for i := 0; i < 5; i++ {
		fmt.Println(bar)

		bar++
	}
}
