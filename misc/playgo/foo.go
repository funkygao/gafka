package main

import "fmt"

var bar int

func main() {
	for i := 0; i < 5; i++ {
		fmt.Println(strcat("he", "ll", "o"))
	}

}

func strcat(a, b, c string) string {
	return a + b + c
}
