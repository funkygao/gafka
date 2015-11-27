package main

import (
	"fmt"
	"io"
	"net/http"
	"syscall"
)

func hello(rw http.ResponseWriter, req *http.Request) {
	io.WriteString(rw, "hello world")
}

func main() {
	syscall.Dup2(1, 2)

	http.HandleFunc("/", hello)
	fmt.Println("listening on :9090")
	if err := http.ListenAndServe(":9090", nil); err != nil {
		panic(err)
	}
}
