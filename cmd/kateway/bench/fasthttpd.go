package main

import (
	"flag"
	"fmt"
	"syscall"

	"github.com/valyala/fasthttp"
)

var (
	port int
)

func init() {
	flag.IntVar(&port, "p", 9090, "http port to bind")
	flag.Parse()
}

func hello(ctx *fasthttp.RequestCtx) {
	ctx.SetContentType("text/plain; charset=utf8")
	ctx.Write([]byte("hello world"))

}

func main() {
	syscall.Dup2(1, 2)

	listen := fmt.Sprintf(":%d", port)
	fmt.Printf("listening on %s\n", listen)

	if err := fasthttp.ListenAndServe(listen, hello); err != nil {
		fmt.Printf("Error in ListenAndServe: %s\n", err)
	}

}
