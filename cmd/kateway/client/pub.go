package main

import (
	"flag"
	"fmt"

	"github.com/funkygao/gafka/cmd/kateway/api"
)

var (
	addr   string
	msgKey string
	n      int
)

func init() {
	flag.StringVar(&addr, "addr", "http://localhost:9191", "pub kateway addr")
	flag.IntVar(&n, "n", 50, "run pub how many times")
	flag.StringVar(&msgKey, "key", "", "pub message key")

	flag.Parse()

}

func main() {
	c := api.NewClient("demo", nil)
	c.Connect(addr)
	for i := 0; i < n; i++ {
		err := c.Publish("foobar", "v1", "", []byte("hello world"))
		fmt.Println(err)
	}
}
