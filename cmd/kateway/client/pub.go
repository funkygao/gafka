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
	flag.StringVar(&addr, "addr", "localhost:9191", "pub kateway addr")
	flag.IntVar(&n, "n", 50, "run pub how many times")
	flag.StringVar(&msgKey, "key", "", "pub message key")

	flag.Parse()

}

func main() {
	cf := api.DefaultConfig("app1", "mykey")
	cf.Pub.Endpoint = addr
	c := api.NewClient(cf)
	opt := api.PubOption{
		Topic: "foobar",
		Ver:   "v1",
	}
	for i := 0; i < n; i++ {
		err := c.Pub("", []byte("hello world"), opt)
		fmt.Println(err)
	}
}
