package main

import (
	"flag"
	"fmt"
	"log"
	"math/rand"
	"strings"
	"sync/atomic"

	"github.com/funkygao/gafka/cmd/kateway/api"
)

var (
	c       int
	addr    string
	n       int64
	msgSize int
)

func main() {
	flag.IntVar(&c, "c", 10, "client concurrency")
	flag.IntVar(&msgSize, "sz", 100, "msg size")
	flag.StringVar(&addr, "h", "http://localhost:9191", "pub http addr")
	flag.Parse()

	for i := 0; i < c; i++ {
		go pubGatewayLoop(i)
	}

	select {}
}

func pubGatewayLoop(seq int) {
	cf := api.DefaultConfig()
	client := api.NewClient("app1", cf)
	client.Connect(addr)

	var (
		err error
		msg string
		no  int64
		sz  int
	)

	for {
		sz = msgSize + rand.Intn(msgSize)
		no = atomic.AddInt64(&n, 1)
		if no%5000 == 0 {
			log.Println(no)
		}

		msg = fmt.Sprintf("%2d %10d %s", seq, no, strings.Repeat("X", sz))
		err = client.Publish("foobar", "v1", "", []byte(msg))
		if err != nil {
			fmt.Println(err)
		}
	}

}
