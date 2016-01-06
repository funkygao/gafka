package main

import (
	"flag"
	"fmt"
	"log"
	"math/rand"
	"strings"
	"sync/atomic"
	"time"

	"github.com/funkygao/gafka/cmd/kateway/api"
)

var (
	c       int
	addr    string
	n       int64
	msgSize int
	step    int64
	appid   string
	topic   string
	sleep   time.Duration
)

func main() {
	flag.IntVar(&c, "c", 10, "client concurrency")
	flag.IntVar(&msgSize, "sz", 100, "msg size")
	flag.StringVar(&appid, "appid", "app1", "app id")
	flag.DurationVar(&sleep, "sleep", 0, "sleep between pub")
	flag.StringVar(&addr, "h", "http://localhost:9191", "pub http addr")
	flag.Int64Var(&step, "step", 1, "display progress step")
	flag.StringVar(&topic, "t", "foobar", "topic to pub")
	flag.Parse()

	for i := 0; i < c; i++ {
		go pubGatewayLoop(i)
	}

	select {}
}

func pubGatewayLoop(seq int) {
	cf := api.DefaultConfig()
	client := api.NewClient(appid, cf)
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

		msg = fmt.Sprintf("seq:%-2d no:%-10d payload:%s", seq, no, strings.Repeat("X", sz))
		err = client.Publish(topic, "v1", "", []byte(msg))
		if err != nil {
			fmt.Println(err)
		}

		if no%step == 0 {
			log.Println(msg)
		}

		if sleep > 0 {
			time.Sleep(sleep)
		}
	}

}
