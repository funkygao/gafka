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
	"github.com/funkygao/gafka/ctx"
)

var (
	c        int
	addr     string
	n        int64
	msgSize  int
	step     int64
	appid    string
	topic    string
	key      string
	workerId string
	sleep    time.Duration
)

func main() {
	flag.IntVar(&c, "c", 10, "client concurrency")
	flag.IntVar(&msgSize, "sz", 100, "msg size")
	flag.StringVar(&appid, "appid", "app1", "app id")
	flag.DurationVar(&sleep, "sleep", 0, "sleep between pub")
	ip, _ := ctx.LocalIP()
	flag.StringVar(&addr, "h", fmt.Sprintf("http://%s:9191", ip.String()), "pub http addr")
	flag.Int64Var(&step, "step", 1, "display progress step")
	flag.StringVar(&key, "key", "", "message key")
	flag.StringVar(&topic, "t", "foobar", "topic to pub")
	flag.StringVar(&workerId, "id", "1", "worker id")
	flag.Parse()

	for i := 0; i < c; i++ {
		go pubGatewayLoop(i)
	}

	select {}
}

func pubGatewayLoop(seq int) {
	cf := api.DefaultConfig()
	cf.AppId = appid
	cf.PubEndpoint = addr
	cf.Debug = true
	client := api.NewClient(cf)

	var (
		err error
		msg string
		no  int64
		sz  int
	)

	for {
		sz = msgSize + rand.Intn(msgSize)
		no = atomic.AddInt64(&n, 1)

		msg = fmt.Sprintf("%s w:%s seq:%-2d no:%-10d payload:%s",
			time.Now(),
			workerId, seq, no, strings.Repeat("X", sz))
		err = client.Pub(topic, "v1", key, []byte(msg))
		if err != nil {
			fmt.Println(err)
			no = atomic.AddInt64(&n, -1)
		} else {
			if no%step == 0 {
				log.Println(msg)
			}
		}

		if sleep > 0 {
			time.Sleep(sleep)
		}
	}

}
