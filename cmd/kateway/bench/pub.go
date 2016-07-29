package main

import (
	"flag"
	"fmt"
	"log"
	"math/rand"
	"strings"
	"sync/atomic"
	"time"

	"github.com/funkygao/gafka/cmd/kateway/api/v1"
	"github.com/funkygao/gafka/ctx"
)

var (
	c        int
	addr     string
	n        int64
	msgSize  int
	debug    bool
	step     int64
	appid    string
	topic    string
	key      string
	workerId string
	sleep    time.Duration
	tag      string
)

func init() {
	ip, _ := ctx.LocalIP()
	flag.IntVar(&c, "c", 10, "client concurrency")
	flag.IntVar(&msgSize, "sz", 100, "msg size")
	flag.StringVar(&appid, "appid", "app1", "app id")
	flag.DurationVar(&sleep, "sleep", 0, "sleep between pub")
	flag.StringVar(&addr, "h", fmt.Sprintf("%s:9191", ip.String()), "pub http addr")
	flag.Int64Var(&step, "step", 1, "display progress step")
	flag.StringVar(&key, "key", "", "message key")
	flag.BoolVar(&debug, "debug", false, "debug")
	flag.StringVar(&tag, "tag", "", "add tag to each message")
	flag.StringVar(&topic, "t", "foobar", "topic to pub")
	flag.StringVar(&workerId, "id", "1", "worker id")
	flag.Parse()
}

func main() {
	log.SetFlags(log.LstdFlags | log.Lshortfile)

	for i := 0; i < c; i++ {
		go pubGatewayLoop(i)
	}

	select {}
}

func pubGatewayLoop(seq int) {
	cf := api.DefaultConfig(appid, "mysecret")
	cf.Pub.Endpoint = addr
	cf.Debug = debug
	client := api.NewClient(cf)

	var (
		err error
		msg string
		no  int64
		sz  int
	)

	var opt api.PubOption
	opt.Topic = topic
	opt.Ver = "v1"
	for {
		sz = msgSize + rand.Intn(msgSize)
		no = atomic.AddInt64(&n, 1)

		msg = fmt.Sprintf("%s w:%s seq:%-2d no:%-10d payload:%s",
			time.Now(),
			workerId, seq, no, strings.Repeat("X", sz))
		if tag != "" {
			opt.Tag = tag
		}

		err = client.Pub(key, []byte(msg), opt)
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
