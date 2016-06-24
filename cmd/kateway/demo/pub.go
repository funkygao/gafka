package main

import (
	"flag"
	"fmt"
	"time"

	"github.com/funkygao/gafka/cmd/kateway/api/v1"
)

var (
	addr   string
	msgKey string
	n      int
	timeit bool
	sleep  time.Duration
)

func init() {
	flag.StringVar(&addr, "addr", "localhost:9191", "pub kateway addr")
	flag.IntVar(&n, "n", 50, "run pub how many times")
	flag.StringVar(&msgKey, "key", "", "pub message key")
	flag.DurationVar(&sleep, "sleep", time.Millisecond*100, "sleep between each pub")
	flag.BoolVar(&timeit, "time", false, "time each pub message cost")

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

	var t time.Time
	for i := 0; i < n; i++ {
		if timeit {
			t = time.Now()
		}
		err := c.Pub("", []byte(fmt.Sprintf("hello world[%d]!", i+1)), opt)
		if timeit {
			fmt.Println(i, time.Since(t), err)
		} else if err != nil {
			fmt.Println(i, err)
		}

		if sleep > 0 {
			time.Sleep(sleep)
		}
	}
}
