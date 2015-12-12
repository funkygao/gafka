package main

import (
	"flag"
	"fmt"
	"log"
	"time"

	"github.com/funkygao/gafka/cmd/kateway/api"
)

var (
	addr  string
	n     int
	appid string
	group string
)

func init() {
	flag.StringVar(&addr, "addr", "http://localhost:9192", "sub kateway addr")
	flag.StringVar(&group, "g", "mygroup1", "consumer group name")
	flag.StringVar(&appid, "appid", "app1", "consume whose topic")
	flag.IntVar(&n, "n", 1000000, "run sub how many times")
	flag.Parse()
}

func main() {
	c := api.NewClient("app2", nil)
	c.Connect(addr)
	i := 0
	t0 := time.Now()
	step := 3000
	err := c.Subscribe(appid, "foobar", "v1", group, func(statusCode int, msg []byte) error {
		i++
		if i >= n {
			return api.ErrSubStop
		}

		if i%step == 0 {
			log.Println(i)
		}

		return nil
	})
	if err != nil {
		fmt.Println(err)
	}

	elapsed := time.Since(t0)
	fmt.Printf("%d msgs in %s, tps: %.2f\n", n, elapsed, float64(n)/elapsed.Seconds())
}
