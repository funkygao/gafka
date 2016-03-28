package main

import (
	"flag"
	"fmt"
	"log"
	"math/rand"
	"time"

	"github.com/funkygao/gafka/cmd/kateway/api"
	"github.com/funkygao/gafka/ctx"
	"github.com/funkygao/golib/color"
	rd "github.com/funkygao/golib/rand"
)

var (
	addr  string
	n     int
	appid string
	group string
	topic string
	mode  string
	step  int
	sleep time.Duration
)

func init() {
	ip, _ := ctx.LocalIP()
	flag.StringVar(&addr, "addr", fmt.Sprintf("%s:9192", ip), "sub kateway addr")
	flag.StringVar(&group, "g", "mygroup1", "consumer group name")
	flag.StringVar(&appid, "appid", "app1", "consume whose topic")
	flag.IntVar(&step, "step", 1, "display progress step")
	flag.StringVar(&mode, "mode", "subx", "sub mode")
	flag.StringVar(&topic, "t", "foobar", "topic to sub")
	flag.DurationVar(&sleep, "sleep", 0, "sleep between pub")
	flag.IntVar(&n, "n", 1000000, "run sub how many times")
	flag.Parse()

	rd.RandSeedWithTime()
}

func main() {
	cf := api.DefaultConfig("app2", "mysecret")
	cf.Debug = true
	cf.Sub.Endpoint = addr
	c := api.NewClient(cf)
	i := 0
	t0 := time.Now()
	var err error
	opt := api.SubOption{
		AppId: appid,
		Topic: topic,
		Ver:   "v1",
		Group: group,
	}
	if mode == "subx" {
		err = c.SubX(opt, func(statusCode int, msg []byte,
			r *api.SubXResult) error {
			i++
			if n > 0 && i >= n {
				return api.ErrSubStop
			}

			if i%step == 0 {
				log.Println(statusCode, string(msg))
			}

			if sleep > 0 {
				time.Sleep(sleep)
			}

			// handle the msg here
			if rand.Int()%2 == 0 {
				// simulate handle this msg successfully
				log.Println(color.Green("ok"))
			} else {
				// this msg was not successfully handled
				if rand.Int()%2 == 0 {
					// after retry several times, give up
					r.Bury = api.ShadowRetry
					log.Println(color.Red("shadow"))
				} else {
					// simulate handle msg successfully after retry
					if sleep > 0 {
						time.Sleep(sleep)
					}
					log.Println(color.Yellow("retried"))
				}
			}

			log.Println()

			return nil
		})
	} else {
		err = c.Sub(opt, func(statusCode int, msg []byte) error {
			i++
			if n > 0 && i >= n {
				return api.ErrSubStop
			}

			if i%step == 0 {
				log.Println(statusCode, string(msg))
			}

			if sleep > 0 {
				time.Sleep(sleep)
			}

			return nil
		})
	}

	if err != nil {
		log.Println(err)
	}

	elapsed := time.Since(t0)
	log.Printf("%d msgs in %s, tps: %.2f\n", n, elapsed, float64(n)/elapsed.Seconds())
}
