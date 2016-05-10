package main

import (
	"flag"
	"fmt"
	"log"
	"strconv"
	"time"

	"github.com/funkygao/gafka/cmd/kateway/api/v1"
	"github.com/funkygao/gafka/ctx"
)

var (
	addr  string
	n     int
	appid string
	group string
	topic string
	step  int
	sleep time.Duration
)

func init() {
	ip, _ := ctx.LocalIP()
	flag.StringVar(&addr, "addr", fmt.Sprintf("%s:9192", ip), "sub kateway addr")
	flag.StringVar(&group, "g", "mygroup1", "consumer group name")
	flag.StringVar(&appid, "appid", "app1", "consume whose topic")
	flag.IntVar(&step, "step", 1, "display progress step")
	flag.StringVar(&topic, "t", "foobar", "topic to sub")
	flag.DurationVar(&sleep, "sleep", 0, "sleep between pub")
	flag.IntVar(&n, "n", 1000000, "run sub how many times")
	flag.Parse()
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
	err = c.SubX(opt, func(statusCode int, msg []byte, r *api.SubXResult) error {
		log.Printf("i=%d, status:%d, r:%+v msg:%s tag:%s", i, statusCode, *r, string(msg),
			r.Tag)

		offset, _ := strconv.Atoi(r.Offset)
		if false && i < 3 {
			i++
			r.Partition = "-1"
			r.Offset = "-1"
			return nil
		} else {
			r.Offset = fmt.Sprintf("%d", offset+10)
			log.Println("try error: commit too large offset")
		}

		if i == 4 {
			r.Offset = fmt.Sprintf("%d", offset-10)
			log.Println("try error: commit too small offset")
		}

		i++
		if i > 2 {
			return api.ErrSubStop
		}

		if false {
			time.Sleep(time.Second * 2)
		}

		return nil
	})

	if err != nil {
		log.Println(err)
	}

	elapsed := time.Since(t0)
	log.Printf("%d msgs in %s, tps: %.2f\n", n, elapsed, float64(n)/elapsed.Seconds())
}
