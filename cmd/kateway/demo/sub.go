package main

import (
	"flag"
	"fmt"
	"time"

	"github.com/funkygao/gafka/cmd/kateway/api/v1"
)

var (
	addr   string
	n      int
	appid  string
	subkey string
	topic  string
	ver    string
	group  string
	sleep  time.Duration
)

func init() {
	flag.StringVar(&addr, "addr", "localhost:9192", "sub kateway addr")
	flag.StringVar(&group, "g", "mygroup1", "consumer group name")
	flag.StringVar(&appid, "appid", "", "consume whose topic")
	flag.StringVar(&subkey, "subkey", "", "sub key")
	flag.DurationVar(&sleep, "sleep", 0, "sleep between fetch")
	flag.StringVar(&topic, "topic", "", "topic")
	flag.StringVar(&ver, "ver", "v1", "topic version")
	flag.IntVar(&n, "n", 20, "run sub how many times")
	flag.Parse()
}

func main() {
	cf := api.DefaultConfig(appid, subkey)
	cf.Sub.Endpoint = addr
	c := api.NewClient(cf)
	opt := api.SubOption{
		AppId: appid,
		Topic: topic,
		Ver:   ver,
		Group: group,
	}

	i := 0
	err := c.Sub(opt, func(statusCode int, msg []byte) error {
		fmt.Printf("%10d: %s\n", i+1, string(msg))

		i++
		if i >= n {
			return api.ErrSubStop
		}

		if sleep > 0 {
			time.Sleep(sleep)
		}

		return nil
	})

	fmt.Println(err)
}
