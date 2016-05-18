// Pub/Sub benchmark
package main

import (
	"flag"
	"fmt"
	"io/ioutil"
	"log"
	"time"

	"github.com/funkygao/gafka/cmd/kateway/api/v1"
	"github.com/funkygao/gafka/cmd/kateway/gateway"
	"github.com/funkygao/golib/stress"
)

var (
	mode     string
	appId    string
	secret   string
	ver      string
	topic    string
	limit    int
	debug    bool
	endpoint string
	batch    int
	group    string
	msgfile  string
	subAppid string
	sleep    time.Duration
)

func init() {
	flag.StringVar(&mode, "mode", "pub", "mode: <pub|sub|help>")
	flag.StringVar(&appId, "appid", "", "app id")
	flag.StringVar(&secret, "key", "", "app secret")
	flag.StringVar(&ver, "ver", "v1", "version")
	flag.IntVar(&limit, "limit", 100000, "limit msg count")
	flag.StringVar(&topic, "topic", "", "topic name")
	flag.StringVar(&endpoint, "ep", "pub.sit.ffan.com:9191", "end point")
	flag.StringVar(&msgfile, "msgfile", "", "message file to Pub")
	flag.StringVar(&group, "group", "bench_go", "sub group name")
	flag.DurationVar(&sleep, "sleep", 0, "sleep between loops")
	flag.IntVar(&batch, "batch", 1, "sub batch limit")
	flag.StringVar(&subAppid, "subappid", "", "sub which app's msg")
	flag.BoolVar(&debug, "debug", false, "debug")

	flag.Parse()
}

func main() {
	switch mode {
	case "pub":
		stress.RunStress(benchmarkPub)

	case "sub":
		stress.RunStress(benchmarkSub)

	case "help":
		fmt.Println("Pub: go run pubsub.go -c1 10 -step 5 -mode pub -appid 73 -key xxxx -msgfile msg -ep pub.sit.ffan.com:9191 -topic risk_beacon_test")
		fmt.Println("Sub: go run pubsub.go -c1 1 -c2 1 -mode sub -appid app2 -subappid app1 -key xxx -ep sub.sit.ffan.com:9192 -topic risk_beacon_test -group bench_go -debug")

	}

}

func benchmarkPub(seq int) {
	cf := api.DefaultConfig(appId, secret)
	cf.Pub.Endpoint = endpoint
	cf.Debug = debug
	client := api.NewClient(cf)
	var opt api.PubOption
	opt.Topic = topic
	opt.Ver = ver
	opt.Async = true

	msg, err := ioutil.ReadFile(msgfile)
	if err != nil {
		panic(err)
	}

	for i := 0; i < limit; i++ {
		err := client.Pub("", msg, opt)
		if err != nil {
			stress.IncCounter("fail", 1)
			log.Println(err)
		} else {
			stress.IncCounter("ok", 1)
		}

	}
}

func benchmarkSub(seq int) {
	cf := api.DefaultConfig(appId, secret)
	cf.Debug = false
	cf.Sub.Endpoint = endpoint
	client := api.NewClient(cf)

	opt := api.SubOption{
		AppId: subAppid,
		Topic: topic,
		Ver:   ver,
		Batch: batch,
		Group: group,
	}
	var i int

	err := client.SubX(opt, func(statusCode int, msg []byte, r *api.SubXResult) error {
		if debug {
			if batch > 1 {
				msgs := gateway.DecodeMessageSet(msg)
				for idx, m := range msgs {
					log.Printf("%d P:%d O:%d V:%s", idx, m.Partition, m.Offset, string(m.Value))
				}
			} else {
				log.Println(string(msg))
			}
		}
		if statusCode == 200 {
			stress.IncCounter("ok", int64(batch))
		} else {
			if debug {
				log.Println(string(msg))
			}

			stress.IncCounter("fail", 1)
		}

		i++
		if i > limit {
			return api.ErrSubStop
		}

		if sleep > 0 {
			time.Sleep(sleep)
		}

		return nil
	})

	if err != nil {
		fmt.Println(err)
	}
}
