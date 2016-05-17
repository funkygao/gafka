// Pub/Sub benchmark
package main

import (
	"flag"
	"fmt"
	"io/ioutil"
	"log"

	"github.com/funkygao/gafka/cmd/kateway/api/v1"
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
	msgfile  string
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
		fmt.Println("Pub: go run pubsub.go -mode pub -appid 73 -key xxxx -msgfile msg -ep pub.sit.ffan.com:9191 -topic risk_beacon_test")
		fmt.Println("Sub: ")

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

}
