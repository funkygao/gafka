package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"runtime/debug"
	"strings"

	"github.com/funkygao/etclib"
	"github.com/funkygao/gafka"
	"github.com/funkygao/gafka/ctx"
	zkr "github.com/funkygao/gafka/registry/zk"
)

var (
	options struct {
		zone              string
		configFile        string
		tplFile           string
		haproxyConfigFile string
		haproxyBin        string
		showVersion       bool
	}
)

func parseFlags() {
	flag.StringVar(&options.zone, "z", "prod", "zone")
	flag.BoolVar(&options.showVersion, "version", false, "display version and exit")
	flag.StringVar(&options.haproxyBin, "haproxy", "/opt/app/haproxy/haproxy", "haproxy binary path")
	flag.StringVar(&options.configFile, "c", "/etc/kateway.cf", "config file path")
	flag.StringVar(&options.tplFile, "t", "etc/haproxy.tpl", "haproxy config template file")
	flag.StringVar(&options.haproxyConfigFile, "haproxycf", ".haproxy.cf", "haproxy config file")

	flag.Parse()
}

func main() {
	defer func() {
		if err := recover(); err != nil {
			fmt.Println(err)
			debug.PrintStack()
		}
	}()

	parseFlags()
	if options.showVersion {
		fmt.Fprintf(os.Stderr, "%s-%s\n", gafka.Version, gafka.BuildId)
		os.Exit(0)
	}

	ctx.LoadConfig(options.configFile)

	if err := etclib.Dial(strings.Split(ctx.ZoneZkAddrs(options.zone), ",")); err != nil {
		panic(err)
	}

	ch := make(chan []string, 10)
	go etclib.WatchChildren(zkr.Root(options.zone), ch)

	var err error
	for {
		select {
		case <-ch:
			log.Println("kateway cluster changed")

			err = createConfigFile(nil, options.tplFile, options.haproxyConfigFile)
			if err != nil {
				log.Println(err)
				continue
			}

			err = reloadHAproxy(options.haproxyBin, options.haproxyConfigFile)
			if err != nil {
				log.Println(err)
			}
		}
	}

}
