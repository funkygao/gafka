package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"runtime/debug"
	"strings"

	"github.com/funkygao/etclib"
	"github.com/funkygao/gafka"
	"github.com/funkygao/gafka/ctx"
	zkr "github.com/funkygao/gafka/registry/zk"
	log "github.com/funkygao/log4go"
)

var (
	options struct {
		zone              string
		configFile        string
		guide             bool
		tplFile           string
		haproxyConfigFile string
		haproxyBin        string
		showVersion       bool
	}
)

func parseFlags() {
	flag.StringVar(&options.zone, "z", "prod", "zone")
	flag.BoolVar(&options.guide, "guide", false, "guide")
	flag.BoolVar(&options.showVersion, "version", false, "display version and exit")
	flag.StringVar(&options.haproxyBin, "haproxy", "/opt/app/haproxy/haproxy", "haproxy binary path")
	flag.StringVar(&options.configFile, "c", "/etc/kateway.cf", "config file path")
	flag.StringVar(&options.tplFile, "t", "etc/haproxy.tpl", "haproxy config template file")
	flag.StringVar(&options.haproxyConfigFile, "haproxycf", ".haproxy.cf", "haproxy config file")

	flag.Parse()

	if options.guide {
		displayGuide()
		os.Exit(0)
	}
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

	root := zkr.Root(options.zone)
	ch := make(chan []string, 10)
	go etclib.WatchChildren(root, ch)

	var servers BackendServers
	for {
		select {
		case <-ch:
			children, err := etclib.Children(root)
			if err != nil {
				log.Error(err)
				continue
			}

			log.Info("kateway cluster changed to %+v", children)

			for _, kwId := range children {
				kwNode := fmt.Sprintf("%s/%s", root, kwId)
				data, err := etclib.Get(kwNode)
				if err != nil {
					log.Error(err)
					continue
				}

				info := make(map[string]string)
				if err = json.Unmarshal([]byte(data), &info); err != nil {
					log.Error(err)
					continue
				}

				if info["pub"] != "" {
					if servers.Pub == nil {
						servers.Pub = make([]Backend, 0)
					}
					be := Backend{
						Name: "s" + info["id"],
						Ip:   info["host"],
						Port: info["pub"],
					}
					servers.Pub = append(servers.Pub, be)
				}
				if info["sub"] != "" {
					if servers.Sub == nil {
						servers.Sub = make([]Backend, 0)
					}
					be := Backend{
						Name: "s" + info["id"],
						Ip:   info["host"],
						Port: info["sub"],
					}
					servers.Sub = append(servers.Sub, be)
				}
				if info["man"] != "" {
					if servers.Man == nil {
						servers.Man = make([]Backend, 0)
					}
					be := Backend{
						Name: "s" + info["id"],
						Ip:   info["host"],
						Port: info["man"],
					}
					servers.Man = append(servers.Man, be)
				}

				log.Info(servers)
			}

			if err = createConfigFile(servers, options.tplFile, options.haproxyConfigFile); err != nil {
				log.Error(err)
				continue
			}

			if err = reloadHAproxy(options.haproxyBin, options.haproxyConfigFile); err != nil {
				log.Error(err)
			}

		}
	}

}
