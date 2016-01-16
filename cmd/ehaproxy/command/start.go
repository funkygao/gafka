package command

import (
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"strings"

	"github.com/funkygao/etclib"
	"github.com/funkygao/gafka/ctx"
	zkr "github.com/funkygao/gafka/registry/zk"
	"github.com/funkygao/gocli"
	log "github.com/funkygao/log4go"
)

type Start struct {
	Ui  cli.Ui
	Cmd string

	zone string
	root string
}

func (this *Start) Run(args []string) (exitCode int) {
	cmdFlags := flag.NewFlagSet("start", flag.ContinueOnError)
	cmdFlags.Usage = func() { this.Ui.Output(this.Help()) }
	cmdFlags.StringVar(&this.zone, "z", "", "")
	cmdFlags.StringVar(&this.root, "p", defaultPrefix, "")
	if err := cmdFlags.Parse(args); err != nil {
		return 1
	}

	if err := os.Chdir(this.root); err != nil {
		panic(err)
	}

	this.main()

	return
}

func (this *Start) main() {
	ctx.LoadFromHome()

	if err := etclib.Dial(strings.Split(ctx.ZoneZkAddrs(this.zone), ",")); err != nil {
		panic(err)
	}

	root := zkr.Root(this.zone)
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

			if err = this.createConfigFile(servers); err != nil {
				log.Error(err)
				continue
			}

			if err = this.reloadHAproxy(); err != nil {
				log.Error(err)
			}

		}
	}
}

func (this *Start) Synopsis() string {
	return fmt.Sprintf("Start %s system on localhost", this.Cmd)
}

func (this *Start) Help() string {
	help := fmt.Sprintf(`
Usage: %s start [options]

    Start %s system on localhost

Options:

    -z zone

    -p prefix

`, this.Cmd, this.Cmd)
	return strings.TrimSpace(help)
}
