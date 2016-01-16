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

	zone    string
	root    string
	command string
	pid     int
}

func (this *Start) Run(args []string) (exitCode int) {
	cmdFlags := flag.NewFlagSet("start", flag.ContinueOnError)
	cmdFlags.Usage = func() { this.Ui.Output(this.Help()) }
	cmdFlags.StringVar(&this.zone, "z", ctx.ZkDefaultZone(), "")
	cmdFlags.StringVar(&this.root, "p", defaultPrefix, "")
	if err := cmdFlags.Parse(args); err != nil {
		return 1
	}

	err := os.Chdir(this.root)
	swalllow(err)

	if _, err = os.Stat(configFile); err == nil {
		this.Ui.Error(fmt.Sprintf("another %s is running", this.Cmd))
		return 1
	}

	this.command = fmt.Sprintf("%s/sbin/haproxy", this.root)
	if _, err := os.Stat(this.command); err != nil {
		panic(err)
	}

	defer os.Remove(configFile)

	this.pid = -1
	this.main()

	return
}

func (this *Start) main() {
	ctx.LoadFromHome()

	err := etclib.Dial(strings.Split(ctx.ZoneZkAddrs(this.zone), ","))
	swalllow(err)

	root := zkr.Root(this.zone)
	ch := make(chan []string, 10)
	go etclib.WatchChildren(root, ch)

	var servers BackendServers
	var lastInstances []string
	for {
		select {
		case <-ch:
			kwInstances, err := etclib.Children(root)
			if err != nil {
				log.Error(err)
				continue
			}

			log.Info("kateway cluster changed from %+v to %+v", lastInstances, kwInstances)
			lastInstances = kwInstances

			for _, kwId := range kwInstances {
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

				// pub
				if info["pub"] != "" {
					if servers.Pub == nil {
						servers.Pub = make([]Backend, 0)
					}
					be := Backend{
						Name: "s" + info["id"],
						Addr: info["pub"],
					}
					servers.Pub = append(servers.Pub, be)
				}

				// sub
				if info["sub"] != "" {
					if servers.Sub == nil {
						servers.Sub = make([]Backend, 0)
					}
					be := Backend{
						Name: "s" + info["id"],
						Addr: info["sub"],
					}
					servers.Sub = append(servers.Sub, be)
				}

				// man
				if info["man"] != "" {
					if servers.Man == nil {
						servers.Man = make([]Backend, 0)
					}
					be := Backend{
						Name: "s" + info["id"],
						Addr: info["man"],
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
      Default %s

    -p prefix
      Default %s

`, this.Cmd, this.Cmd, ctx.ZkDefaultZone(), defaultPrefix)
	return strings.TrimSpace(help)
}
