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

	zone     string
	root     string
	command  string
	logfile  string
	starting bool
}

func (this *Start) Run(args []string) (exitCode int) {
	cmdFlags := flag.NewFlagSet("start", flag.ContinueOnError)
	cmdFlags.Usage = func() { this.Ui.Output(this.Help()) }
	cmdFlags.StringVar(&this.logfile, "log", defaultLogfile, "")
	cmdFlags.StringVar(&this.zone, "z", ctx.ZkDefaultZone(), "")
	cmdFlags.StringVar(&this.root, "p", defaultPrefix, "")
	if err := cmdFlags.Parse(args); err != nil {
		return 1
	}

	err := os.Chdir(this.root)
	swalllow(err)

	this.command = fmt.Sprintf("%s/sbin/haproxy", this.root)
	if _, err := os.Stat(this.command); err != nil {
		panic(err)
	}

	defer os.Remove(configFile)

	this.setupLogging(this.logfile, "info", "panic")
	this.starting = true
	this.main()

	return
}

func (this *Start) main() {
	ctx.LoadFromHome()

	// TODO zk session timeout
	err := etclib.Dial(strings.Split(ctx.ZoneZkAddrs(this.zone), ","))
	swalllow(err)

	root := zkr.Root(this.zone)
	ch := make(chan []string, 10)
	go etclib.WatchChildren(root, ch)

	var servers = BackendServers{
		CpuNum:      ctx.NumCPU(),
		HaproxyRoot: this.root,
		LogDir:      fmt.Sprintf("%s/logs", this.root),
	}
	var lastInstances []string
	for {
		select {
		case <-ch:
			kwInstances, err := etclib.Children(root)
			if err != nil {
				log.Error(err)
				continue
			}

			log.Info("kateway ids: %+v -> %+v", lastInstances, kwInstances)
			lastInstances = kwInstances

			servers.reset()
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
					be := Backend{
						Name: "p" + info["id"],
						Addr: info["pub"],
						Cpu:  info["cpu"],
					}
					servers.Pub = append(servers.Pub, be)
				}

				// sub
				if info["sub"] != "" {
					be := Backend{
						Name: "s" + info["id"],
						Addr: info["sub"],
						Cpu:  info["cpu"],
					}
					servers.Sub = append(servers.Sub, be)
				}

				// man
				if info["man"] != "" {
					be := Backend{
						Name: "m" + info["id"],
						Addr: info["man"],
						Cpu:  info["cpu"],
					}
					servers.Man = append(servers.Man, be)
				}

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

    -log log file
      Default %s

`, this.Cmd, this.Cmd, ctx.ZkDefaultZone(), defaultPrefix, defaultLogfile)
	return strings.TrimSpace(help)
}
