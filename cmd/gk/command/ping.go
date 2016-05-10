package command

import (
	"flag"
	"fmt"
	"strings"
	"time"

	"github.com/Shopify/sarama"
	"github.com/funkygao/gafka/ctx"
	"github.com/funkygao/gafka/zk"
	"github.com/funkygao/gocli"
	log "github.com/funkygao/log4go"
)

type Ping struct {
	Ui  cli.Ui
	Cmd string

	zone     string
	logfile  string
	interval time.Duration
}

// TODO run 3 nodes in a zone to monitor as daemon
// register the 3 nodes as host service tag.
func (this *Ping) Run(args []string) (exitCode int) {
	cmdFlags := flag.NewFlagSet("ping", flag.ContinueOnError)
	cmdFlags.Usage = func() { this.Ui.Output(this.Help()) }
	cmdFlags.StringVar(&this.zone, "z", "", "")
	cmdFlags.DurationVar(&this.interval, "interval", time.Minute*5, "")
	cmdFlags.StringVar(&this.logfile, "logfile", "gk.ping.log", "")
	if err := cmdFlags.Parse(args); err != nil {
		return 1
	}

	if validateArgs(this, this.Ui).
		require("-z").
		invalid(args) {
		return 2
	}

	this.setupLog()

	for {
		this.diagnose()

		time.Sleep(this.interval)
	}

	return
}

func (this *Ping) setupLog() {
	log.DeleteFilter("stdout")

	filer := log.NewFileLogWriter(this.logfile, true, false)
	filer.SetFormat("[%d %T] [%L] (%S) %M")
	filer.SetRotateSize(0)
	filer.SetRotateLines(0)
	filer.SetRotateDaily(true)
	log.AddFilter("file", log.DEBUG, filer)
}

func (this *Ping) diagnose() {
	zkzone := zk.NewZkZone(zk.DefaultConfig(this.zone, ctx.ZoneZkAddrs(this.zone)))
	zkzone.ForSortedClusters(func(zkcluster *zk.ZkCluster) {
		registeredBrokers := zkcluster.RegisteredInfo().Roster
		for _, broker := range registeredBrokers {
			log.Debug("ping %s", broker.Addr())
			kfk, err := sarama.NewClient([]string{broker.Addr()}, sarama.NewConfig())
			if err != nil {
				log.Error("broker %s: %v", broker.Addr(), err)
				continue
			}

			_, err = kfk.Topics() // kafka didn't provide ping, so use Topics() as ping
			if err != nil {
				log.Error("broker %s: %v", broker.Addr(), err)
			}
			kfk.Close()
		}
	})

}

func (*Ping) Synopsis() string {
	return "Ping liveness of all registered brokers in a zone"
}

func (this *Ping) Help() string {
	help := fmt.Sprintf(`
Usage: %s ping -z zone [options]

    Ping liveness of all registered brokers in a zone

Options:

    -interval duration
      Defaults 5m

    -logfile filename
      Defaults gk.ping.log in current directory

`, this.Cmd)
	return strings.TrimSpace(help)
}
