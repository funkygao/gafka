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
	"github.com/funkygao/golib/color"
	log "github.com/funkygao/log4go"
)

type Ping struct {
	Ui  cli.Ui
	Cmd string

	zone            string
	cluster         string
	zkzone          *zk.ZkZone
	logfile         string
	problematicMode bool
	interval        time.Duration
}

// TODO run 3 nodes in a zone to monitor as daemon
// register the 3 nodes as host service tag.
func (this *Ping) Run(args []string) (exitCode int) {
	cmdFlags := flag.NewFlagSet("ping", flag.ContinueOnError)
	cmdFlags.Usage = func() { this.Ui.Output(this.Help()) }
	cmdFlags.StringVar(&this.zone, "z", ctx.ZkDefaultZone(), "")
	cmdFlags.StringVar(&this.cluster, "c", "", "")
	cmdFlags.DurationVar(&this.interval, "interval", time.Minute*5, "")
	cmdFlags.StringVar(&this.logfile, "logfile", "stdout", "")
	cmdFlags.BoolVar(&this.problematicMode, "p", false, "")
	if err := cmdFlags.Parse(args); err != nil {
		return 1
	}

	this.setupLog()
	this.zkzone = zk.NewZkZone(zk.DefaultConfig(this.zone, ctx.ZoneZkAddrs(this.zone)))

	for {
		this.diagnose()
		if this.logfile == "stdout" {
			break
		}

		time.Sleep(this.interval)
	}

	return
}

func (this *Ping) setupLog() {
	if this.logfile != "stdout" {
		log.DeleteFilter("stdout")

		filer := log.NewFileLogWriter(this.logfile, true, false, 0)
		filer.SetFormat("[%d %T] [%L] (%S) %M")
		filer.SetRotateSize(0)
		filer.SetRotateLines(0)
		filer.SetRotateDaily(true)
		log.AddFilter("file", log.DEBUG, filer)
	}

}

func (this *Ping) diagnose() {
	this.zkzone.ForSortedClusters(func(zkcluster *zk.ZkCluster) {
		if this.cluster != "" && this.cluster != zkcluster.Name() {
			return
		}

		registeredBrokers := zkcluster.RegisteredInfo().Roster
		for _, broker := range registeredBrokers {
			log.Debug("ping %s", broker.Addr())

			kfk, err := sarama.NewClient([]string{broker.Addr()}, saramaConfig())
			if err != nil {
				log.Error("%25s %30s %s", broker.Addr(), broker.NamedAddr(), color.Red(err.Error()))

				continue
			}

			_, err = kfk.Topics() // kafka didn't provide ping, so use Topics() as ping
			if err != nil {
				log.Error("%25s %30s %s", broker.Addr(), broker.NamedAddr(), color.Red(err.Error()))
			} else {
				if !this.problematicMode {
					log.Info("%25s %30s %s", broker.Addr(), broker.NamedAddr(), color.Green("ok"))
				}
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
Usage: %s ping [options]

    %s

Options:

    -z zone

    -c cluster
    
    -p
      Only show problematic brokers

    -interval duration
      Defaults 5m

    -logfile filename
      Defaults stdout in current directory

`, this.Cmd, this.Synopsis())
	return strings.TrimSpace(help)
}
