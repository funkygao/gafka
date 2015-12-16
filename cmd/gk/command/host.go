package command

import (
	"flag"
	"fmt"
	"net"
	"strings"
	"time"

	"github.com/Shopify/sarama"
	"github.com/funkygao/gafka/ctx"
	"github.com/funkygao/gafka/zk"
	"github.com/funkygao/gocli"
	"github.com/funkygao/golib/color"
)

type Host struct {
	Ui  cli.Ui
	Cmd string

	zone string
	host string
}

func (this *Host) Run(args []string) (exitCode int) {
	cmdFlags := flag.NewFlagSet("host", flag.ContinueOnError)
	cmdFlags.Usage = func() { this.Ui.Output(this.Help()) }
	cmdFlags.StringVar(&this.zone, "z", "", "")
	cmdFlags.StringVar(&this.host, "ip", "", "")
	if err := cmdFlags.Parse(args); err != nil {
		return 1
	}

	if validateArgs(this, this.Ui).
		require("-z", "-h").
		invalid(args) {
		return 2
	}

	for {
		this.diagnose()
		this.Ui.Output(color.Green("%s", strings.Repeat("=", 40)))

		time.Sleep(time.Second * 5)
	}

	return
}

func (this *Host) diagnose() {
	clusters := make([]string, 0)
	leadingTopics := make(map[string][]int32) // topic:partitionID
	msgs := make(map[string]int64)

	// check if it is registered
	zkzone := zk.NewZkZone(zk.DefaultConfig(this.zone, ctx.ZoneZkAddrs(this.zone)))
	zkzone.ForSortedBrokers(func(cluster string, liveBrokers map[string]*zk.BrokerZnode) {
		zkcluster := zkzone.NewCluster(cluster)

		for _, broker := range liveBrokers {
			if broker.Host != this.host {
				continue
			}

			this.Ui.Output(broker.String())

			clusters = append(clusters, cluster)

			kfk, err := sarama.NewClient([]string{broker.Addr()}, sarama.NewConfig())
			if err != nil {
				this.Ui.Error(err.Error())
				continue
			}

			topics, err := kfk.Topics()
			swallow(err)
			for _, topic := range topics {
				partions, err := kfk.WritablePartitions(topic)
				swallow(err)
				for _, partitionID := range partions {
					leader, err := kfk.Leader(topic, partitionID)
					swallow(err)
					host, _, err := net.SplitHostPort(leader.Addr())
					swallow(err)
					if host != this.host {
						continue
					}

					latestOffset, err := kfk.GetOffset(topic, partitionID, sarama.OffsetNewest)
					swallow(err)
					oldestOffset, err := kfk.GetOffset(topic, partitionID, sarama.OffsetOldest)
					swallow(err)

					msgs[topic] += (latestOffset - oldestOffset)
					if _, present := leadingTopics[topic]; !present {
						leadingTopics[topic] = make([]int32, 0)
					}
					leadingTopics[topic] = append(leadingTopics[topic], partitionID)
				}
			}

			kfk.Close()
		}

		registeredBrokers := zkcluster.RegisteredInfo().Roster
		for _, b := range registeredBrokers {
			if b.Host == this.host {
				this.Ui.Output(fmt.Sprintf("registered in cluster: %s", cluster))
			}
		}
	})

	this.Ui.Output(color.Cyan("clusters\n%s", strings.Repeat("-", 80)))
	for _, c := range clusters {
		this.Ui.Output(c)
	}

	this.Ui.Output(color.Cyan("clusters\n%s", strings.Repeat("-", 80)))
	for t, ps := range leadingTopics {
		this.Ui.Output(fmt.Sprintf("%35s M:%-10d P:%+v", t, msgs[t], ps))
	}
}

func (*Host) Synopsis() string {
	return "Diagnose a broker by ip address TODO"
}

func (this *Host) Help() string {
	help := fmt.Sprintf(`
Usage: %s host -z zone -ip addr

    Diagnose a broker by ip

`, this.Cmd)
	return strings.TrimSpace(help)
}
