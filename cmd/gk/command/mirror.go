package command

import (
	"flag"
	"fmt"
	"log"
	"os"
	"strings"
	"time"

	"github.com/Shopify/sarama"
	"github.com/funkygao/gafka/ctx"
	"github.com/funkygao/gafka/zk"
	"github.com/funkygao/gocli"
	"github.com/funkygao/kafka-cg/consumergroup"
)

type Mirror struct {
	Ui  cli.Ui
	Cmd string

	zone1, zone2       string
	cluster1, cluster2 string
	excludes           string
	topicsExcluded     map[string]struct{}
}

func (this *Mirror) Run(args []string) (exitCode int) {
	cmdFlags := flag.NewFlagSet("mirror", flag.ContinueOnError)
	cmdFlags.Usage = func() { this.Ui.Output(this.Help()) }
	cmdFlags.StringVar(&this.zone1, "z1", "", "")
	cmdFlags.StringVar(&this.zone2, "z2", "", "")
	cmdFlags.StringVar(&this.cluster1, "c1", "", "")
	cmdFlags.StringVar(&this.cluster2, "c2", "", "")
	cmdFlags.StringVar(&this.excludes, "excluded", "", "")
	if err := cmdFlags.Parse(args); err != nil {
		return 1
	}

	if validateArgs(this, this.Ui).
		require("-z1", "-z2", "-c1", "-c2").
		invalid(args) {
		return 2
	}

	this.topicsExcluded = make(map[string]struct{})
	for _, e := range strings.Split(this.excludes, ",") {
		this.topicsExcluded[e] = struct{}{}
	}

	log.SetOutput(os.Stdout)

	z1 := zk.NewZkZone(zk.DefaultConfig(this.zone1, ctx.ZoneZkAddrs(this.zone1)))
	z2 := zk.NewZkZone(zk.DefaultConfig(this.zone2, ctx.ZoneZkAddrs(this.zone2)))
	c1 := z1.NewCluster(this.cluster1)
	c2 := z2.NewCluster(this.cluster2)
	this.makeMirror(c1, c2)

	return
}

func (this *Mirror) makeMirror(c1, c2 *zk.ZkCluster) {
	pub, err := sarama.NewAsyncProducer(c2.BrokerList(), sarama.NewConfig())
	swallow(err)

	// TODO
	// *. topics might change at any time
	topics := this.topicsOfCluster(c1)
	log.Printf("topics: %+v", topics)
	if len(topics) == 0 {
		log.Println("empty topics")
		return
	}

	cf := consumergroup.NewConfig()
	cf.Zookeeper.Chroot = c1.Chroot()
	cf.Offsets.CommitInterval = time.Minute
	cf.ChannelBufferSize = 0
	cf.Consumer.Return.Errors = true
	sub, err := consumergroup.JoinConsumerGroup("_mirror_group_", topics,
		c1.ZkZone().ZkAddrList(), cf)
	swallow(err)

	log.Println("starting pump")
	this.pump(sub, pub)
}

func (this *Mirror) pump(sub *consumergroup.ConsumerGroup, pub sarama.AsyncProducer) {
	var n int64
	for {
		select {
		case <-time.After(time.Second * 5):
			log.Println("idle 5s waiting for new msg")

		case msg := <-sub.Messages():
			pub.Input() <- &sarama.ProducerMessage{
				Topic: msg.Topic,
				Key:   sarama.ByteEncoder(msg.Key),
				Value: sarama.ByteEncoder(msg.Value),
			}

			n++
			if n%2000 == 0 {
				log.Println(n)
			}

		case err := <-sub.Errors():
			log.Println(err.Error())
		}
	}
}

func (this *Mirror) topicsOfCluster(c *zk.ZkCluster) []string {
	kc, err := sarama.NewClient(c.BrokerList(), sarama.NewConfig())
	swallow(err)
	defer kc.Close()

	topics, err := kc.Topics()
	swallow(err)

	return topics
}

func (*Mirror) Synopsis() string {
	return "Continuously copy data between two Kafka clusters"
}

func (this *Mirror) Help() string {
	help := fmt.Sprintf(`
Usage: %s mirror [options]

    Continuously copy data between two Kafka clusters

Options:

    -z1 from zone

    -z2 to zone

    -c1 from cluster

    -c2 to cluster

    -exclude comma seperated topic names

`, this.Cmd)
	return strings.TrimSpace(help)
}
