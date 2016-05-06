package command

import (
	"flag"
	"fmt"
	"log"
	"os"
	"strings"
	"syscall"
	"time"

	"github.com/Shopify/sarama"
	"github.com/funkygao/gafka/ctx"
	"github.com/funkygao/gafka/zk"
	"github.com/funkygao/gocli"
	"github.com/funkygao/golib/ratelimiter"
	"github.com/funkygao/golib/signal"
	"github.com/funkygao/kafka-cg/consumergroup"
)

type Mirror struct {
	Ui  cli.Ui
	Cmd string

	quit chan struct{}

	zone1, zone2       string
	cluster1, cluster2 string
	excludes           string
	topicsExcluded     map[string]struct{}

	bandwidthLimit       int
	bandwidthRateLimiter *ratelimiter.LeakyBucket
	progressStep         int64
}

func (this *Mirror) Run(args []string) (exitCode int) {
	cmdFlags := flag.NewFlagSet("mirror", flag.ContinueOnError)
	cmdFlags.Usage = func() { this.Ui.Output(this.Help()) }
	cmdFlags.StringVar(&this.zone1, "z1", "", "")
	cmdFlags.StringVar(&this.zone2, "z2", "", "")
	cmdFlags.StringVar(&this.cluster1, "c1", "", "")
	cmdFlags.StringVar(&this.cluster2, "c2", "", "")
	cmdFlags.StringVar(&this.excludes, "excluded", "", "")
	cmdFlags.IntVar(&this.bandwidthLimit, "net", 100, "")
	cmdFlags.Int64Var(&this.progressStep, "step", 5000, "")
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

	this.quit = make(chan struct{})
	this.bandwidthRateLimiter = ratelimiter.NewLeakyBucket((1<<20)*this.bandwidthLimit/8, time.Second)
	log.SetOutput(os.Stdout)
	signal.RegisterSignalsHandler(func(sig os.Signal) {
		log.Printf("received signal: %s", strings.ToUpper(sig.String()))

		close(this.quit)
	}, syscall.SIGINT, syscall.SIGTERM)

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
	cf.Offsets.CommitInterval = time.Second * 10
	cf.ChannelBufferSize = 200
	cf.Consumer.Return.Errors = true
	group := fmt.Sprintf("%s.%s._mirror_", c1.Name(), c2.Name())
	sub, err := consumergroup.JoinConsumerGroup(group, topics, c1.ZkZone().ZkAddrList(), cf)
	swallow(err)

	log.Println("start pumping")
	this.pump(sub, pub)
}

func (this *Mirror) pump(sub *consumergroup.ConsumerGroup, pub sarama.AsyncProducer) {
	var (
		n                 int64
		active            = true
		watchTopicsTicker = time.NewTicker(time.Minute)
	)

	defer func() {
		watchTopicsTicker.Stop()

		sub.Close()
		pub.Close()

		log.Println("pump cleanup ok")
	}()

	for {
		select {
		case <-this.quit:
			return

		case <-watchTopicsTicker.C:
			// TODO check whether topics has changed

		case <-time.After(time.Second * 10):
			active = false
			log.Println("idle 10s waiting for new msg")

		case msg := <-sub.Messages():
			if !active {
				log.Printf("<-[%d] T:%s M:%s", n, msg.Topic, string(msg.Value))
			}
			active = true

			pub.Input() <- &sarama.ProducerMessage{
				Topic: msg.Topic,
				Key:   sarama.ByteEncoder(msg.Key),
				Value: sarama.ByteEncoder(msg.Value),
			}
			sub.CommitUpto(msg)

			// rate limit, never overflood the limited bandwidth between IDCs
			for {
				bytesN := len(msg.Topic) + len(msg.Key) + len(msg.Value) + 20 // payload overhead
				if !this.bandwidthRateLimiter.Pour(bytesN) {
					time.Sleep(time.Millisecond * 5)
				}
			}

			n++
			if n%this.progressStep == 0 {
				log.Println(n)
			}

		case err := <-sub.Errors():
			log.Println(err.Error()) // TODO need reconnect?
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

    -net bandwidth limit in Mbps
      Defaults 100Mbps.

    -step n
      Defaults 5000.

`, this.Cmd)
	return strings.TrimSpace(help)
}
