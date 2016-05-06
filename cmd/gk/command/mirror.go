package command

import (
	"flag"
	"fmt"
	"log"
	"os"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/Shopify/sarama"
	"github.com/funkygao/gafka/ctx"
	"github.com/funkygao/gafka/zk"
	"github.com/funkygao/gocli"
	"github.com/funkygao/golib/gofmt"
	"github.com/funkygao/golib/ratelimiter"
	"github.com/funkygao/golib/signal"
	"github.com/funkygao/kafka-cg/consumergroup"
)

type Mirror struct {
	Ui  cli.Ui
	Cmd string

	quit chan struct{}
	once sync.Once

	zone1, zone2       string
	cluster1, cluster2 string
	excludes           string
	topicsExcluded     map[string]struct{}
	debug              bool

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
	cmdFlags.BoolVar(&this.debug, "debug", false, "")
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

	log.SetOutput(os.Stdout)
	this.quit = make(chan struct{})
	limit := (1 << 20) * this.bandwidthLimit / 8
	this.bandwidthRateLimiter = ratelimiter.NewLeakyBucket(limit*10, time.Second*10)
	log.Printf("[%s]%s -> [%s]%s with bandwidth %dbps",
		this.zone1, this.cluster1,
		this.zone2, this.cluster2,
		limit)
	signal.RegisterSignalsHandler(func(sig os.Signal) {
		log.Printf("received signal: %s", strings.ToUpper(sig.String()))
		log.Println("quiting...")

		this.once.Do(func() {
			close(this.quit)
		})
	}, syscall.SIGINT, syscall.SIGTERM)

	z1 := zk.NewZkZone(zk.DefaultConfig(this.zone1, ctx.ZoneZkAddrs(this.zone1)))
	z2 := zk.NewZkZone(zk.DefaultConfig(this.zone2, ctx.ZoneZkAddrs(this.zone2)))
	c1 := z1.NewCluster(this.cluster1)
	c2 := z2.NewCluster(this.cluster2)
	this.makeMirror(c1, c2)

	return
}

func (this *Mirror) makeMirror(c1, c2 *zk.ZkCluster) {
	pub, err := this.makePub(c2)
	swallow(err)

	topics, topicsChanges, err := c1.WatchTopics()
	swallow(err)

	log.Printf("topics: %+v", topics)
	if len(topics) == 0 {
		log.Println("empty topics")
		return
	}

	group := fmt.Sprintf("%s.%s._mirror_", c1.Name(), c2.Name())
	sub, err := this.makeSub(c1, group, topics)
	swallow(err)

	pumpStopper := make(chan struct{})
	go this.pump(sub, pub, pumpStopper)

	for {
		select {
		case <-topicsChanges:
			log.Println("topics changed, stopping pump...")
			pumpStopper <- struct{}{} // stop pump
			<-pumpStopper             // await pump cleanup

			// refresh c1 topics
			topics, err = c1.Topics()
			if err != nil {
				// TODO how to handle this err?
				log.Println(err)
			}

			log.Printf("topics: %+v", topics)

			sub, err = this.makeSub(c1, group, topics)
			if err != nil {
				// TODO how to handle this err?
				log.Println(err)
			}

			go this.pump(sub, pub, pumpStopper)

		case <-this.quit:
			log.Println("awaiting pump cleanup...")
			<-pumpStopper
			break
		}
	}

}

func (this *Mirror) makePub(c2 *zk.ZkCluster) (sarama.AsyncProducer, error) {
	// TODO setup batch size
	return sarama.NewAsyncProducer(c2.BrokerList(), sarama.NewConfig())
}

func (this *Mirror) makeSub(c1 *zk.ZkCluster, group string, topics []string) (*consumergroup.ConsumerGroup, error) {
	cf := consumergroup.NewConfig()
	cf.Zookeeper.Chroot = c1.Chroot()
	cf.Offsets.CommitInterval = time.Second * 10
	cf.ChannelBufferSize = 200
	cf.Consumer.Return.Errors = true
	sub, err := consumergroup.JoinConsumerGroup(group, topics, c1.ZkZone().ZkAddrList(), cf)
	return sub, err
}

func (this *Mirror) pump(sub *consumergroup.ConsumerGroup, pub sarama.AsyncProducer, stop chan struct{}) {
	var (
		n      int64
		active = true
	)

	defer func() {
		log.Println("pump cleanup...")
		sub.Close()
		pub.Close()
		log.Println("pump cleanup ok")

		stop <- struct{}{} // notify others I'm done
	}()

	log.Printf("start pumping")
	for {
		select {
		case <-this.quit:
			log.Println("received quit command")
			return

		case <-stop:
			// yes sir!
			log.Println("received stop command")
			return

		case <-time.After(time.Second * 10):
			active = false
			log.Println("idle 10s waiting for new msg")

		case msg := <-sub.Messages():
			if !active || this.debug {
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
			bytesN := len(msg.Topic) + len(msg.Key) + len(msg.Value) + 20 // payload overhead
			for {
				if !this.bandwidthRateLimiter.Pour(bytesN) {
					time.Sleep(time.Millisecond * 5)
					log.Printf("%d -> bandwidth reached", bytesN)
				}
			}

			n++
			if n%this.progressStep == 0 {
				log.Println(gofmt.Comma(n))
			}

		case err := <-sub.Errors():
			log.Println(err.Error()) // TODO need reconnect?
		}
	}
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

    -debug

`, this.Cmd)
	return strings.TrimSpace(help)
}
