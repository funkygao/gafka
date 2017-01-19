package command

import (
	"bytes"
	"encoding/json"
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
	"github.com/funkygao/go-metrics"
	"github.com/funkygao/gocli"
	"github.com/funkygao/golib/color"
	"github.com/funkygao/golib/gofmt"
	"github.com/funkygao/golib/signal"
)

var (
	stats *peekStats
)

type peekStats struct {
	MsgCountPerSecond metrics.Meter
	MsgBytesPerSecond metrics.Meter
}

func newPeekStats() *peekStats {
	this := &peekStats{
		MsgCountPerSecond: metrics.NewMeter(),
		MsgBytesPerSecond: metrics.NewMeter(),
	}

	metrics.Register("msg.count.per.second", this.MsgCountPerSecond)
	metrics.Register("msg.bytes.per.second", this.MsgBytesPerSecond)
	return this
}

func (this *peekStats) start() {
	metrics.Log(metrics.DefaultRegistry, time.Second*10,
		log.New(os.Stdout, "metrics: ", log.Lmicroseconds))
}

type Peek struct {
	Ui  cli.Ui
	Cmd string

	offset   int64
	lastN    int64 // peek the most recent N messages
	colorize bool
	limit    int
	quit     chan struct{}
	once     sync.Once
	column   string
	beep     bool
	pretty   bool
	bodyOnly bool
}

func (this *Peek) Run(args []string) (exitCode int) {
	var (
		cluster      string
		zone         string
		topicPattern string
		partitionId  int
		wait         time.Duration
		tillNow      bool
		silence      bool
	)
	cmdFlags := flag.NewFlagSet("peek", flag.ContinueOnError)
	cmdFlags.Usage = func() { this.Ui.Output(this.Help()) }
	cmdFlags.StringVar(&zone, "z", ctx.ZkDefaultZone(), "")
	cmdFlags.StringVar(&cluster, "c", "", "")
	cmdFlags.StringVar(&topicPattern, "t", "", "")
	cmdFlags.IntVar(&partitionId, "p", 0, "")
	cmdFlags.BoolVar(&this.colorize, "color", true, "")
	cmdFlags.Int64Var(&this.lastN, "last", -1, "")
	cmdFlags.BoolVar(&this.pretty, "pretty", false, "")
	cmdFlags.IntVar(&this.limit, "n", -1, "")
	cmdFlags.StringVar(&this.column, "col", "", "") // TODO support multiple columns
	cmdFlags.BoolVar(&this.beep, "beep", false, "")
	cmdFlags.Int64Var(&this.offset, "offset", sarama.OffsetNewest, "")
	cmdFlags.BoolVar(&silence, "s", false, "")
	cmdFlags.DurationVar(&wait, "d", time.Hour, "")
	cmdFlags.BoolVar(&tillNow, "now", false, "")
	cmdFlags.BoolVar(&this.bodyOnly, "body", false, "")
	if err := cmdFlags.Parse(args); err != nil {
		return 1
	}

	if this.pretty {
		this.bodyOnly = true
	}

	this.quit = make(chan struct{})

	if silence {
		stats := newPeekStats()
		go stats.start()
	}

	zkzone := zk.NewZkZone(zk.DefaultConfig(zone, ctx.ZoneZkAddrs(zone)))
	msgChan := make(chan *sarama.ConsumerMessage, 20000) // msg aggerator channel
	if cluster == "" {
		zkzone.ForSortedClusters(func(zkcluster *zk.ZkCluster) {
			this.consumeCluster(zkcluster, topicPattern, partitionId, msgChan)
		})
	} else {
		zkcluster := zkzone.NewCluster(cluster)
		this.consumeCluster(zkcluster, topicPattern, partitionId, msgChan)
	}

	signal.RegisterHandler(func(sig os.Signal) {
		log.Printf("received signal: %s", strings.ToUpper(sig.String()))
		log.Println("quiting...")

		this.once.Do(func() {
			close(this.quit)
		})
	}, syscall.SIGINT, syscall.SIGTERM)

	var (
		startAt = time.Now()
		msg     *sarama.ConsumerMessage
		total   int
		bytesN  int64
	)

	var (
		j          map[string]interface{}
		prettyJSON bytes.Buffer
	)

LOOP:
	for {
		if time.Since(startAt) >= wait {
			this.Ui.Output(fmt.Sprintf("Total: %s msgs, %s, elapsed: %s",
				gofmt.Comma(int64(total)), gofmt.ByteSize(bytesN), time.Since(startAt)))
			elapsed := time.Since(startAt).Seconds()
			if elapsed > 1. {
				this.Ui.Output(fmt.Sprintf("Speed: %d/s", total/int(elapsed)))
				if total > 0 {
					this.Ui.Output(fmt.Sprintf("Size : %s/msg", gofmt.ByteSize(bytesN/int64(total))))
				}
			}

			return
		}

		select {
		case <-this.quit:
			this.Ui.Output(fmt.Sprintf("Total: %s msgs, %s, elapsed: %s",
				gofmt.Comma(int64(total)), gofmt.ByteSize(bytesN), time.Since(startAt)))
			elapsed := time.Since(startAt).Seconds()
			if elapsed > 1. {
				this.Ui.Output(fmt.Sprintf("Speed: %d/s", total/int(elapsed)))
				if total > 0 {
					this.Ui.Output(fmt.Sprintf("Size : %s/msg", gofmt.ByteSize(bytesN/int64(total))))
				}
			}

			return

		case <-time.After(time.Second):
			if tillNow {
				return
			} else {
				continue
			}

		case msg = <-msgChan:
			if silence {
				stats.MsgCountPerSecond.Mark(1)
				stats.MsgBytesPerSecond.Mark(int64(len(msg.Value)))
			} else {
				var outmsg string
				if this.column != "" {
					if err := json.Unmarshal(msg.Value, &j); err != nil {
						this.Ui.Error(err.Error())
					} else {
						var colVal string
						switch t := j[this.column].(type) {
						case string:
							colVal = t
						case float64:
							colVal = fmt.Sprintf("%.0f", t)
						case int:
							colVal = fmt.Sprintf("%d", t)
						}

						if this.bodyOnly {
							if this.pretty {
								if err = json.Indent(&prettyJSON, []byte(colVal), "", "    "); err != nil {
									fmt.Println(err.Error())
								} else {
									outmsg = string(prettyJSON.Bytes())
								}
							} else {
								outmsg = colVal
							}
						} else if this.colorize {
							outmsg = fmt.Sprintf("%s/%d %s k:%s v:%s",
								color.Green(msg.Topic), msg.Partition,
								gofmt.Comma(msg.Offset), string(msg.Key), colVal)
						} else {
							// colored UI will have invisible chars output
							outmsg = fmt.Sprintf("%s/%d %s k:%s v:%s",
								msg.Topic, msg.Partition,
								gofmt.Comma(msg.Offset), string(msg.Key), colVal)
						}
					}

				} else {
					if this.bodyOnly {
						if this.pretty {
							json.Indent(&prettyJSON, msg.Value, "", "    ")
							outmsg = string(prettyJSON.Bytes())
						} else {
							outmsg = string(msg.Value)
						}
					} else if this.colorize {
						outmsg = fmt.Sprintf("%s/%d %s k:%s, v:%s",
							color.Green(msg.Topic), msg.Partition,
							gofmt.Comma(msg.Offset), string(msg.Key), string(msg.Value))
					} else {
						// colored UI will have invisible chars output
						outmsg = fmt.Sprintf("%s/%d %s k:%s, v:%s",
							msg.Topic, msg.Partition,
							gofmt.Comma(msg.Offset), string(msg.Key), string(msg.Value))
					}
				}

				if outmsg != "" {
					if this.beep {
						outmsg += "\a"
					}

					this.Ui.Output(outmsg)
				}
			}

			total++
			bytesN += int64(len(msg.Value))

			if this.limit > 0 && total >= this.limit {
				break LOOP
			}
			if this.lastN > 0 && total >= int(this.lastN) {
				break LOOP
			}

		}
	}

	return
}

func (this *Peek) consumeCluster(zkcluster *zk.ZkCluster, topicPattern string,
	partitionId int, msgChan chan *sarama.ConsumerMessage) {
	brokerList := zkcluster.BrokerList()
	if len(brokerList) == 0 {
		return
	}
	kfk, err := sarama.NewClient(brokerList, sarama.NewConfig())
	if err != nil {
		this.Ui.Output(err.Error())
		return
	}
	//defer kfk.Close() // FIXME how to close it

	topics, err := kfk.Topics()
	if err != nil {
		this.Ui.Output(err.Error())
		return
	}

	for _, t := range topics {
		if patternMatched(t, topicPattern) {
			go this.simpleConsumeTopic(zkcluster, kfk, t, int32(partitionId), msgChan)
		}
	}

}

func (this *Peek) simpleConsumeTopic(zkcluster *zk.ZkCluster, kfk sarama.Client, topic string, partitionId int32,
	msgCh chan *sarama.ConsumerMessage) {
	consumer, err := sarama.NewConsumerFromClient(kfk)
	if err != nil {
		panic(err)
	}
	defer consumer.Close()

	if partitionId == -1 {
		// all partitions
		partitions, err := kfk.Partitions(topic)
		if err != nil {
			panic(err)
		}

		for _, p := range partitions {
			offset := this.offset
			if this.lastN > 0 {
				latestOffset, err := kfk.GetOffset(topic, p, sarama.OffsetNewest)
				swallow(err)

				oldestOffset, err := kfk.GetOffset(topic, p, sarama.OffsetOldest)
				swallow(err)

				offset = latestOffset - this.lastN
				if offset < oldestOffset {
					offset = oldestOffset
				}

				if offset == 0 {
					// no message in store
					return
				}
			}

			go this.consumePartition(zkcluster, kfk, consumer, topic, p, msgCh, offset)
		}

	} else {
		offset := this.offset
		if this.lastN > 0 {
			latestOffset, err := kfk.GetOffset(topic, partitionId, sarama.OffsetNewest)
			swallow(err)
			offset = latestOffset - this.lastN
			if offset < 0 {
				offset = sarama.OffsetOldest
			}
		}
		this.consumePartition(zkcluster, kfk, consumer, topic, partitionId, msgCh, offset)
	}

}

func (this *Peek) consumePartition(zkcluster *zk.ZkCluster, kfk sarama.Client, consumer sarama.Consumer,
	topic string, partitionId int32, msgCh chan *sarama.ConsumerMessage, offset int64) {
	p, err := consumer.ConsumePartition(topic, partitionId, offset)
	if err != nil {
		this.Ui.Error(fmt.Sprintf("%s %s/%d: offset=%d %v", zkcluster.Name(), topic, partitionId, offset, err))
		return
	}
	defer p.Close()

	n := int64(0)
	for {
		select {
		case <-this.quit:
			return

		case msg := <-p.Messages():
			msgCh <- msg

			n++
			if this.lastN > 0 && n >= this.lastN {
				return
			}
		}
	}
}

func (*Peek) Synopsis() string {
	return "Peek kafka cluster messages ongoing from any offset"
}

func (this *Peek) Help() string {
	help := fmt.Sprintf(`
Usage: %s peek [options]

    %s

Options:

    -z zone
      Default %s

    -c cluster

    -t topic pattern
   
    -p partition id
      -1 will peek all partitions of a topic

    -beep
      Make a beep sound for each message

    -pretty
      Pretty print the json message body

    -col json column name
      Will json decode message and extract specified column value only

    -last n
      Peek the most recent N messages

    -offset message offset value
      -1 OffsetNewest, -2 OffsetOldest. 
      You can specify your own offset.
      Default -1(OffsetNewest)

    -n count
      Limit how many messages to consume

    -d duration
      Limit how long to keep peeking
      e,g. -d 5m

    -body
      Only display message body

    -now
      Iterate the stream till now

    -s
      Silence mode, only display statastics instead of message content

    -color
      Enable colorized output
`, this.Cmd, this.Synopsis(), ctx.ZkDefaultZone())
	return strings.TrimSpace(help)
}
