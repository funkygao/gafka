package command

import (
	"flag"
	"fmt"
	"strings"
	"time"

	"github.com/Shopify/sarama"
	"github.com/funkygao/gocli"
	"github.com/funkygao/golib/color"
	"github.com/funkygao/golib/gofmt"
)

type Pub struct {
	Ui cli.Ui
}

func (this *Pub) Run(args []string) (exitCode int) {
	var (
		id      string
		console bool
		stress  bool
		size    int
		topic   string
		step    int
	)
	cmdFlags := flag.NewFlagSet("pub", flag.ContinueOnError)
	cmdFlags.Usage = func() { this.Ui.Output(this.Help()) }
	cmdFlags.StringVar(&id, "id", "", "")
	cmdFlags.StringVar(&topic, "topic", "", "")
	cmdFlags.BoolVar(&console, "console", false, "")
	cmdFlags.BoolVar(&stress, "stress", true, "")
	cmdFlags.IntVar(&size, "size", 500, "")
	cmdFlags.IntVar(&step, "step", 10000, "")
	if err := cmdFlags.Parse(args); err != nil {
		return 1
	}

	if id == "" {
		this.Ui.Error(color.Red("-id is required"))
		this.Ui.Error(this.Help())
		return 2
	}

	if topic == "" {
		this.Ui.Error(color.Red("-topic is required"))
		this.Ui.Error(this.Help())
		return 2
	}

	zk := NewZk(DefaultConfig(id, ZkAddr))
	zk.EnsureOutboxExists(topic)

	if stress {
		cf := sarama.NewConfig()
		cf.Producer.RequiredAcks = sarama.WaitForLocal
		cf.Producer.Retry.Max = 3
		producer, err := sarama.NewSyncProducer(KafkaBrokerList, cf)
		if err != nil {
			panic(err)
		}
		defer producer.Close()

		msgBody := strings.Repeat("X", size)
		var msg string
		var n int64 = 0
		for {
			msg = fmt.Sprintf("%d:[%s]%s", n, id, msgBody)
			producer.SendMessage(&sarama.ProducerMessage{
				Topic: KafkaOutboxTopic(id, topic),
				Value: sarama.StringEncoder(msg),
			})
			if false {
				time.Sleep(time.Second * 5)
			}

			n++
			if n%int64(step) == 0 {
				this.Ui.Output(color.Green("%s msgs written, current %s",
					gofmt.Comma(n), msg))
			}
		}

		return
	}

	// TODO console

	return

}

func (*Pub) Synopsis() string {
	return "Publish to my outbox"
}

func (*Pub) Help() string {
	help := `
Usage: pubsub pub -id appId [options]

Options:

  -topic name
  
  -console

  -stress

  -size size
   	Under stress mode, specify each msg size.

  -step 
  	Progress bar interval.
`
	return strings.TrimSpace(help)
}
