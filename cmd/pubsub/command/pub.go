package command

import (
	"flag"
	"fmt"
	"strings"

	"github.com/Shopify/sarama"
	"github.com/funkygao/gocli"
	"github.com/funkygao/golib/color"
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
		step    = 1000
	)
	cmdFlags := flag.NewFlagSet("pub", flag.ContinueOnError)
	cmdFlags.Usage = func() { this.Ui.Output(this.Help()) }
	cmdFlags.StringVar(&id, "id", "", "")
	cmdFlags.StringVar(&topic, "topic", "", "")
	cmdFlags.BoolVar(&console, "console", false, "")
	cmdFlags.BoolVar(&stress, "stress", true, "")
	cmdFlags.IntVar(&size, "size", 100, "")
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

	zk := NewZk(DefaultConfig(id, zkAddr))
	zk.EnsureOutboxExists(topic)

	if stress {
		cf := sarama.NewConfig()
		cf.Producer.RequiredAcks = sarama.WaitForLocal
		cf.Producer.Retry.Max = 3
		producer, err := sarama.NewSyncProducer(kafkaBrokerList, cf)
		if err != nil {
			panic(err)
		}
		defer producer.Close()

		msgBody := strings.Repeat("X", size)
		var i int64 = 1
		for {
			producer.SendMessage(&sarama.ProducerMessage{
				Topic: KafkaOutboxTopic(id, topic),
				Value: sarama.StringEncoder(fmt.Sprintf("%d: %s", i, msgBody)),
			})

			i++
			if i%int64(step) == 0 {
				this.Ui.Output(color.Green("%d msgs written", i))
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
`
	return strings.TrimSpace(help)
}
