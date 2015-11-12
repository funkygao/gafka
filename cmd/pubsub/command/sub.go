package command

import (
	"flag"
	"strings"

	"github.com/Shopify/sarama"
	"github.com/funkygao/gocli"
	"github.com/funkygao/golib/color"
	log "github.com/funkygao/log4go"
)

type Sub struct {
	Ui cli.Ui
}

func (this *Sub) Run(args []string) (exitCode int) {
	var (
		id   string
		step int
	)
	cmdFlags := flag.NewFlagSet("sub", flag.ContinueOnError)
	cmdFlags.Usage = func() { this.Ui.Output(this.Help()) }
	cmdFlags.StringVar(&id, "id", "", "")
	cmdFlags.IntVar(&step, "step", 10000, "")
	if err := cmdFlags.Parse(args); err != nil {
		return 1
	}

	if id == "" {
		this.Ui.Error(color.Red("-id is required"))
		this.Ui.Error(this.Help())
		return 2
	}

	zk := NewZk(DefaultConfig(id, ZkAddr))
	for _, inbox := range zk.Inboxes() {
		log.Info("subscribed my[%s] inbox: %s", id, inbox)
		go this.consumeTopic(id, inbox, step)
	}

	select {}

	return

}

func (this *Sub) consumeTopic(app string, inbox string, step int) {
	kfk, err := sarama.NewClient(KafkaBrokerList, sarama.NewConfig())
	if err != nil {
		panic(err)
	}
	defer kfk.Close()

	consumer, err := sarama.NewConsumerFromClient(kfk)
	if err != nil {
		panic(err)
	}
	defer consumer.Close()

	topic := KafkaInboxTopic(app, inbox)
	p, _ := consumer.ConsumePartition(topic, 0, sarama.OffsetNewest)
	defer p.Close()

	var i int64 = 0
	for {
		select {
		case msg := <-p.Messages():
			i++
			if i%int64(step) == 0 {
				this.Ui.Output(color.Green("inbox:%s consumed %d messages <- %s",
					inbox, i,
					string(msg.Value)))
			}
		}
	}

}

func (*Sub) Synopsis() string {
	return "Receive messages from my inbox"
}

func (*Sub) Help() string {
	help := `
Usage: pubsub sub -id appId [options]

	Receive messages from my inbox

Options:
  
  -step n
`
	return strings.TrimSpace(help)
}
