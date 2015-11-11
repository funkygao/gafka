package command

import (
	"flag"
	"strings"

	"github.com/funkygao/gocli"
	"github.com/funkygao/golib/color"
)

type Outbox struct {
	Ui cli.Ui
}

func (this *Outbox) Run(args []string) (exitCode int) {
	var (
		id    string
		list  bool
		topic string
	)
	cmdFlags := flag.NewFlagSet("bind", flag.ContinueOnError)
	cmdFlags.Usage = func() { this.Ui.Output(this.Help()) }
	cmdFlags.StringVar(&id, "id", "", "")
	cmdFlags.StringVar(&topic, "add", "", "")
	cmdFlags.BoolVar(&list, "list", false, "")
	if err := cmdFlags.Parse(args); err != nil {
		return 1
	}

	if id == "" {
		this.Ui.Error(color.Red("-id is required"))
		this.Ui.Error(this.Help())
		return 2
	}

	zk := NewZk(DefaultConfig(id, ZkAddr))

	if list {
		this.Ui.Output("outboxes:")
		for _, outbox := range zk.Outboxes() {
			this.Ui.Output(color.Green("    %s", outbox))
		}
		return
	}

	// add new outbox
	if topic == "" {
		this.Ui.Error(color.Red("-add required"))
		this.Ui.Output(this.Help())
		return 2
	}

	if err := zk.RegisterOutbox(topic); err != nil {
		this.Ui.Error(color.Red("%v", err))
		return 1
	}
	if err := KafkaCreateTopic(KafkaOutboxTopic(id, topic)); err != nil {
		this.Ui.Error(color.Red("%v", err))
		return 1
	}

	this.Ui.Output(color.Green("outbox:%s added successfully", topic))

	return
}

func (*Outbox) Synopsis() string {
	return "Manipulate my outboxes"
}

func (*Outbox) Help() string {
	help := `
Usage: pubsub outbox -id appId [options]

	Manipulate my outboxes

Options:
  
  -list

  -add topic

`
	return strings.TrimSpace(help)
}
