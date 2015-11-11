package command

import (
	"flag"
	"strings"

	"github.com/funkygao/gocli"
	"github.com/funkygao/golib/color"
)

type Inbox struct {
	Ui cli.Ui
}

func (this *Inbox) Run(args []string) (exitCode int) {
	var (
		id    string
		list  bool
		topic string
	)
	cmdFlags := flag.NewFlagSet("bind", flag.ContinueOnError)
	cmdFlags.Usage = func() { this.Ui.Output(this.Help()) }
	cmdFlags.StringVar(&id, "id", "", "")
	cmdFlags.StringVar(&topic, "add", "", "")
	cmdFlags.BoolVar(&list, "list", true, "")
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
		this.Ui.Output("inboxes:")
		for _, inbox := range zk.Inboxes() {
			this.Ui.Output(color.Green("    %s", inbox))
		}
		return
	}

	// add new inbox
	if topic == "" {
		this.Ui.Error(color.Red("-add required"))
		this.Ui.Output(this.Help())
		return 2
	}

	if err := zk.RegisterInbox(topic); err != nil {
		this.Ui.Error(color.Red("%v", err))
		return 1
	}
	if err := KafkaCreateTopic(KafkaInboxTopic(id, topic)); err != nil {
		this.Ui.Error(color.Red("%v", err))
		return 1
	}

	this.Ui.Output(color.Green("inbox:%s added successfully", topic))

	return
}

func (*Inbox) Synopsis() string {
	return "Manipulate my inboxes"
}

func (*Inbox) Help() string {
	help := `
Usage: pubsub inbox -id appId [options]

	Manipulate my inboxes

Options:
  
  -list

  -add topic

`
	return strings.TrimSpace(help)
}
