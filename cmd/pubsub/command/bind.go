package command

import (
	"flag"
	"strings"

	"github.com/funkygao/gocli"
)

type Bind struct {
	Ui cli.Ui
}

func (this *Bind) Run(args []string) (exitCode int) {
	var (
		id       string
		list     bool
		add      bool
		from, to string
	)
	cmdFlags := flag.NewFlagSet("bind", flag.ContinueOnError)
	cmdFlags.Usage = func() { this.Ui.Output(this.Help()) }
	cmdFlags.StringVar(&id, "id", "", "")
	cmdFlags.StringVar(&to, "to", "", "")
	cmdFlags.StringVar(&from, "from", "", "")
	cmdFlags.BoolVar(&add, "add", false, "")
	cmdFlags.BoolVar(&list, "list", true, "")
	if err := cmdFlags.Parse(args); err != nil {
		return 1
	}

	if id == "" {
		this.Ui.Output("-id is required")
		this.Ui.Output(this.Help())
		return 2
	}

	//zk := NewZk(DefaultConfig(id, zkAddr))

	return

}

func (*Bind) Synopsis() string {
	return "Bind somebody's outbox to my inbox"
}

func (*Bind) Help() string {
	help := `
Usage: pubsub bind -id appId [options]

	Bind somebody's outbox to my inbox

Options:
  
  -list 
  	List current bindings.

  -add
  	Define a new binding.

  -from topic

  -to topic
`
	return strings.TrimSpace(help)
}
