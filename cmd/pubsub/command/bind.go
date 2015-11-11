package command

import (
	"flag"
	"strings"

	"github.com/funkygao/gocli"
	"github.com/funkygao/golib/color"
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
	cmdFlags.BoolVar(&list, "list", false, "")
	if err := cmdFlags.Parse(args); err != nil {
		return 1
	}

	if id == "" {
		this.Ui.Error(color.Red("-id is required"))
		this.Ui.Error(this.Help())
		return 2
	}

	if strings.Count(from, ":") != 1 {
		this.Ui.Error(color.Red("-from must be appId:outbox format"))
		return 2
	}

	zk := NewZk(DefaultConfig(id, ZkAddr))
	binding, err := zk.Binding()
	if err != nil {
		this.Ui.Error(color.Red("%v", err))
		return 2
	}

	if list {
		for k, v := range binding {
			this.Ui.Output(color.Green("%s -> %s", k, v))
		}

		return
	}

	// add bindings TODO multi-multi
	// TODO ensure from and to exists

	binding[from] = to
	if err = zk.Bind(binding); err != nil {
		this.Ui.Error(color.Red("%v", err))
		return 1
	}

	this.Ui.Output(color.Green("bound successfully"))

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

  -del
  	Unbind a binding(TODO).

  -from appId:outbox

  -to inbox
`
	return strings.TrimSpace(help)
}
