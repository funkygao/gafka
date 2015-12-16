package command

import (
	"fmt"
	"strings"

	"github.com/funkygao/gocli"
	"github.com/funkygao/golib/color"
)

type Checkup struct {
	Ui  cli.Ui
	Cmd string
}

func (this *Checkup) Run(args []string) (exitCode int) {
	var cmd cli.Command
	this.Ui.Output(color.Cyan("checking zookeepeer\n%s", strings.Repeat("-", 80)))
	cmd = &Zookeeper{
		Ui:  this.Ui,
		Cmd: this.Cmd,
	}
	cmd.Run(append(args, "-c", "srvr"))
	this.Ui.Output("")

	this.Ui.Output(color.Cyan("checking registered brokers are alive\n%s", strings.Repeat("-", 80)))
	cmd = &Clusters{
		Ui:  this.Ui,
		Cmd: this.Cmd,
	}
	cmd.Run(append(args, "-verify"))
	this.Ui.Output("")

	this.Ui.Output(color.Cyan("checking offline brokers\n%s", strings.Repeat("-", 80)))
	cmd = &Brokers{
		Ui:  this.Ui,
		Cmd: this.Cmd,
	}
	cmd.Run(append(args, "-stale"))
	this.Ui.Output("")

	this.Ui.Output(color.Cyan("checking under replicated brokers\n%s", strings.Repeat("-", 80)))
	cmd = &UnderReplicated{
		Ui:  this.Ui,
		Cmd: this.Cmd,
	}
	cmd.Run(args)
	this.Ui.Output("")

	this.Ui.Output(color.Cyan("checking controllers\n%s", strings.Repeat("-", 80)))
	cmd = &Controllers{
		Ui:  this.Ui,
		Cmd: this.Cmd,
	}
	cmd.Run(args)
	this.Ui.Output("")

	this.Ui.Output(color.Cyan("checking problematic lag consumers\n%s", strings.Repeat("-", 80)))
	cmd = &Lags{
		Ui:  this.Ui,
		Cmd: this.Cmd,
	}
	cmd.Run(append(args, "-p"))

	this.Ui.Output("Did you find something wrong?")

	return
}

func (*Checkup) Synopsis() string {
	return "Health checkup of kafka runtime"
}

func (this *Checkup) Help() string {
	help := fmt.Sprintf(`
Usage: %s [options]

    Health checkup of kafka runtime

Options:

    -z zone

    -c cluster name

`, this.Cmd)
	return strings.TrimSpace(help)
}
