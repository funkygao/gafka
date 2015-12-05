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
	fmt.Println(color.Cyan("checking zookeepeer\n%s", strings.Repeat("-", 80)))
	cmd = &Zookeeper{
		Ui:  this.Ui,
		Cmd: this.Cmd,
	}
	cmd.Run(append(args, "-c", "srvr"))

	fmt.Println(color.Cyan("checking offline brokers\n%s", strings.Repeat("-", 80)))
	cmd = &Brokers{
		Ui:  this.Ui,
		Cmd: this.Cmd,
	}
	cmd.Run(append(args, "-stale"))

	fmt.Println(color.Cyan("checking under replicated brokers\n%s", strings.Repeat("-", 80)))
	cmd = &UnderReplicated{
		Ui:  this.Ui,
		Cmd: this.Cmd,
	}
	cmd.Run(args)

	fmt.Println(color.Cyan("checking controllers\n%s", strings.Repeat("-", 80)))
	cmd = &Controllers{
		Ui:  this.Ui,
		Cmd: this.Cmd,
	}
	cmd.Run(args)

	fmt.Println(color.Cyan("checking problematic lag consumers\n%s", strings.Repeat("-", 80)))
	cmd = &Lags{
		Ui:  this.Ui,
		Cmd: this.Cmd,
	}
	cmd.Run(append(args, "-p"))

	return
}

func (*Checkup) Synopsis() string {
	return "Health checkup of zookeepers and brokers"
}

func (this *Checkup) Help() string {
	help := fmt.Sprintf(`
Usage: %s [options]

    Health checkup of zookeepers and brokers

Options:

    -z zone

    -c cluster name

`, this.Cmd)
	return strings.TrimSpace(help)
}
