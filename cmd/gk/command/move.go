package command

import (
	"flag"
	"fmt"
	"path/filepath"
	"strings"

	"github.com/funkygao/gocli"
	gio "github.com/funkygao/golib/io"
)

type Move struct {
	Ui  cli.Ui
	Cmd string

	from, to string
}

func (this *Move) Run(args []string) (exitCode int) {
	cmdFlags := flag.NewFlagSet("move", flag.ContinueOnError)
	cmdFlags.Usage = func() { this.Ui.Output(this.Help()) }
	cmdFlags.StringVar(&this.from, "from", "", "")
	cmdFlags.StringVar(&this.to, "to", "", "")
	if err := cmdFlags.Parse(args); err != nil {
		return 1
	}

	recoveryPointCheckpointFile := "recovery-point-offset-checkpoint"
	highWatermarkFilename := "replication-offset-checkpoint"

	if validateArgs(this, this.Ui).
		require("-from", "-to").
		invalid(args) {
		return 2
	}

	parent := filepath.Dir(this.to)
	if !gio.DirExists(parent) {
		this.Ui.Error(fmt.Sprintf("target %s not exists", this.to))
		return 1
	}

	this.Ui.Warn("shutdown kafka instance before you proceed")
	this.Ui.Output(fmt.Sprintf("skip %s which flush every 1m, %s which flush every 5s",
		recoveryPointCheckpointFile, highWatermarkFilename))
	this.Ui.Info(fmt.Sprintf("mv %s %s", this.from, this.to))

	return
}

func (*Move) Synopsis() string {
	return "Move kafka partition from one dir to another"
}

func (this *Move) Help() string {
	help := fmt.Sprintf(`
Usage: %s move [options]

    %s

    If you encounter high iowait problem, this command can help.
    e,g.
    shutdown kafka process
    gk move -from /data12/kfk_trade/order-0 -to /data5/kfk_trade/order-0

    -from dir

    -to dir

`, this.Cmd, this.Synopsis())
	return strings.TrimSpace(help)
}
