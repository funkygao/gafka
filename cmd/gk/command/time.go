package command

import (
	"flag"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/funkygao/gocli"
)

type Time struct {
	Ui  cli.Ui
	Cmd string
}

func (this *Time) Run(args []string) (exitCode int) {
	cmdFlags := flag.NewFlagSet("time", flag.ContinueOnError)
	cmdFlags.Usage = func() { this.Ui.Output(this.Help()) }
	if err := cmdFlags.Parse(args); err != nil {
		return 2
	}

	if len(args) == 0 {
		this.Ui.Error("missing <timestamp>")
		return 2
	}

	timestamp := args[len(args)-1]
	this.Ui.Output(fmt.Sprintf("%s %s", timestamp, this.timestampToTime(timestamp)))

	return
}

func (this *Time) timestampToTime(t string) time.Time {
	i, err := strconv.ParseInt(t, 10, 64)
	swallow(err)

	if i > 133761237100 {
		// in ms
		i = i / 1000
	}
	return time.Unix(i, 0)
}

func (*Time) Synopsis() string {
	return "Parse Unix timestamp to human readable time"
}

func (this *Time) Help() string {
	help := fmt.Sprintf(`
Usage: %s time <timestamp>

    %s

`, this.Cmd, this.Synopsis())
	return strings.TrimSpace(help)
}
