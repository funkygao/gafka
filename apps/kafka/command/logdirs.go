package command

import (
	"flag"
	"fmt"
	"strings"

	"github.com/funkygao/gocli"
)

type LogDirs struct {
	Ui  cli.Ui
	Cmd string
}

func (this *LogDirs) Run(args []string) (exitCode int) {
	var (
		dirN int
	)
	cmdFlags := flag.NewFlagSet("logdirs", flag.ContinueOnError)
	cmdFlags.Usage = func() { this.Ui.Output(this.Help()) }
	cmdFlags.IntVar(&dirN, "d", 12, "log.dirs count")
	if err := cmdFlags.Parse(args); err != nil {
		return 1
	}

	this.Ui.Output("nextLogDir...")
	if dirN == 1 {
		this.Ui.Output(fmt.Sprintf("dir/0"))
		return
	} else {
		this.Ui.Output(`
Scan each log.dirs and choose the directory with the least partitions in it			
			`)
	}

	return
}

func (*LogDirs) Synopsis() string {
	return "log.dirs balance algorithm"
}

func (this *LogDirs) Help() string {
	help := fmt.Sprintf(`
Usage: %s logdirs [options]

    log.dirs balance algorithm

    -d log.dirs count

`, this.Cmd)
	return strings.TrimSpace(help)
}
