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
		curLogDirss  int
		curConsumers int
	)
	cmdFlags := flag.NewFlagSet("logdirs", flag.ContinueOnError)
	cmdFlags.Usage = func() { this.Ui.Output(this.Help()) }
	cmdFlags.IntVar(&curLogDirss, "p", 17, "partitions count")
	cmdFlags.IntVar(&curConsumers, "c", 4, "consumers count")
	if err := cmdFlags.Parse(args); err != nil {
		return 1
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

    -p partitions

    -c consumer instances

`, this.Cmd)
	return strings.TrimSpace(help)
}
