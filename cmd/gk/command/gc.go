package command

import (
	"flag"
	"fmt"
	"strings"

	"github.com/funkygao/gocli"
)

type GC struct {
	Ui  cli.Ui
	Cmd string
}

func (this *GC) Run(args []string) (exitCode int) {
	cmdFlags := flag.NewFlagSet("gc", flag.ContinueOnError)
	cmdFlags.Usage = func() { this.Ui.Output(this.Help()) }
	if err := cmdFlags.Parse(args); err != nil {
		return 1
	}

	// 2017-06-19T07:44:51.169+0800: 23997477.962: [GC2017-06-19T07:44:51.169+0800: 23997477.962: [ParNew: 1125653K->6816K(1258304K), 0.0084040 secs] 1335820K->216983K(4054528K), 0.0084870 secs] [Times: user=0.10 sys=0.00, real=0.01 secs]
	// 2017-06-19T07:47:29.468+0800: 18518585.280: [GC [PSYoungGen: 14656K->768K(14848K)] 59237K->45445K(73728K), 0.0028030 secs] [Times: user=0.01 sys=0.01, real=0.01 secs]

	return
}

func (*GC) Synopsis() string {
	return "Java GC log analyzer and visualizer"
}

func (this *GC) Help() string {
	help := fmt.Sprintf(`
Usage: %s gc <gc.log>

    %s

`, this.Cmd, this.Synopsis())
	return strings.TrimSpace(help)
}
