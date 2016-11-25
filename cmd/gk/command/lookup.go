package command

import (
	"flag"
	"fmt"
	"net"
	"strings"

	"github.com/funkygao/gafka/ctx"
	"github.com/funkygao/gocli"
)

type Lookup struct {
	Ui  cli.Ui
	Cmd string
}

func (this *Lookup) Run(args []string) (exitCode int) {
	cmdFlags := flag.NewFlagSet("lookup", flag.ContinueOnError)
	cmdFlags.Usage = func() { this.Ui.Output(this.Help()) }
	if err := cmdFlags.Parse(args); err != nil {
		return 1
	}

	if len(args) == 0 {
		this.Ui.Error("missing ip:port")
		return 2
	}

	ipPort := args[len(args)-1]
	ip, port, err := net.SplitHostPort(ipPort)
	swallow(err)

	for _, host := range ctx.LookupIpPort(ip, port) {
		this.Ui.Output(host)
	}

	return
}

func (this *Lookup) Synopsis() string {
	return "Internal reverse DNS lookup utility"
}

func (this *Lookup) Help() string {
	help := fmt.Sprintf(`
Usage: %s lookup [ip]:[port]

    %s

    e,g 
      gk lookup 1.2.3.3
      gk lookup :10008

`, this.Cmd, this.Synopsis())
	return strings.TrimSpace(help)
}
