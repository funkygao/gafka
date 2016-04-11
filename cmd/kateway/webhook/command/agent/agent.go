package agent

import (
	"fmt"
	"net/http"
	"strings"

	"github.com/funkygao/gafka/ctx"
	"github.com/funkygao/gocli"
)

type Agent struct {
	Ui  cli.Ui
	Cmd string

	debug    bool
	zone     string
	logfile  string
	starting bool
	quitCh   chan struct{}

	callbackConn *http.Client
}

func (this *Agent) Synopsis() string {
	return "Start webhook agent on localhost"
}

func (this *Agent) Help() string {
	help := fmt.Sprintf(`
Usage: %s agent [options]

    Start webhook agent on localhost

Options:

    -z zone
      Default %s

    -log log file
      Default stdout

`, this.Cmd, ctx.ZkDefaultZone())
	return strings.TrimSpace(help)
}
