package start

import (
	"fmt"
	"net/http"
	"strings"

	"github.com/funkygao/gafka/ctx"
	"github.com/funkygao/gocli"
)

type Start struct {
	Ui  cli.Ui
	Cmd string

	debug    bool
	zone     string
	logfile  string
	starting bool
	quitCh   chan struct{}

	callbackConn *http.Client
}

func (this *Start) Synopsis() string {
	return "Start webhook worker on localhost"
}

func (this *Start) Help() string {
	help := fmt.Sprintf(`
Usage: %s start [options]

    Start webhook worker on localhost

Options:

    -z zone
      Default %s

    -log log file
      Default stdout

`, this.Cmd, ctx.ZkDefaultZone())
	return strings.TrimSpace(help)
}
