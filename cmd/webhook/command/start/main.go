package start

import (
	"flag"
	"fmt"
	"os"
	"strings"
	"syscall"

	"github.com/funkygao/gafka/ctx"
	"github.com/funkygao/gocli"
	"github.com/funkygao/golib/signal"
	log "github.com/funkygao/log4go"
)

type Start struct {
	Ui  cli.Ui
	Cmd string

	zone     string
	logfile  string
	starting bool
	quitCh   chan struct{}
}

func (this *Start) Run(args []string) (exitCode int) {
	cmdFlags := flag.NewFlagSet("start", flag.ContinueOnError)
	cmdFlags.Usage = func() { this.Ui.Output(this.Help()) }
	cmdFlags.StringVar(&this.logfile, "log", "stdout", "")
	cmdFlags.StringVar(&this.zone, "z", ctx.ZkDefaultZone(), "")
	if err := cmdFlags.Parse(args); err != nil {
		return 1
	}

	this.starting = true
	this.quitCh = make(chan struct{})
	signal.RegisterSignalsHandler(func(sig os.Signal) {
		log.Info("got signal %s", sig)
		this.shutdown()
		log.Info("shutdown complete")
	}, syscall.SIGINT, syscall.SIGTERM)

	this.main()

	return
}

func (this *Start) main() {

}

func (this *Start) shutdown() {
	close(this.quitCh)
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
