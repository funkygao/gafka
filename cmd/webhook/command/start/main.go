package start

import (
	"flag"
	"net"
	"net/http"
	"os"
	"syscall"
	"time"

	"github.com/funkygao/gafka/ctx"
	"github.com/funkygao/golib/signal"
	log "github.com/funkygao/log4go"
)

func (this *Start) Run(args []string) (exitCode int) {
	cmdFlags := flag.NewFlagSet("start", flag.ContinueOnError)
	cmdFlags.Usage = func() { this.Ui.Output(this.Help()) }
	cmdFlags.StringVar(&this.logfile, "log", "stdout", "")
	cmdFlags.StringVar(&this.zone, "z", ctx.ZkDefaultZone(), "")
	cmdFlags.BoolVar(&this.debug, "debug", false, "")
	if err := cmdFlags.Parse(args); err != nil {
		return 1
	}

	this.starting = true
	this.quitCh = make(chan struct{})
	timeout := time.Second * 4
	this.callbackConn = &http.Client{
		Timeout: timeout,
		Transport: &http.Transport{
			MaxIdleConnsPerHost: 5,
			Proxy:               http.ProxyFromEnvironment,
			Dial: (&net.Dialer{
				Timeout: timeout,
			}).Dial,
			DisableKeepAlives:     false, // enable http conn reuse
			ResponseHeaderTimeout: timeout,
			TLSHandshakeTimeout:   timeout,
		},
	}

	signal.RegisterSignalsHandler(func(sig os.Signal) {
		log.Info("got signal %s", sig)
		this.shutdown()
		log.Info("shutdown complete")
	}, syscall.SIGINT, syscall.SIGTERM)

	this.main()

	return
}

func (this *Start) main() {
	this.runManager()

}

func (this *Start) shutdown() {
	close(this.quitCh)
}
