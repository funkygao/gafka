package command

import (
	"flag"
	"fmt"
	"strings"

	"github.com/funkygao/gafka/ctx"
	"github.com/funkygao/gafka/zk"
	"github.com/funkygao/gocli"
	"github.com/funkygao/golib/color"
	"github.com/funkygao/golib/gofmt"
)

type Kguard struct {
	Ui  cli.Ui
	Cmd string

	zone    string
	longFmt bool
}

func (this *Kguard) Run(args []string) (exitCode int) {
	cmdFlags := flag.NewFlagSet("kguard", flag.ContinueOnError)
	cmdFlags.Usage = func() { this.Ui.Output(this.Help()) }
	cmdFlags.StringVar(&this.zone, "z", ctx.ZkDefaultZone(), "")
	cmdFlags.BoolVar(&this.longFmt, "l", false, "")
	if err := cmdFlags.Parse(args); err != nil {
		return 2
	}

	zkzone := zk.NewZkZone(zk.DefaultConfig(this.zone, ctx.ZoneZkAddrs(this.zone)))
	kguards, err := zkzone.KguardInfos()
	if err != nil {
		this.Ui.Error(fmt.Sprintf("%s %v", zk.KguardLeaderPath, err.Error()))
		return
	}

	leader := kguards[0]
	this.Ui.Output(fmt.Sprintf("%s(out of %d candidates) up: %s",
		color.Green(leader.Host), leader.Candidates,
		gofmt.PrettySince(leader.Ctime)))

	if this.longFmt {
		this.showStats(leader.Host)
	}

	return
}

func (this *Kguard) showStats(host string) {
	url := fmt.Sprintf("http://%s:10025/metrics", host)
	req := gorequest.New()
	req.Get(url).Set("User-Agent", "gk")
	_, b, errs := req.EndBytes()
	if len(errs) > 0 {
		for _, err := range errs {
			this.Ui.Error(err.Error())
		}
		return
	}

	this.Ui.Output(string(b))
}

func (*Kguard) Synopsis() string {
	return "List online kguard instances"
}

func (this *Kguard) Help() string {
	help := fmt.Sprintf(`
Usage: %s kateway -z zone [options]

    List online kguard instances

Options:

    -l
	  Use a long listing format.

`, this.Cmd)
	return strings.TrimSpace(help)
}
