package command

import (
	"flag"
	"fmt"
	"log"
	"os"
	"runtime"
	"strings"

	"github.com/funkygao/gocli"
	"github.com/tylertreat/comcast/throttler"
)

type Chaos struct {
	Ui  cli.Ui
	Cmd string
}

func (this *Chaos) Run(args []string) (exitCode int) {
	var (
		ipv6, dryrun        bool
		device, mode, ports string
		loss                float64
		latency             int
	)
	cmdFlags := flag.NewFlagSet("chaos", flag.ContinueOnError)
	cmdFlags.Usage = func() { this.Ui.Output(this.Help()) }
	cmdFlags.StringVar(&device, "i", "", "")
	cmdFlags.StringVar(&mode, "mode", throttler.Start, "")
	cmdFlags.IntVar(&latency, "latency", -1, "")
	cmdFlags.StringVar(&ports, "ports", "", "")
	cmdFlags.Float64Var(&loss, "loss", 0, "")
	cmdFlags.BoolVar(&dryrun, "dryrun", false, "")
	cmdFlags.BoolVar(&ipv6, "ipv6", false, "")
	if err := cmdFlags.Parse(args); err != nil {
		return 1
	}

	log.SetOutput(os.Stdout)
	throttler.Run(&throttler.Config{
		Device:      device,
		Mode:        mode,
		Latency:     latency,
		DryRun:      dryrun,
		IPv6:        ipv6,
		TargetPorts: strings.Split(ports, ","),
		PacketLoss:  loss,
	})

	return
}

func (*Chaos) Synopsis() string {
	return "Chaos monkey which simulates common network problems"
}

func (this *Chaos) Help() string {
	var underlying string
	switch runtime.GOOS {
	case "darwin", "freebsd":
		underlying = "ipfw"

	case "linux":
		underlying = "tc"
	}

	help := fmt.Sprintf(`
Usage: %s chaos [options]

    %s

    A wrapper of %s

Options:

    -mode <start|stop>

    -i eth0

    -latency n in ms

    -loss 0.01
     1%% packet loss.

    -ports comma seperated port list

    -dryrun

    -ipv6

`, this.Cmd, this.Synopsis(), underlying)
	return strings.TrimSpace(help)
}
