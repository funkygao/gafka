package command

import (
	"flag"
	"fmt"
	"io/ioutil"
	"strconv"
	"strings"
	"time"

	"github.com/funkygao/gocli"
	"github.com/funkygao/golib/gofmt"
)

type Pps struct {
	Ui  cli.Ui
	Cmd string

	interval time.Duration
}

func (this *Pps) Run(args []string) (exitCode int) {
	cmdFlags := flag.NewFlagSet("pps", flag.ContinueOnError)
	cmdFlags.Usage = func() { this.Ui.Output(this.Help()) }
	cmdFlags.DurationVar(&this.interval, "sleep", time.Second*2, "")
	if err := cmdFlags.Parse(args); err != nil {
		return 1
	}

	if len(args) == 0 {
		this.Ui.Error("missing nic")
		return 2
	}

	nic := args[len(args)-1]
	this.showPps(nic)

	return
}

func (this *Pps) showPps(nic string) {
	tx := fmt.Sprintf("/sys/class/net/%s/statistics/tx_packets", nic)
	rx := fmt.Sprintf("/sys/class/net/%s/statistics/rx_packets", nic)

	var lastTx, lastRx int64
	s := int64(this.interval.Seconds())
	for {
		brx, err := ioutil.ReadFile(rx)
		swallow(err)
		btx, err := ioutil.ReadFile(tx)
		swallow(err)

		rxN, err := strconv.ParseInt(strings.TrimSpace(string(brx)), 10, 64)
		swallow(err)
		txN, err := strconv.ParseInt(strings.TrimSpace(string(btx)), 10, 64)
		swallow(err)

		if lastRx != 0 && lastTx != 0 {
			rxPps := (rxN - lastRx) / s
			txPps := (txN - lastTx) / s
			sumPps := rxPps + txPps

			this.Ui.Output(fmt.Sprintf("%10s rx:%-8s tx:%-8s sum:%-8s",
				nic, gofmt.Comma(rxPps), gofmt.Comma(txPps), gofmt.Comma(sumPps)))
		}

		lastRx = rxN
		lastTx = txN

		time.Sleep(this.interval)
	}
}

func (*Pps) Synopsis() string {
	return "Show PacketPerSecond of a NIC on Linux"
}

func (this *Pps) Help() string {
	help := fmt.Sprintf(`
Usage: %s pps [option] nic

    %s

Options:

    -sleep duration    

`, this.Cmd, this.Synopsis())
	return strings.TrimSpace(help)
}
