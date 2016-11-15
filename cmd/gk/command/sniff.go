package command

import (
	"flag"
	"fmt"
	"strings"

	"github.com/funkygao/gocli"
	"github.com/google/gopacket/pcap"
)

type Sniff struct {
	Ui  cli.Ui
	Cmd string
}

func (this *Sniff) Run(args []string) (exitCode int) {
	var (
		protocol string
	)
	cmdFlags := flag.NewFlagSet("sniff", flag.ContinueOnError)
	cmdFlags.Usage = func() { this.Ui.Output(this.Help()) }
	cmdFlags.StringVar(&protocol, "p", "http", "")
	if err := cmdFlags.Parse(args); err != nil {
		return 1
	}

	devices, err := pcap.FindAllDevs()
	swallow(err)

	for _, device := range devices {
		fmt.Println(device.Name, device.Description)
	}

	return
}

func (this *Sniff) Synopsis() string {
	return fmt.Sprintf("Sniff traffic on a network with libpcap", this.Cmd)
}

func (this *Sniff) Help() string {
	help := fmt.Sprintf(`
Usage: %s sniff [options]

    %s

Options:
   

`, this.Cmd, this.Synopsis())
	return strings.TrimSpace(help)
}
