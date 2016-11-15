package command

import (
	"flag"
	"fmt"
	"strings"
	"time"

	"github.com/funkygao/gocli"
	"github.com/google/gopacket"
	"github.com/google/gopacket/layers"
	"github.com/google/gopacket/pcap"
)

type Sniff struct {
	Ui  cli.Ui
	Cmd string
}

func (this *Sniff) Run(args []string) (exitCode int) {
	var (
		device string
		filter string
		sleep  time.Duration
	)
	cmdFlags := flag.NewFlagSet("sniff", flag.ContinueOnError)
	cmdFlags.Usage = func() { this.Ui.Output(this.Help()) }
	cmdFlags.StringVar(&device, "i", "", "")
	cmdFlags.StringVar(&filter, "f", "", "")
	cmdFlags.DurationVar(&sleep, "s", 0, "")
	if err := cmdFlags.Parse(args); err != nil {
		return 1
	}

	if validateArgs(this, this.Ui).
		require("-i", "-f").
		invalid(args) {
		return 2
	}

	handle, err := pcap.OpenLive(device, 1<<10, false, time.Second*30)
	swallow(err)
	defer handle.Close()

	swallow(handle.SetBPFFilter(filter))

	// Use the handle as a packet source to process all packets
	packetSource := gopacket.NewPacketSource(handle, handle.LinkType())
	for packet := range packetSource.Packets() {
		this.handlePacket(packet)

		if sleep > 0 {
			time.Sleep(sleep)
		}
	}

	return
}

func (this *Sniff) handlePacket(packet gopacket.Packet) {
	ipLayer := packet.Layer(layers.LayerTypeIPv4)
	if ipLayer == nil {
		return
	}
	ip, _ := ipLayer.(*layers.IPv4)

	tcpLayer := packet.Layer(layers.LayerTypeTCP)
	if tcpLayer == nil {
		return
	}
	tcp, _ := tcpLayer.(*layers.TCP)

	applicationLayer := packet.ApplicationLayer()
	if applicationLayer == nil {
		return
	}

	this.Ui.Info(fmt.Sprintf("%s:%s -> %s:%s %dB", ip.SrcIP, tcp.SrcPort, ip.DstIP, tcp.DstPort, len(applicationLayer.Payload())))
	this.Ui.Output(fmt.Sprintf("%s", string(applicationLayer.Payload())))
}

func (this *Sniff) Synopsis() string {
	return fmt.Sprintf("Sniff traffic on a network with libpcap")
}

func (this *Sniff) Help() string {
	help := fmt.Sprintf(`
Usage: %s sniff [options]

    %s

Options:

    -i interface

    -f filter
      e,g. tcp and port 80

    -s sleep duration
      e,g 5ms 1s
   

`, this.Cmd, this.Synopsis())
	return strings.TrimSpace(help)
}
