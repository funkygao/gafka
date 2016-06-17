package command

import (
	"bufio"
	"flag"
	"fmt"
	"io"
	"os"
	"strconv"
	"strings"

	"github.com/funkygao/gocli"
	"github.com/funkygao/golib/gofmt"
)

type Histogram struct {
	Ui  cli.Ui
	Cmd string

	offsetFile  string
	networkFile string // consul exec ifconfig bond0 | grep 'RX bytes' | awk '{print $1,$3,$7}' | sort
}

func (this *Histogram) Run(args []string) (exitCode int) {
	cmdFlags := flag.NewFlagSet("histogram", flag.ContinueOnError)
	cmdFlags.Usage = func() { this.Ui.Output(this.Help()) }
	cmdFlags.StringVar(&this.offsetFile, "f", "/var/wd/topics_offsets/offsets", "")
	cmdFlags.StringVar(&this.networkFile, "n", "/var/wd/topics_offsets/network", "")
	if err := cmdFlags.Parse(args); err != nil {
		return 1
	}

	this.showOffsetGrowth()
	this.showNetworkGrowth()

	return
}

func (this *Histogram) showOffsetGrowth() {
	f, err := os.OpenFile(this.offsetFile, os.O_RDONLY, 0660)
	swallow(err)
	defer f.Close()

	r := bufio.NewReader(f)
	var (
		lastN = int64(0)
		tm    string
	)

	for {
		line, err := r.ReadString('\n')
		if err == io.EOF {
			break
		}

		line = strings.TrimSpace(line)

		if !strings.Contains(line, "CUM Messages") {
			// time info: Thu Jun 16 22:45:01 CST 2016
			tm = line
		} else {
			// offset:            -CUM Messages- 255,705,684,384
			n := strings.Split(line, "-CUM Messages-")[1]
			n = strings.Replace(n, ",", "", -1)
			n = strings.TrimSpace(n)
			offset, err := strconv.ParseInt(n, 10, 64)
			swallow(err)
			if lastN > 0 {
				this.Ui.Output(fmt.Sprintf("%55s Message+ %15s/%s", tm,
					gofmt.Comma(offset-lastN), gofmt.Comma(lastN)))
			}

			lastN = offset
		}

	}
}

func (this *Histogram) showNetworkGrowth() {
	f, err := os.OpenFile(this.networkFile, os.O_RDONLY, 0660)
	swallow(err)
	defer f.Close()

	r := bufio.NewReader(f)
	var (
		lastRx           = int64(0)
		lastTx           = int64(0)
		rxTotal, txTotal int64
		tm               string
	)

	for {
		// CDM1C01-209018015: bytes:98975866482403 bytes:115679008715688
		line, err := r.ReadString('\n')
		if err == io.EOF {
			if lastRx > 0 {
				this.Ui.Output(fmt.Sprintf("%55s    RX+:%10s/%-10s TX+:%10s/%-10s",
					tm, gofmt.ByteSize(rxTotal-lastRx), gofmt.ByteSize(lastRx),
					gofmt.ByteSize(txTotal-lastTx), gofmt.ByteSize(lastTx)))
			}
			break
		}

		line = strings.TrimSpace(line)
		if !strings.Contains(line, "bytes") {
			// time info: Thu Jun 16 22:45:01 CST 2016

			if lastRx > 0 {
				this.Ui.Output(fmt.Sprintf("%55s    RX+:%10s/%-10s TX+:%10s/%-10s",
					tm, gofmt.ByteSize(rxTotal-lastRx), gofmt.ByteSize(lastRx),
					gofmt.ByteSize(txTotal-lastTx), gofmt.ByteSize(lastTx)))
			}

			tm = line

			lastRx = rxTotal
			lastTx = txTotal
			rxTotal = 0
			txTotal = 0
		} else {
			// CDM1C01-209018015: bytes:98975866482403 bytes:115679008715688
			parts := strings.Split(line, " ")
			//host := strings.TrimRight(parts[0], ":")
			rxBytes := strings.Split(parts[1], ":")[1]
			txBytes := strings.Split(parts[2], ":")[1]

			n, err := strconv.ParseInt(rxBytes, 10, 64)
			swallow(err)
			rxTotal += n

			n, err = strconv.ParseInt(txBytes, 10, 64)
			swallow(err)
			txTotal += n
		}
	}
}

func (*Histogram) Synopsis() string {
	return "Histogram of kafka produced messages every hour"
}

func (this *Histogram) Help() string {
	help := fmt.Sprintf(`
	Usage: %s histogram [options]

	    Histogram of kafka produced messages every hour

	Options:

	    -f offset file

	    -n network volumn file

	`, this.Cmd)
	return strings.TrimSpace(help)
}
