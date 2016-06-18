package command

import (
	"bufio"
	"flag"
	"fmt"
	"io"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/funkygao/gocli"
	"github.com/funkygao/golib/gofmt"
	"github.com/funkygao/termui"
	"github.com/nsf/termbox-go"
)

/*
#!/bin/sh
date >> /var/wd/topics_offsets/offsets; /usr/bin/gk topics -z prod -l -plain | grep CUM >> /var/wd/topics_offsets/offsets

date >> /var/wd/topics_offsets/network; consul exec ifconfig bond0 | grep 'RX bytes' | awk '{print $1,$3,$7}' | sort >> /var/wd/topics_offsets/network
*/
type Histogram struct {
	Ui  cli.Ui
	Cmd string

	offsetFile  string
	networkFile string // consul exec ifconfig bond0 | grep 'RX bytes' | awk '{print $1,$3,$7}' | sort
	drawMode    bool
}

func (this *Histogram) Run(args []string) (exitCode int) {
	cmdFlags := flag.NewFlagSet("histogram", flag.ContinueOnError)
	cmdFlags.Usage = func() { this.Ui.Output(this.Help()) }
	cmdFlags.BoolVar(&this.drawMode, "d", false, "")
	cmdFlags.StringVar(&this.offsetFile, "f", "/var/wd/topics_offsets/offsets", "")
	cmdFlags.StringVar(&this.networkFile, "n", "/var/wd/topics_offsets/network", "")
	if err := cmdFlags.Parse(args); err != nil {
		return 1
	}

	offsetTs, offsets := this.showOffsetGrowth()
	netTs, rx, tx := this.showNetworkGrowth()

	if this.drawMode {
		this.drawAll(offsetTs, offsets, netTs, rx, tx)
	}

	return
}

func (this *Histogram) showOffsetGrowth() ([]time.Time, []int64) {
	f, err := os.OpenFile(this.offsetFile, os.O_RDONLY, 0660)
	swallow(err)
	defer f.Close()

	ts := make([]time.Time, 0)
	vs := make([]int64, 0)

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
				t, e := time.Parse("Mon Jan 2 15:04:05 MST 2006", tm)
				swallow(e)
				ts = append(ts, t)
				vs = append(vs, offset-lastN)

				this.Ui.Output(fmt.Sprintf("%55s Message+ %15s/%s", tm,
					gofmt.Comma(offset-lastN), gofmt.Comma(lastN)))
			}

			lastN = offset
		}
	}

	return ts, vs
}

func (this *Histogram) showNetworkGrowth() ([]time.Time, []int64, []int64) {
	f, err := os.OpenFile(this.networkFile, os.O_RDONLY, 0660)
	swallow(err)
	defer f.Close()

	ts := make([]time.Time, 0)
	rx := make([]int64, 0)
	tx := make([]int64, 0)

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
				t, e := time.Parse("Mon Jan 2 15:04:05 MST 2006", tm)
				swallow(e)
				ts = append(ts, t)
				rx = append(rx, rxTotal-lastRx)
				tx = append(tx, txTotal-lastTx)

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
				t, e := time.Parse("Mon Jan 2 15:04:05 MST 2006", tm)
				swallow(e)
				ts = append(ts, t)
				rx = append(rx, rxTotal-lastRx)
				tx = append(tx, txTotal-lastTx)

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

	return ts, rx, tx
}

func (this *Histogram) drawAll(offsetTs []time.Time, offsets []int64,
	netTs []time.Time, rx []int64, tx []int64) {
	err := termui.Init()
	swallow(err)
	defer termui.Close()

	termui.UseTheme("helloworld")

	w, h := termbox.Size()

	bc1 := termui.NewBarChart()
	bc1.Border.Label = "Messages Produced/in million"
	data := make([]int, 0)
	for _, off := range offsets {
		data = append(data, int(off/1000000)) // in million
	}
	bclabels := make([]string, 0)
	for _, t := range offsetTs {
		bclabels = append(bclabels, fmt.Sprintf("%02d", t.Hour()))
	}
	bc1.Data = data
	bc1.Width = w
	bc1.SetY(0)
	bc1.Height = h / 3
	bc1.DataLabels = bclabels
	bc1.TextColor = termui.ColorWhite
	bc1.BarColor = termui.ColorRed
	bc1.NumColor = termui.ColorYellow

	bclabels = make([]string, 0) // shared between bc2 and bc3
	for _, t := range netTs {
		bclabels = append(bclabels, fmt.Sprintf("%02d", t.Hour()))
	}

	bc2 := termui.NewBarChart()
	bc2.Border.Label = "Network RX/in 10GB"
	data = make([]int, 0)
	for _, r := range rx {
		data = append(data, int(r>>30)/10)
	}
	bc2.Data = data
	bc2.Width = w
	bc2.SetY(h / 3)
	bc2.Height = h / 3
	bc2.DataLabels = bclabels
	bc2.TextColor = termui.ColorGreen
	bc2.BarColor = termui.ColorRed
	bc2.NumColor = termui.ColorYellow

	bc3 := termui.NewBarChart()
	bc3.Border.Label = "Network TX/in 10GB"
	data = make([]int, 0)
	for _, t := range tx {
		data = append(data, int(t>>30)/10)
	}
	bc3.Data = data
	bc3.Width = w
	bc3.SetY(h * 2 / 3)
	bc3.Height = h / 3
	bc3.DataLabels = bclabels
	bc3.TextColor = termui.ColorGreen
	bc3.BarColor = termui.ColorRed
	bc3.NumColor = termui.ColorYellow

	termui.Render(bc1, bc2, bc3)

	termbox.PollEvent()
}

func (*Histogram) Synopsis() string {
	return "Histogram of kafka produced messages and network volumn"
}

func (this *Histogram) Help() string {
	help := fmt.Sprintf(`
	Usage: %s histogram [options]

	    Histogram of kafka produced messages and network volumn

	Options:
	    -d
	      Draw mode.

	    -f offset file

	    -n network volumn file

	`, this.Cmd)
	return strings.TrimSpace(help)
}
