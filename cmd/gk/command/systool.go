package command

import (
	"flag"
	"fmt"
	"io/ioutil"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/funkygao/columnize"
	"github.com/funkygao/gocli"
	"github.com/funkygao/golib/color"
	"github.com/funkygao/golib/gofmt"
	"github.com/shirou/gopsutil/disk"
	"github.com/shirou/gopsutil/mem"
	"github.com/shirou/gopsutil/net"
)

type Systool struct {
	Ui  cli.Ui
	Cmd string
}

func (this *Systool) Run(args []string) (exitCode int) {
	var (
		diskTool  bool
		netTool   bool
		ioSched   bool
		itop      bool
		vmTool    bool
		ppsDevice string
		interval  time.Duration
	)
	cmdFlags := flag.NewFlagSet("systool", flag.ContinueOnError)
	cmdFlags.Usage = func() { this.Ui.Output(this.Help()) }
	cmdFlags.BoolVar(&diskTool, "d", false, "")
	cmdFlags.BoolVar(&vmTool, "m", false, "")
	cmdFlags.BoolVar(&netTool, "n", false, "")
	cmdFlags.BoolVar(&ioSched, "io", false, "")
	cmdFlags.StringVar(&ppsDevice, "pps", "", "")
	cmdFlags.BoolVar(&itop, "itop", false, "")
	cmdFlags.DurationVar(&interval, "i", time.Second*3, "")
	if err := cmdFlags.Parse(args); err != nil {
		return 1
	}

	if ppsDevice != "" {
		this.showPps(ppsDevice, interval)
		return
	}

	if ioSched {
		this.runIOScheduler()
		return
	}

	if vmTool {
		this.runVMTool(interval)
		return
	}

	if diskTool {
		this.runDiskTool(interval)
		return
	}

	if netTool {
		this.runNetTool(interval)
		return
	}

	if itop {
		this.runItop(interval)
	}

	return
}

func (this *Systool) runItop(interval time.Duration) {
	path := "/tmp/itop.pl"
	writeFileFromTemplate("template/tools/itop", path, 0755, nil, nil)
	runCmd(path, []string{"-i", fmt.Sprintf("%d", int(interval.Seconds()))})
}

func (this *Systool) runVMTool(interval time.Duration) {
	for {
		refreshScreen()

		s, err := mem.VirtualMemory()
		swallow(err)

		this.Ui.Outputf("cache:        %s", gofmt.ByteSize(s.Cached))
		this.Ui.Outputf("dirty:        %s", gofmt.ByteSize(s.Dirty))
		this.Ui.Outputf("writeback:    %s", gofmt.ByteSize(s.Writeback))
		this.Ui.Outputf("writebacktmp: %d", s.WritebackTmp)
		this.Ui.Outputf("shared:       %s", gofmt.ByteSize(s.Shared))
		this.Ui.Outputf("slab:         %d", s.Slab)
		this.Ui.Outputf("pagetables:   %d", s.PageTables)

		time.Sleep(interval)
	}
}

func (this *Systool) runIOScheduler() {
	stats, err := disk.IOCounters()
	swallow(err)

	sortedDisks := make([]string, 0, len(stats))
	for d := range stats {
		sortedDisks = append(sortedDisks, d)
	}
	sort.Strings(sortedDisks)

	for _, d := range sortedDisks {
		diskSuffix := d[len(d)-1]
		if diskSuffix >= '0' && diskSuffix <= '9' {
			// skip partition
			continue
		}

		fn := fmt.Sprintf("/sys/block/%s/queue/scheduler", d)
		b, err := ioutil.ReadFile(fn)
		swallow(err)

		this.Ui.Outputf("%8s: %s", d, strings.TrimSpace(string(b)))
		this.Ui.Outputf("%9s echo noop > %s", " ", fn)
	}

	this.Ui.Infof("make sure dirty_background_ratio < dirty_ratio")
}

func (*Systool) runNetTool(interval time.Duration) {
	for {
		refreshScreen()

		stats, err := net.ProtoCounters([]string{"tcp"})
		swallow(err)

		stat := stats[0].Stats
		sortedName := make([]string, 0, len(stat))
		for n := range stat {
			sortedName = append(sortedName, n)
		}
		sort.Strings(sortedName)

		for _, name := range sortedName {
			fmt.Printf("%20s %d\n", name, stat[name])
		}

		time.Sleep(interval)
	}
}

func (*Systool) runDiskTool(interval time.Duration) {
	var lastStats = make(map[string]disk.IOCountersStat)
	for {
		stats, err := disk.IOCounters()
		swallow(err)

		sortedDisks := make([]string, 0, len(stats))
		for d := range stats {
			sortedDisks = append(sortedDisks, d)
		}
		sort.Strings(sortedDisks)

		lines := []string{"Disk|io|iops|read bytes|write bytes|read#|write#|mergedR#|mergedW#|readT|writeT|ioT|wio"}
		for _, d := range sortedDisks {
			diskSuffix := d[len(d)-1]
			if diskSuffix > '9' || diskSuffix < '0' {
				// not partitioned
				continue
			}

			stat := stats[d]
			if last, present := lastStats[d]; present {
				stat.ReadBytes -= last.ReadBytes
				stat.WriteBytes -= last.WriteBytes
				stat.ReadCount -= last.ReadCount
				stat.WriteCount -= last.WriteCount
				stat.MergedReadCount -= last.MergedReadCount
				stat.MergedWriteCount -= last.MergedWriteCount
				stat.WriteTime -= last.WriteTime
				stat.ReadTime -= last.ReadTime
				stat.IoTime -= last.IoTime
				stat.WeightedIO -= last.WeightedIO
			}

			var (
				rbytes = gofmt.ByteSize(stat.ReadBytes).String()
				wbytes = gofmt.ByteSize(stat.WriteBytes).String()
			)
			if strings.HasSuffix(rbytes, "MB") || strings.HasSuffix(rbytes, "GB") || strings.HasSuffix(rbytes, "TB") {
				rbytes = color.Red("%-10s", rbytes)
			}
			if strings.HasSuffix(wbytes, "MB") || strings.HasSuffix(wbytes, "GB") || strings.HasSuffix(wbytes, "TB") {
				wbytes = color.Red("%-11s", wbytes)
			}

			lines = append(lines, fmt.Sprintf("%s|%d|%d|%s|%s|%d|%d|%d|%d|%d|%d|%d|%d",
				stat.Name,
				stat.IopsInProgress,
				int(stat.ReadCount+stat.WriteCount)/int(interval.Seconds()),
				rbytes, wbytes,
				stat.ReadCount, stat.WriteCount,
				stat.MergedReadCount, stat.MergedWriteCount,
				stat.ReadTime, stat.WriteTime,
				stat.IoTime,
				stat.WeightedIO))
		}

		refreshScreen()
		fmt.Println(columnize.SimpleFormat(lines))

		if len(lastStats) == 0 {
			time.Sleep(interval + time.Second*3)
		} else {
			time.Sleep(interval)
		}

		lastStats = stats
	}

}

func (this *Systool) showPps(nic string, interval time.Duration) {
	tx := fmt.Sprintf("/sys/class/net/%s/statistics/tx_packets", nic)
	rx := fmt.Sprintf("/sys/class/net/%s/statistics/rx_packets", nic)

	var lastTx, lastRx int64
	s := int64(interval.Seconds())
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

		time.Sleep(interval)
	}
}

func (*Systool) Synopsis() string {
	return "OS diagnostics tool"
}

func (this *Systool) Help() string {
	help := fmt.Sprintf(`
Usage: %s systool [options]

    %s

Options:

    -d
      Disk diagnostics

    -n
      Network diagnostics

    -pps nic
     Show PacketPerSecond of a NIC on Linux

    -m
      VM diagnostics

    -i interval
      e,g. -i 3s

    -io
      IO scheduler

    -itop
      Interrupt top

`, this.Cmd, this.Synopsis())
	return strings.TrimSpace(help)
}
