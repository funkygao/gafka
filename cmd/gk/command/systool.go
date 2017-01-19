package command

import (
	"flag"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/funkygao/columnize"
	"github.com/funkygao/gocli"
	"github.com/funkygao/golib/gofmt"
	"github.com/shirou/gopsutil/disk"
)

type Systool struct {
	Ui  cli.Ui
	Cmd string
}

func (this *Systool) Run(args []string) (exitCode int) {
	var (
		diskTool bool
	)
	cmdFlags := flag.NewFlagSet("systool", flag.ContinueOnError)
	cmdFlags.Usage = func() { this.Ui.Output(this.Help()) }
	cmdFlags.BoolVar(&diskTool, "d", false, "")
	if err := cmdFlags.Parse(args); err != nil {
		return 1
	}

	if diskTool {
		this.runDiskTool()
		return
	}

	return
}

func (*Systool) runDiskTool() {
	var lastStats = make(map[string]disk.IOCountersStat)
	for {
		stats, err := disk.IOCounters()
		swallow(err)

		sortedDisks := make([]string, 0, len(stats))
		for d := range stats {
			sortedDisks = append(sortedDisks, d)
		}
		sort.Strings(sortedDisks)

		lines := []string{"Disk|iops|read bytes|write bytes|read#|write#|mergedR#|mergedW#|readT|writeT|ioT|wio"}
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

			lines = append(lines, fmt.Sprintf("%s|%d|%s|%s|%d|%d|%d|%d|%d|%d|%d|%d",
				stat.Name,
				stat.IopsInProgress,
				gofmt.ByteSize(stat.ReadBytes), gofmt.ByteSize(stat.WriteBytes),
				stat.ReadCount, stat.WriteCount,
				stat.MergedReadCount, stat.MergedWriteCount,
				stat.ReadTime, stat.WriteTime,
				stat.IoTime,
				stat.WeightedIO))
		}

		refreshScreen()
		fmt.Println(columnize.SimpleFormat(lines))

		if len(lastStats) == 0 {
			time.Sleep(time.Second * 5)
		} else {
			time.Sleep(time.Second * 3)
		}

		lastStats = stats
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

`, this.Cmd, this.Synopsis())
	return strings.TrimSpace(help)
}
