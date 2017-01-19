package command

import (
	"flag"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/funkygao/columnize"
	"github.com/funkygao/gocli"
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
	for {
		stats, err := disk.IOCounters()
		swallow(err)

		sortedDisks := make([]string, 0, len(stats))
		for d := range stats {
			sortedDisks = append(sortedDisks, d)
		}
		sort.Strings(sortedDisks)

		lines := []string{"Disk|iops|read#|write#|mergedR#|mergedW#|readT|writeT|ioT"}
		for _, d := range sortedDisks {
			stat := stats[d]

			lines = append(lines, fmt.Sprintf("%s|%d|%d|%d|%d|%d|%d|%d|%d", stat.Name,
				stat.IopsInProgress,
				stat.ReadCount, stat.WriteCount,
				stat.MergedReadCount, stat.MergedWriteCount,
				stat.ReadTime, stat.WriteTime,
				stat.IoTime))
		}
		fmt.Println(columnize.SimpleFormat(lines))

		time.Sleep(time.Second * 2)
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
