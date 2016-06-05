package command

import (
	"errors"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/funkygao/gocli"
	"github.com/funkygao/golib/gofmt"
	"github.com/pmylund/sortutil"
)

type Segment struct {
	Ui  cli.Ui
	Cmd string

	rootPath string
	limit    int
}

func (this *Segment) Run(args []string) (exitCode int) {
	cmdFlags := flag.NewFlagSet("segment", flag.ContinueOnError)
	cmdFlags.Usage = func() { this.Ui.Output(this.Help()) }
	cmdFlags.StringVar(&this.rootPath, "p", "", "")
	cmdFlags.IntVar(&this.limit, "n", -1, "")
	if err := cmdFlags.Parse(args); err != nil {
		return 1
	}

	if validateArgs(this, this.Ui).
		require("-p").
		invalid(args) {
		return 2
	}

	segments := make(map[int]map[int]int64) // day:hour:size
	err := filepath.Walk(this.rootPath, func(path string, f os.FileInfo, err error) error {
		if f == nil {
			return err
		}
		if f.IsDir() {
			return nil
		}
		if !strings.HasSuffix(f.Name(), ".index") && !strings.HasSuffix(f.Name(), ".log") {
			return errors.New(fmt.Sprintf("filename: %s invalid segment file", f.Name()))
		}
		if !strings.HasSuffix(f.Name(), ".log") {
			return nil
		}

		if _, present := segments[f.ModTime().Day()]; !present {
			segments[f.ModTime().Day()] = make(map[int]int64)
		}
		segments[f.ModTime().Day()][f.ModTime().Hour()] += f.Size()
		return nil
	})
	if err != nil {
		this.Ui.Error(err.Error())
	}

	type segment struct {
		day  int
		hour int
		size int64
	}
	summary := make([]segment, 0)
	for day, hourSize := range segments {
		for hour, size := range hourSize {
			summary = append(summary, segment{
				day:  day,
				hour: hour,
				size: size,
			})
		}
	}
	sortutil.AscByField(summary, "size")
	if this.limit > 0 && len(summary) > this.limit {
		summary = summary[:this.limit]
	}
	for _, s := range summary {
		this.Ui.Output(fmt.Sprintf("day:%2d hour:%2d size:%s",
			s.day, s.hour, gofmt.ByteSize(s.size)))
	}

	return
}

func (*Segment) Synopsis() string {
	return "Scan the kafka segments and display summary"
}

func (this *Segment) Help() string {
	help := fmt.Sprintf(`
Usage: %s segment [options]

    Scan the kafka segments and display summary

    -p dir

    -n limit
      Default unlimited.

`, this.Cmd)
	return strings.TrimSpace(help)
}
