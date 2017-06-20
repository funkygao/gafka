package command

import (
	"bufio"
	"flag"
	"fmt"
	"os"
	"path"
	"strconv"
	"strings"

	"github.com/funkygao/gocli"
	gio "github.com/funkygao/golib/io"
)

type GC struct {
	Ui  cli.Ui
	Cmd string

	threshold float64
}

func (this *GC) Run(args []string) (exitCode int) {
	cmdFlags := flag.NewFlagSet("gc", flag.ContinueOnError)
	cmdFlags.Usage = func() { this.Ui.Output(this.Help()) }
	cmdFlags.Float64Var(&this.threshold, "t", 2., "")
	if err := cmdFlags.Parse(args); err != nil {
		return 1
	}

	if len(args) == 0 {
		this.Ui.Error("missing <gc.log>")
		return 2
	}

	logfile := args[len(args)-1]

	this.analyzeKafkaGCLog(logfile)

	return
}

// 2017-06-20T13:29:38.206+0800: 172272.850: [GC2017-06-20T13:29:38.206+0800: 172272.850: [ParNew: 1121793K->2982K(1258304K), 0.0039060 secs] 1189130K->70395K(4054528K), 0.0039930 secs] [Times: user=0.08 sys=0.00, real=0.00 secs]
func (this *GC) analyzeKafkaGCLog(name string) {
	f, err := os.Open(name)
	if err != nil {
		this.Ui.Error(err.Error())
		return
	}
	defer f.Close()

	reader := bufio.NewReader(f)
	for {
		line, err := gio.ReadLine(reader)
		if err != nil {
			break
		}

		ut, rt, at := this.parseKafkaGCLine(string(line))
		if ut > this.threshold || rt > this.threshold {
			this.Ui.Outputf("%s %s user:%.2f real:%.2f", path.Base(path.Dir(path.Dir(f.Name()))), at, ut, rt)
		}
	}
}

func (*GC) parseKafkaGCLine(line string) (usert, realt float64, at string) {
	tuples := strings.Fields(line)
	if len(tuples) < 15 {
		// unrecognized line
		return
	}

	at = tuples[0]

	us := strings.SplitN(tuples[12], "=", 2)
	if us[0] != "user" {
		// unrecognized line
		return
	}
	usert, _ = strconv.ParseFloat(us[1], 2)

	rt := strings.SplitN(tuples[14], "=", 2)
	if rt[0] != "real" {
		// unrecognized line
		return
	}
	realt, _ = strconv.ParseFloat(rt[1], 2)

	return
}

func (*GC) Synopsis() string {
	return "Java GC log analyzer and visualizer"
}

func (this *GC) Help() string {
	help := fmt.Sprintf(`
Usage: %s gc [options] <gc.log>

    %s

Options:

    -t threashold
     Default 2.0
`, this.Cmd, this.Synopsis())
	return strings.TrimSpace(help)
}
