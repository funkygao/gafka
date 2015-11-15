package command

import (
	"fmt"
	"os"
	"os/user"
	"path/filepath"
	"strings"

	"github.com/funkygao/gocli"
	"github.com/peterh/liner"
)

type Console struct {
	Ui   cli.Ui
	Cmd  string
	Line *liner.State
}

func (this *Console) Run(args []string) (exitCode int) {
	this.Line = liner.NewLiner()
	defer this.Line.Close()

	var historyFile string
	if usr, err := user.Current(); err == nil {
		historyFile = filepath.Join(usr.HomeDir, fmt.Sprintf(".%s_history", this.Cmd))
		if f, e := os.Open(historyFile); e == nil {
			this.Line.ReadHistory(f)
			f.Close()
		}
	}

	for {
		line, err := this.Line.Prompt("> ")
		if err != err {
			break
		}
		line = strings.TrimSpace(line)
		if line == "" || line == "exit" {
			break
		}

		if this.runCommand(line) {
			// write out the history
			if len(historyFile) > 0 {
				this.Line.AppendHistory(line)
				if f, e := os.Create(historyFile); e == nil {
					this.Line.WriteHistory(f)
					f.Close()
				}
			}
		} else {
			break
		}
	}
	return
}

func (this *Console) runCommand(cmd string) (ok bool) {
	fmt.Println(cmd)
	ok = true
	return
}

func (*Console) Synopsis() string {
	return "Interactive mode"
}

func (this *Console) Help() string {
	help := fmt.Sprintf(`
Usage: %s console [options]

	Interactive mode

`, this.Cmd)
	return strings.TrimSpace(help)
}
