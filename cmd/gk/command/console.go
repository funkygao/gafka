package command

import (
	"fmt"
	"io/ioutil"
	"os"
	"os/user"
	"path/filepath"
	"strings"

	"github.com/funkygao/gocli"
	"github.com/peterh/liner"
)

type Console struct {
	Ui          cli.Ui
	Cmd         string
	Cmds        map[string]cli.CommandFactory
	Line        *liner.State
	historyFile string
}

func (this *Console) Run(args []string) (exitCode int) {
	this.Line = liner.NewLiner()
	this.Line.SetCtrlCAborts(true)
	this.Line.SetCompleter(func(line string) (c []string) {
		for cmd, _ := range this.Cmds {
			if strings.HasPrefix(cmd, strings.ToLower(line)) {
				c = append(c, cmd)
			}
		}
		return
	})
	defer this.Line.Close()

	if usr, err := user.Current(); err == nil {
		this.historyFile = filepath.Join(usr.HomeDir, fmt.Sprintf(".%s_history", this.Cmd))
		if f, e := os.Open(this.historyFile); e == nil {
			this.Line.ReadHistory(f)
			f.Close()
		}
	}

	for {
		line, err := this.Line.Prompt(fmt.Sprintf("%s> ", this.Cmd))
		if err != err {
			break
		}
		line = strings.TrimSpace(line)
		if line == "" || line == "exit" {
			break
		}

		if this.runCommand(line) {
			// write out the history
			if len(this.historyFile) > 0 {
				this.Line.AppendHistory(line)
				if f, e := os.Create(this.historyFile); e == nil {
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
	ok = true

	parts := strings.Split(cmd, " ")
	switch parts[0] {
	case "help":
		this.doHelp()
	case "history":
		this.doHistory()
	default:
		if _, present := this.Cmds[parts[0]]; !present {
			this.Ui.Error(fmt.Sprintf("unkown command: %s", parts[0]))
			return
		}

		cmd, _ := this.Cmds[parts[0]]()
		cmd.Run(parts)

	}

	return
}

func (this *Console) doHelp() {
	outline := ""
	width := 0
	for cmd, _ := range this.Cmds {
		outline += cmd + " "
		width += len(cmd)
		if width > 80 {
			outline += "\n"
			width = 0
		}
	}
	this.Ui.Output(outline)
}

func (this *Console) doHistory() {
	if this.historyFile == "" {
		return
	}

	if history, err := ioutil.ReadFile(this.historyFile); err == nil {
		this.Ui.Output(string(history))
	}
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
