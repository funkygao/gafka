package command

import (
	"fmt"
	"io/ioutil"
	"os"
	"os/user"
	"path/filepath"
	"sort"
	"strings"

	"github.com/funkygao/gafka/ctx"
	"github.com/funkygao/gocli"
	"github.com/peterh/liner"
)

type Console struct {
	Ui  cli.Ui
	Cmd string

	Cmds        map[string]cli.CommandFactory
	Line        *liner.State
	historyFile string

	builtinCmds []string
}

func (this *Console) Run(args []string) (exitCode int) {
	this.builtinCmds = []string{"help", "history"}

	this.Line = liner.NewLiner()
	this.Line.SetCtrlCAborts(false)
	this.Line.SetCompleter(func(line string) (c []string) {
		for cmd := range this.Cmds {
			if strings.HasPrefix(cmd, strings.ToLower(line)) {
				c = append(c, cmd)
			}
		}
		for _, cmd := range ctx.Aliases() {
			if strings.HasPrefix(cmd, strings.ToLower(line)) {
				c = append(c, cmd)
			}
		}
		for _, cmd := range this.builtinCmds {
			if strings.HasPrefix(cmd, strings.ToLower(line)) {
				c = append(c, cmd)
			}
		}
		c = append(c, this.builtinCmds...)
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
		if err != nil {
			break
		}

		line = strings.TrimSpace(line)
		if line == "" {
			continue
		}
		if line == "bye" || line == "q" || line == "quit" || line == "exit" {
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
		}
	}
	return
}

func (this *Console) runCommand(cmdLine string) (ok bool) {
	ok = true

	parts := strings.Split(cmdLine, " ")
	for i := 0; i < len(parts); i++ {
		parts[i] = strings.TrimSpace(parts[i])
	}

	cmd := parts[0]
	switch cmd {
	case "help", "h":
		this.doHelp()

	case "history":
		this.doHistory()

	default:
		args := parts
		if alias, present := ctx.Alias(cmd); present {
			aliasCmdParts := strings.Split(alias, " ")
			cmd = aliasCmdParts[0]
			args = make([]string, 0)
			args = append(args, aliasCmdParts...)
			if len(args) > 1 {
				args = append(args, parts[1:]...)
			}
		}
		if _, present := this.Cmds[cmd]; !present {
			this.Ui.Error(fmt.Sprintf("unknown command: %s", parts[0]))
			return
		}

		targetCmd, _ := this.Cmds[cmd]()
		targetCmd.Run(args[1:])
	}

	return
}

func (this *Console) doHelp() {
	outline := ""
	width := 0

	cmds := make([]string, 0)
	for cmd := range this.Cmds {
		cmds = append(cmds, cmd)
	}
	for _, cmd := range ctx.Aliases() {
		cmds = append(cmds, cmd)
	}
	cmds = append(cmds, this.builtinCmds...)
	sort.Strings(cmds)

	for _, cmd := range cmds {
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

    %s

`, this.Cmd, this.Synopsis())
	return strings.TrimSpace(help)
}
