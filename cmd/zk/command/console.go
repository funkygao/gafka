package command

import (
	"flag"
	"fmt"
	"io/ioutil"
	"os"
	"os/user"
	"path/filepath"
	"sort"
	"strings"

	"github.com/funkygao/gafka/ctx"
	gzk "github.com/funkygao/gafka/zk"
	"github.com/funkygao/gocli"
	"github.com/funkygao/golib/color"
	"github.com/peterh/liner"
)

type Console struct {
	Ui  cli.Ui
	Cmd string

	Cmds        map[string]cli.CommandFactory
	Line        *liner.State
	historyFile string

	prompt      string
	zkzone      *gzk.ZkZone
	zone        string
	cwd         string
	builtinCmds []string
}

func (this *Console) Run(args []string) (exitCode int) {
	cmdFlags := flag.NewFlagSet("console", flag.ContinueOnError)
	cmdFlags.Usage = func() { this.Ui.Output(this.Help()) }
	cmdFlags.StringVar(&this.zone, "z", ctx.ZkDefaultZone(), "")
	if err := cmdFlags.Parse(args); err != nil {
		return 1
	}

	this.builtinCmds = []string{"help", "history", "ls", "cat", "pwd", "cd"}
	this.cwd = "/"

	this.zkzone = gzk.NewZkZone(gzk.DefaultConfig(this.zone, ctx.ZoneZkAddrs(this.zone)))
	if err := this.zkzone.Connect(); err != nil {
		panic(err)
	}
	defer this.zkzone.Close()

	this.Line = liner.NewLiner()
	this.Line.SetCtrlCAborts(true)
	this.Line.SetCompleter(func(line string) (c []string) {
		p := strings.SplitN(line, " ", 2)
		if len(p) == 2 && strings.TrimSpace(p[1]) != "" {
			children, _, err := this.zkzone.Conn().Children(this.cwd)
			if err != nil {
				this.Ui.Error(err.Error())
				return
			}

			for _, child := range children {
				if strings.HasPrefix(child, p[1]) {
					c = append(c, fmt.Sprintf("%s %s", p[0], child))
				}
			}

			return
		}

		for cmd := range this.Cmds {
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
		this.refreshPrompt()

		line, err := this.Line.Prompt(color.Green("%s> ", this.prompt))
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

		this.runCommand(line)
		// write out the history
		if len(this.historyFile) > 0 {
			this.Line.AppendHistory(line)
			if f, e := os.Create(this.historyFile); e == nil {
				this.Line.WriteHistory(f)
				f.Close()
			}
		}
	}

	return
}

func (this *Console) runCommand(cmdLine string) {
	parts := strings.Split(cmdLine, " ")
	for i := 0; i < len(parts); i++ {
		parts[i] = strings.TrimSpace(parts[i])
	}

	cmd := parts[0]
	switch cmd {
	case "h", "help":
		this.doHelp()

	case "cd":
		if len(parts) < 2 {
			return
		}

		switch {
		case parts[1][0] == '/':
			// absolute path
			this.cwd = parts[1]

		case parts[1] == ".":
			return

		case parts[1] == ".." || parts[1] == "../":
			idx := strings.LastIndex(this.cwd, "/")
			if idx != -1 {
				this.cwd = this.cwd[:idx]
			}

		default:
			if strings.HasSuffix(this.cwd, "/") {
				this.cwd = fmt.Sprintf("%s%s", this.cwd, parts[1])
			} else {
				this.cwd = fmt.Sprintf("%s/%s", this.cwd, parts[1])
			}
		}

	case "ls":
		// ls [path] [-R] [-l]
		// TODO -R -l
		path := this.cwd
		if len(parts) > 1 && parts[1][0] != '-' {
			path = parts[1]
		}
		if path[0] != '/' {
			// comparative path
			if this.cwd == "/" {
				path = this.cwd + path
			} else {
				path = fmt.Sprintf("%s/%s", this.cwd, path)
			}

		}

		var (
			recursive bool
			longFmt   bool
		)
		for _, arg := range parts {
			switch arg {
			case "-R":
				recursive = true

			case "-l":
				longFmt = true
			}
		}

		if recursive {
			this.showChildrenRecursively(path, longFmt)
			return
		}

		_ = longFmt // TODO

		children, _, err := this.zkzone.Conn().Children(path)
		if err != nil {
			this.Ui.Error(err.Error())
			return
		}

		for _, child := range children {
			this.Ui.Output(fmt.Sprintf("%s", child))
		}

	case "cat":
		// TODO -R

		if len(parts) < 2 {
			this.Ui.Error("Usage: cat <znode>")
			return
		}

		path := this.realZpath(parts[1])
		data, _, err := this.zkzone.Conn().Get(path)
		if err != nil {
			this.Ui.Error(err.Error())
			return
		}
		if len(data) == 0 {
			this.Ui.Warn("empty")
			return
		}

		this.Ui.Output(string(data))

	case "pwd":
		this.Ui.Output(this.cwd)

	case "history":
		this.doHistory()

	default:
		args := parts

		if _, present := this.Cmds[cmd]; !present {
			this.Ui.Error(fmt.Sprintf("unknown command: %s", parts[0]))
			return
		}

		targetCmd, _ := this.Cmds[cmd]()
		targetCmd.Run(args[1:])
	}

}

func (this *Console) showChildrenRecursively(path string, longFmt bool) {
	children, stat, err := this.zkzone.Conn().Children(path)
	if err != nil {
		return
	}

	sort.Strings(children)
	for _, child := range children {
		if path == "/" {
			path = ""
		}

		if longFmt {
			this.Ui.Output(fmt.Sprintf("ctime:%d mtime:%d %s",
				stat.Ctime, stat.Mtime,
				path+"/"+child))
		} else {
			this.Ui.Output(path + "/" + child)
		}

		this.showChildrenRecursively(path+"/"+child, longFmt)
	}
}

func (this *Console) realZpath(path string) string {
	if path[0] == '/' {
		return path
	}

	if strings.HasSuffix(this.cwd, "/") {
		return fmt.Sprintf("%s%s", this.cwd, path)
	}
	return fmt.Sprintf("%s/%s", this.cwd, path)
}

func (this *Console) refreshPrompt() {
	this.prompt = fmt.Sprintf("%s:%s", this.zone, this.cwd)
}

func (this *Console) doHelp() {
	outline := ""
	width := 0

	cmds := make([]string, 0)
	for cmd := range this.Cmds {
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

    Interactive mode

Options:

    -z zone

`, this.Cmd)
	return strings.TrimSpace(help)
}
