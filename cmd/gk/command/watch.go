package command

import (
	"flag"
	"fmt"
	"strings"

	"github.com/fsnotify/fsnotify"
	"github.com/funkygao/gocli"
)

// TODO calculate how much data produced each day "github.com/hashicorp/go-memdb"
type Watch struct {
	Ui  cli.Ui
	Cmd string
}

func (this *Watch) Run(args []string) (exitCode int) {
	var recursive bool
	cmdFlags := flag.NewFlagSet("watch", flag.ContinueOnError)
	cmdFlags.Usage = func() { this.Ui.Output(this.Help()) }
	cmdFlags.BoolVar(&recursive, "R", false, "")
	if err := cmdFlags.Parse(args); err != nil {
		return 1
	}

	if len(args) == 0 {
		this.Ui.Error("missing path")
		this.Ui.Output(this.Help())
		return 2
	}

	this.watchDir(args[len(args)-1], recursive)

	return
}

func (this *Watch) watchDir(path string, recursive bool) {
	w, err := fsnotify.NewWatcher()
	swallow(err)
	defer w.Close()

	go func() {
		for {
			select {
			case event := <-w.Events:
				this.Ui.Outputf("%+v", event)
			case err := <-w.Errors:
				this.Ui.Error(err.Error())
			}
		}
	}()

	err = w.Add(path)
	swallow(err)

	select {}
}

func (*Watch) Synopsis() string {
	return "Watch file system event"
}

func (this *Watch) Help() string {
	help := fmt.Sprintf(`
Usage: %s watch [options] path

    %s

    -R
     Recursive.

`, this.Cmd, this.Synopsis())
	return strings.TrimSpace(help)
}
