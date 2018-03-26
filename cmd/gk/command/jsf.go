package command

import (
	"flag"
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"strings"

	"github.com/funkygao/gocli"
)

type Jsf struct {
	Ui  cli.Ui
	Cmd string
}

func (this *Jsf) Run(args []string) (exitCode int) {
	cmdFlags := flag.NewFlagSet("jsf", flag.ContinueOnError)
	cmdFlags.Usage = func() { this.Ui.Output(this.Help()) }
	if err := cmdFlags.Parse(args); err != nil {
		return 1
	}

	if len(args) == 0 {
		this.Ui.Error("missing path")
		return 2
	}

	this.parseRootDir(args[len(args)-1])

	return
}

func (this *Jsf) parseRootDir(root string) {
	swallow(filepath.Walk(root, func(path string, f os.FileInfo, err error) error {
		if f == nil {
			return err
		}
		if f.IsDir() {
			return nil
		}
		if !strings.HasSuffix(f.Name(), ".java") {
			return nil
		}

		fmt.Println(f.Name())

		_, err = ioutil.ReadFile(path)
		if err != nil {
			return err
		}

		return nil

	}))
}

func (*Jsf) Synopsis() string {
	return "Statically parse JSF services from java files"
}

func (this *Jsf) Help() string {
	help := fmt.Sprintf(`
Usage: %s jsf path

    %s

`, this.Cmd, this.Synopsis())
	return strings.TrimSpace(help)
}
