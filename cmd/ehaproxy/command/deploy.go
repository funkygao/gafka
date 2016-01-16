package command

import (
	"flag"
	"fmt"
	"io/ioutil"
	"os"
	"strings"

	"github.com/funkygao/gocli"
)

type Deploy struct {
	Ui  cli.Ui
	Cmd string

	root string
}

func (this *Deploy) Run(args []string) (exitCode int) {
	cmdFlags := flag.NewFlagSet("deploy", flag.ContinueOnError)
	cmdFlags.Usage = func() { this.Ui.Output(this.Help()) }
	cmdFlags.StringVar(&this.root, "p", defaultPrefix, "")
	if err := cmdFlags.Parse(args); err != nil {
		return 1
	}

	err := os.MkdirAll(this.root, 0755)
	swalllow(err)
	err = os.MkdirAll(fmt.Sprintf("%s/sbin", this.root), 0755)
	swalllow(err)
	err = os.MkdirAll(fmt.Sprintf("%s/logs", this.root), 0755)
	swalllow(err)
	err = os.MkdirAll(fmt.Sprintf("%s/src", this.root), 0755)
	swalllow(err)

	srcPath := fmt.Sprintf("%s/src/haproxy-1.6.3.tar.gz", this.root)
	b, _ := Asset("templates/haproxy-1.6.3.tar.gz")
	if err = ioutil.WriteFile(srcPath, b, 0644); err != nil {
		panic(err)
	}
	initPath := fmt.Sprintf("%s/src/init.ehaproxy", this.root)
	b, _ = Asset("templates/init.ehaproxy")
	err = ioutil.WriteFile(initPath, b, 0755)
	swalllow(err)

	this.Ui.Info("useradd haproxy")
	this.Ui.Info(fmt.Sprintf("compile haproxy to %s/sbin", this.root))
	this.Ui.Info(fmt.Sprintf("cp %s to /etc/init.d/ehaproxy", initPath))
	this.Ui.Info(fmt.Sprintf("chkconfig --add ehaproxy"))

	return
}

func (this *Deploy) Synopsis() string {
	return fmt.Sprintf("Deploy %s system on localhost", this.Cmd)
}

func (this *Deploy) Help() string {
	help := fmt.Sprintf(`
Usage: %s deploy [options]

    Deploy %s system on localhost

Options:

    -p prefix dir
      Defaults %s

`, this.Cmd, this.Cmd, defaultPrefix)
	return strings.TrimSpace(help)
}
