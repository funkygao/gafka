package command

import (
	"bufio"
	"flag"
	"fmt"
	"os"
	"os/user"
	"strings"

	"github.com/funkygao/gocli"
	"github.com/funkygao/golib/pipestream"
)

type Upgrade struct {
	Ui  cli.Ui
	Cmd string

	storeUrl       string
	uploadDir      string
	mode           string
	upgradeKateway bool
	upgradeZk      bool
}

func (this *Upgrade) Run(args []string) (exitCode int) {
	cmdFlags := flag.NewFlagSet("upgrade", flag.ContinueOnError)
	cmdFlags.Usage = func() { this.Ui.Output(this.Help()) }
	cmdFlags.StringVar(&this.storeUrl, "url", "http://10.213.57.149:10080/gk", "")
	cmdFlags.StringVar(&this.uploadDir, "upload", "/var/www/html", "")
	cmdFlags.StringVar(&this.mode, "m", "d", "")
	cmdFlags.BoolVar(&this.upgradeKateway, "k", false, "")
	cmdFlags.BoolVar(&this.upgradeZk, "zk", false, "")
	if err := cmdFlags.Parse(args); err != nil {
		return 1
	}

	gopath := os.Getenv("GOPATH")

	if this.upgradeKateway {
		this.storeUrl = "http://10.213.57.149:10080/kateway"

		switch this.mode {
		case "d":
			this.runCmd("wget", []string{this.storeUrl})
			this.runCmd("chmod", []string{"a+x", "kateway"})
			this.runCmd("mv", []string{"-f", "kateway", "/var/wd/kateway/kateway"})

		case "u":
			this.Ui.Warn("you must run './build.sh -it kateway' first.")
			this.runCmd("cp", []string{"-f", fmt.Sprintf("%s/bin/kateway", gopath), this.uploadDir})
		}

		return
	}

	if this.upgradeZk {
		this.storeUrl = "http://10.213.57.149:10080/zk"

		switch this.mode {
		case "d":
			this.runCmd("wget", []string{this.storeUrl})
			this.runCmd("chmod", []string{"a+x", "zk"})
			this.runCmd("mv", []string{"-f", "zk", "/usr/bin/zk"})

		case "u":
			this.runCmd("cp", []string{"-f", fmt.Sprintf("%s/bin/zk", gopath), this.uploadDir})
		}

		return
	}

	// work on gk
	switch this.mode {
	case "d":
		u, err := user.Current()
		swallow(err)
		this.runCmd("/usr/bin/gk", []string{"-v"})
		this.runCmd("rm", []string{"-f", "gk"})
		this.runCmd("rm", []string{"-f", fmt.Sprintf("%s/.gafka.cf", u.HomeDir)})
		this.runCmd("wget", []string{this.storeUrl})
		this.runCmd("chmod", []string{"a+x", "gk"})
		this.runCmd("mv", []string{"-f", "gk", "/usr/bin/gk"})
		this.runCmd("/usr/bin/gk", []string{"-v"})

	case "u":
		this.runCmd("cp", []string{"-f", fmt.Sprintf("%s/bin/gk", gopath), this.uploadDir})

	default:
		this.Ui.Error("invalid mode")
		return 2
	}

	this.Ui.Info("ok")
	return
}

func (this *Upgrade) runCmd(c string, args []string) {
	this.Ui.Output(fmt.Sprintf("  %s %+v", c, args))

	cmd := pipestream.New(c, args...)
	err := cmd.Open()
	swallow(err)
	defer cmd.Close()

	scanner := bufio.NewScanner(cmd.Reader())
	scanner.Split(bufio.ScanLines)
	for scanner.Scan() {
		this.Ui.Output(fmt.Sprintf("    %s", scanner.Text()))
	}
	err = scanner.Err()
	if err != nil {
		this.Ui.Error(err.Error())
	}

}

func (*Upgrade) Synopsis() string {
	return "Upgrade local gk to latest version"
}

func (this *Upgrade) Help() string {
	help := fmt.Sprintf(`
Usage: %s upgrade [options]

    Upgrade local gk to latest version

Options:

    -k
      Upgrade kateway instead of gk

    -zk
      Upgrade zk instead of gk

    -m <d|u>
      Download or upload mode
      Defaults download mode

    -url gk file server url
      Defaults http://10.213.57.149:10080/gk

    -upload dir
      Upload the gk file to target dir, only run on gk file server
      Defaults /var/www/html

`, this.Cmd)
	return strings.TrimSpace(help)
}
