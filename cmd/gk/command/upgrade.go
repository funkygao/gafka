package command

import (
	"flag"
	"fmt"
	"os"
	"os/user"
	"path/filepath"
	"strings"

	"github.com/funkygao/gafka/ctx"
	"github.com/funkygao/gocli"
)

type Upgrade struct {
	Ui  cli.Ui
	Cmd string

	uploadDir       string
	mode            string
	upgradeKateway  bool
	upgradeZk       bool
	upgradeKguard   bool
	upgradeActord   bool
	upgradeEhaproxy bool
	upgradeConfig   bool
	upgradeDbus     bool
	upgradeHelix    bool
}

func (this *Upgrade) Run(args []string) (exitCode int) {
	cmdFlags := flag.NewFlagSet("upgrade", flag.ContinueOnError)
	cmdFlags.Usage = func() { this.Ui.Output(this.Help()) }
	cmdFlags.StringVar(&this.uploadDir, "upload", "/var/www/html", "")
	cmdFlags.StringVar(&this.mode, "m", "d", "")
	cmdFlags.BoolVar(&this.upgradeConfig, "c", false, "")
	cmdFlags.BoolVar(&this.upgradeKateway, "k", false, "")
	cmdFlags.BoolVar(&this.upgradeEhaproxy, "ha", false, "")
	cmdFlags.BoolVar(&this.upgradeActord, "at", false, "")
	cmdFlags.BoolVar(&this.upgradeHelix, "he", false, "")
	cmdFlags.BoolVar(&this.upgradeZk, "zk", false, "")
	cmdFlags.BoolVar(&this.upgradeDbus, "dbus", false, "")
	cmdFlags.BoolVar(&this.upgradeKguard, "kg", false, "")
	if err := cmdFlags.Parse(args); err != nil {
		return 1
	}

	gopath := os.Getenv("GOPATH")
	usr, _ := user.Current()

	if this.upgradeConfig {
		switch this.mode {
		case "d":
			runCmd("wget", []string{this.storeUrl(".gafka.cf"), "-O", ".gafka.cf"})
			runCmd("mv", []string{"-f", ".gafka.cf", filepath.Join(usr.HomeDir, ".gafka.cf")})

		case "u":
			runCmd("cp", []string{"-f", filepath.Join(usr.HomeDir, ".gafka.cf"), this.uploadDir})
		}

		return
	}

	if this.upgradeActord {
		switch this.mode {
		case "d":
			runCmd("wget", []string{this.storeUrl("actord"), "-O", "actord"})
			runCmd("chmod", []string{"a+x", "actord"})
			runCmd("mv", []string{"-f", "actord", "/var/wd/actord/actord"})

		case "u":
			runCmd("cp", []string{"-f", fmt.Sprintf("%s/bin/actord", gopath), this.uploadDir})
		}

		return
	}

	if this.upgradeDbus {
		switch this.mode {
		case "d":
			runCmd("wget", []string{this.storeUrl("dbusd"), "-O", "dbusd"})
			runCmd("chmod", []string{"a+x", "dbusd"})

		case "u":
			runCmd("cp", []string{"-f", fmt.Sprintf("%s/bin/dbusd", gopath), this.uploadDir})
		}

		return
	}

	if this.upgradeEhaproxy {
		switch this.mode {
		case "d":
			runCmd("wget", []string{this.storeUrl("ehaproxy"), "-O", "ehaproxy"})
			runCmd("chmod", []string{"a+x", "ehaproxy"})
			runCmd("mv", []string{"-f", "ehaproxy", "/var/wd/ehaproxy/ehaproxy"})

		case "u":
			runCmd("cp", []string{"-f", fmt.Sprintf("%s/bin/ehaproxy", gopath), this.uploadDir})
		}

		return
	}

	if this.upgradeKateway {
		switch this.mode {
		case "d":
			runCmd("wget", []string{this.storeUrl("kateway"), "-O", "kateway"})
			runCmd("chmod", []string{"a+x", "kateway"})
			runCmd("mv", []string{"-f", "kateway", "/var/wd/kateway/kateway"})

		case "u":
			this.Ui.Warn("you must run './build.sh -it kateway' first.")
			runCmd("cp", []string{"-f", fmt.Sprintf("%s/bin/kateway", gopath), this.uploadDir})
		}

		return
	}

	if this.upgradeZk {
		switch this.mode {
		case "d":
			runCmd("wget", []string{this.storeUrl("zk"), "-O", "zk"})
			runCmd("chmod", []string{"a+x", "zk"})
			runCmd("mv", []string{"-f", "zk", "/usr/bin/zk"})

		case "u":
			runCmd("cp", []string{"-f", fmt.Sprintf("%s/bin/zk", gopath), this.uploadDir})
		}

		return
	}

	if this.upgradeHelix {
		switch this.mode {
		case "d":
			runCmd("wget", []string{this.storeUrl("helix"), "-O", "helix"})
			runCmd("chmod", []string{"a+x", "helix"})
			runCmd("mv", []string{"-f", "helix", "/usr/bin/helix"})

		case "u":
			runCmd("cp", []string{"-f", fmt.Sprintf("%s/bin/helix", gopath), this.uploadDir})
		}

		return
	}

	if this.upgradeKguard {
		switch this.mode {
		case "d":
			runCmd("wget", []string{this.storeUrl("kguard"), "-O", "kguard"})
			runCmd("chmod", []string{"a+x", "kguard"})
			runCmd("mv", []string{"-f", "kguard", "/var/wd/kguard/kguard"})

		case "u":
			runCmd("cp", []string{"-f", fmt.Sprintf("%s/bin/kguard", gopath), this.uploadDir})
		}

		return
	}

	// work on gk
	switch this.mode {
	case "d":
		// upgrade gk
		u, err := user.Current()
		swallow(err)
		runCmd("/usr/bin/gk", []string{"-v"})
		runCmd("rm", []string{"-f", "gk"})
		if this.upgradeConfig {
			runCmd("rm", []string{"-f", fmt.Sprintf("%s/.gafka.cf", u.HomeDir)})
		}
		runCmd("wget", []string{this.storeUrl("gk"), "-O", "gk"})
		runCmd("chmod", []string{"a+x", "gk"})
		runCmd("mv", []string{"-f", "gk", "/usr/bin/gk"})
		runCmd("/usr/bin/gk", []string{"-v"})

		// upgrade conf
		runCmd("wget", []string{this.storeUrl(".gafka.cf"), "-O", ".gafka.cf"})
		runCmd("mv", []string{"-f", ".gafka.cf", filepath.Join(usr.HomeDir, ".gafka.cf")})

	case "u":
		runCmd("cp", []string{"-f", fmt.Sprintf("%s/bin/gk", gopath), this.uploadDir})

	default:
		this.Ui.Error("invalid mode")
		return 2
	}

	this.Ui.Info("ok")
	return
}

func (this *Upgrade) storeUrl(fn string) string {
	return fmt.Sprintf("%s/%s", ctx.UpgradeCenter(), fn)
}

func (*Upgrade) Synopsis() string {
	return "Upgrade local gk tools to latest version"
}

func (this *Upgrade) Help() string {
	help := fmt.Sprintf(`
Usage: %s upgrade [options]

    %s

Options:

    -c
      Upgrade local $HOME/.gafka.cf
      
    -k
      Upgrade kateway instead of gk

    -zk
      Upgrade zk instead of gk

    -ha
      Upgrade ehaproxy instead of gk

    -kg
      Upgrade kguard instead of gk

    -at
      Upgrade actord instead of gk

    -dbus
      Upgrade dbusd instead of gk

    -he 
      Upgrade helix instead of gk

    -m <d|u>
      Download or upload mode
      Defaults download mode

    -upload dir
      Upload the gk file to target dir, only run on gk file server
      Defaults /var/www/html

`, this.Cmd, this.Synopsis())
	return strings.TrimSpace(help)
}
