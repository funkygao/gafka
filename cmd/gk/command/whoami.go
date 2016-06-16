package command

import (
	"fmt"
	"strings"

	"github.com/funkygao/gafka/ctx"
	"github.com/funkygao/gocli"
)

type Whoami struct {
	Ui  cli.Ui
	Cmd string
}

func (this *Whoami) Run(args []string) (exitCode int) {
	var tags = map[string]string{
		// misc
		"10.213.57.149": "dev",

		// pubsub
		"10.209.36.14": "psub 1",
		"10.209.36.33": "psub 2",
		"10.213.1.210": "psub 3",
		"10.213.9.245": "psub 4",

		// kafka brokers with small disks
		"10.209.37.39":  "kk 1",
		"10.209.33.20":  "kk 2",
		"10.209.37.69":  "kk 3",
		"10.209.33.40":  "kk 4",
		"10.209.11.166": "kk 5",
		"10.209.11.195": "kk 6",
		"10.209.10.161": "kk 7",
		"10.209.10.141": "kk 8",

		// kafka brokers with big disks
		"10.209.18.15":   "k 1",
		"10.209.18.16":   "k 2",
		"10.209.18.65":   "k 3",
		"10.209.18.66":   "k 4",
		"10.209.19.143":  "k 5",
		"10.209.19.144":  "k 6",
		"10.209.22.142":  "k 7",
		"10.209.19.35":   "k 8",
		"10.209.19.36":   "k 9",
		"10.209.240.191": "k 11",
		"10.209.240.192": "k 12",
		"10.209.240.193": "k 13",
		"10.209.240.194": "k 14",

		// zk
		"10.209.33.69": "czk 1",
		"10.209.37.19": "czk 2",
		"10.209.37.68": "czk 3",
	}

	ip, _ := ctx.LocalIP()
	if tag, present := tags[ip.String()]; present {
		this.Ui.Output(tag)
	} else {
		this.Ui.Warn("unknown")
	}

	return
}

func (*Whoami) Synopsis() string {
	return "Display effective expect script tag of current host"
}

func (this *Whoami) Help() string {
	help := fmt.Sprintf(`
Usage: %s whoami [options]

    Display effective expect script tag of current host

`, this.Cmd)
	return strings.TrimSpace(help)
}
