package command

import (
	"flag"
	"fmt"
	"io/ioutil"
	"os"
	"strings"

	"github.com/funkygao/gafka/ctx"
	"github.com/funkygao/gocli"
	"github.com/funkygao/golib/pipestream"
)

type Ssh struct {
	Ui  cli.Ui
	Cmd string
}

func (this *Ssh) Run(args []string) (exitCode int) {
	var (
		zone string
		host string
	)
	cmdFlags := flag.NewFlagSet("ssh", flag.ContinueOnError)
	cmdFlags.Usage = func() { this.Ui.Output(this.Help()) }
	cmdFlags.StringVar(&host, "h", "", "")
	cmdFlags.StringVar(&zone, "z", "", "")
	if err := cmdFlags.Parse(args); err != nil {
		return 2
	}

	if validateArgs(this, this.Ui).require("-z", "-h").invalid(args) {
		return 2
	}

	p := strings.SplitN(ctx.Tunnels()[zone], "@", 2)
	user, relayHost := p[0], p[1]

	this.ssh(user, relayHost, host)

	return
}

func (this *Ssh) ssh(user, relayHost, host string) {
	expectScript := fmt.Sprintf(`
#!/usr/bin/expect

spawn ssh %s@%s
expect "*to exit*"
send "%s\r"
send "sudo -s\r"
interact
`, user, relayHost, host)
	f, err := ioutil.TempFile("", "expect_script")
	if err != nil {
		panic(err)
	}

	fname := f.Name()
	f.Close()
	defer os.Remove(fname)

	err = ioutil.WriteFile(fname, []byte(expectScript), 0666)
	if err != nil {
		panic(err)
	}

	cmd := pipestream.New("/usr/bin/expect", fname)
	err = cmd.Open()
	if err != nil {
		panic(err)
	}
	defer cmd.Close()
}

func (*Ssh) Synopsis() string {
	return "ssh to a host through tunnel"
}

func (this *Ssh) Help() string {
	help := fmt.Sprintf(`
Usage: %s ssh -z zone -h host

    ssh to a host through tunnel

`, this.Cmd)
	return strings.TrimSpace(help)
}
