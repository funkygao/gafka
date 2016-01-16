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

	root    string
	rsyslog bool
}

func (this *Deploy) Run(args []string) (exitCode int) {
	cmdFlags := flag.NewFlagSet("deploy", flag.ContinueOnError)
	cmdFlags.Usage = func() { this.Ui.Output(this.Help()) }
	cmdFlags.StringVar(&this.root, "p", defaultPrefix, "")
	cmdFlags.BoolVar(&this.rsyslog, "rsyslog", true, "")
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

	for _, errcode := range []int{400, 401, 500, 502, 503, 504} {
		this.touch(fmt.Sprintf("%s/logs/%d.http", this.root, errcode))
	}

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

	if this.rsyslog {
		this.Ui.Info("install and setup rsyslog for haproxy")

		this.configRsyslog()
	}

	return
}

func (this *Deploy) configRsyslog() {
	this.Ui.Output(fmt.Sprintf(`
vim  /etc/rsyslog.d/haproxy.conf
$ModLoad imudp
$UDPServerRun 514
local3.*     /var/log/haproxy.log

vim /etc/sysconfig/rsyslog
SYSLOGD_OPTIONS=”-c 2 -r -m 0″
#-c 2 使用兼容模式，默认是 -c 5
#-r 开启远程日志
#-m 0 标记时间戳。单位是分钟，为0时，表示禁用该功能	
		`))
}

func (this *Deploy) touch(fn string) {
	err := ioutil.WriteFile(fn, []byte{}, 0644)
	swalllow(err)
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

    -rsyslog
      Display rsyslog integration with haproxy

`, this.Cmd, this.Cmd, defaultPrefix)
	return strings.TrimSpace(help)
}
