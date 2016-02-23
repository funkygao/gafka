package command

import (
	"fmt"
	"strings"

	"github.com/funkygao/gocli"
)

type Rsyslog struct {
	Ui  cli.Ui
	Cmd string
}

func (this *Rsyslog) Run(args []string) (exitCode int) {
	guide := `
vim /etc/logrotate.d/haproxy
/varlog/haproxy*.log
{
    rotate 4
    daily
    missingok
    notifempty
    compress
    delaycompress
    sharedscripts
    postrotate
        reload rsyslog >/dev/null 2>&1 || true
    endscript
}

vim  /etc/rsyslog.d/haproxy.conf
local0.*  /var/log/haproxy.log
local1.*  /var/log/haproxy.log
local3.*  /var/log/haproxy.log

vim /etc/sysconfig/rsyslog
SYSLOGD_OPTIONS=”-c 2 -r -m 0″
#-c 2 使用兼容模式，默认是 -c 5
#-r 开启远程日志
#-m 0 标记时间戳。单位是分钟，为0时，表示禁用该功能	

service rsyslog restart
`
	this.Ui.Output(guide)

	return
}

func (this *Rsyslog) Synopsis() string {
	return fmt.Sprintf("How to setup rsyslog for ehaproxy")
}

func (this *Rsyslog) Help() string {
	help := fmt.Sprintf(`
Usage: %s rsyslog

    How to setup rsyslog for ehaproxy


`, this.Cmd, this.Cmd, defaultPrefix)
	return strings.TrimSpace(help)
}
