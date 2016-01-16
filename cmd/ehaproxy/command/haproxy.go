package command

import (
	"fmt"
	"os"
	"os/exec"
	"strconv"
	"text/template"

	log "github.com/funkygao/log4go"
)

//go:generate go-bindata -nomemcopy -pkg command templates/...

type BackendServers struct {
	Pub []Backend
	Sub []Backend
	Man []Backend
}

type Backend struct {
	Name string
	Ip   string
	Port string
}

var (
	tpl *template.Template = nil
	pid int                = -1
)

func (this *Start) createConfigFile(servers BackendServers) error {
	tmp := fmt.Sprintf("%s.tmp", configFile)
	cfgFile, _ := os.Create(tmp)

	b, _ := Asset("templates/haproxy.tpl")
	t := template.Must(template.New("haproxy").Parse(string(b)))

	err := t.Execute(cfgFile, servers)
	cfgFile.Close()

	os.Rename(tmp, configFile)
	return err
}

func (this *Start) reloadHAproxy() (err error) {
	command := fmt.Sprintf("%s/sbin/haproxy", this.root)
	var cmd *exec.Cmd = nil
	waitStartCh := make(chan struct{})
	if pid == -1 {
		log.Info("Starting haproxy")
		cmd = exec.Command(command, "-f", configFile)
		go func() {
			<-waitStartCh
			if err := cmd.Wait(); err != nil {
				log.Error(err)
			}
		}()
	} else {
		log.Info("Restarting haproxy")
		cmd = exec.Command(command, "-f", configFile, "-sf", strconv.Itoa(pid))
		go func() {
			<-waitStartCh
			if err := cmd.Wait(); err != nil {
				log.Error(err)
			}
		}()
	}

	if err = cmd.Start(); err == nil {
		waitStartCh <- struct{}{}

		pid = cmd.Process.Pid
		log.Info("haproxy pid: %d", pid)
	}

	return err
}
