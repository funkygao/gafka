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
	HaproxyRoot string
	LogDir      string

	Pub []Backend
	Sub []Backend
	Man []Backend
}

type Backend struct {
	Name string
	Addr string
}

func (this *Start) createConfigFile(servers BackendServers) error {
	tmp := fmt.Sprintf("%s.tmp", configFile)
	cfgFile, _ := os.Create(tmp)

	b, _ := Asset("templates/haproxy.tpl")
	t := template.Must(template.New("haproxy").Parse(string(b)))

	servers.HaproxyRoot = this.root
	servers.LogDir = fmt.Sprintf("%s/logs", this.root)
	err := t.Execute(cfgFile, servers)
	cfgFile.Close()

	os.Rename(tmp, configFile)
	return err
}

func (this *Start) reloadHAproxy() (err error) {
	var cmd *exec.Cmd = nil
	waitStartCh := make(chan struct{})
	if this.pid == -1 {
		log.Info("starting haproxy")
		cmd = exec.Command(this.command, "-f", configFile)
		go func() {
			<-waitStartCh
			log.Info("haproxy started")
			if err := cmd.Wait(); err != nil {
				log.Error(err)
			}
		}()
	} else {
		log.Info("reloading haproxy")
		cmd = exec.Command(this.command, "-f", configFile, "-sf", strconv.Itoa(this.pid))
		go func() {
			<-waitStartCh
			log.Info("haproxy reloaded")
			if err := cmd.Wait(); err != nil {
				log.Error(err)
			}
		}()
	}

	if err = cmd.Start(); err == nil {
		waitStartCh <- struct{}{}

		this.pid = cmd.Process.Pid
	}

	return err
}
