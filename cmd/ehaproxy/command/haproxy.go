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
	CpuNum      int
	HaproxyRoot string
	LogDir      string

	Pub []Backend
	Sub []Backend
	Man []Backend
}

func (this *BackendServers) reset() {
	this.Pub = make([]Backend, 0)
	this.Sub = make([]Backend, 0)
	this.Man = make([]Backend, 0)
}

type Backend struct {
	Name string
	Addr string
}

func (this *Start) createConfigFile(servers BackendServers) error {
	log.Info("backends: %+v", servers)

	tmpFile := fmt.Sprintf("%s.tmp", configFile)
	cfgFile, err := os.Create(tmpFile)
	if err != nil {
		return err
	}

	b, _ := Asset("templates/haproxy.tpl")
	t := template.Must(template.New("haproxy").Parse(string(b)))

	err = t.Execute(cfgFile, servers)
	cfgFile.Close()

	os.Rename(tmpFile, configFile)
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
			if err := cmd.Wait(); err != nil {
				log.Error("haproxy: %v", err)
			}
		}()
	} else {
		log.Info("reloading haproxy")
		cmd = exec.Command(this.command, "-f", configFile, "-sf", strconv.Itoa(this.pid))
		go func() {
			<-waitStartCh
			if err := cmd.Wait(); err != nil {
				log.Error("haproxy: %v", err)
			}
		}()
	}

	if err = cmd.Start(); err == nil {
		waitStartCh <- struct{}{}

		this.pid = cmd.Process.Pid
		log.Info("haproxy started with pid: %d", this.pid)
	}

	return err
}
