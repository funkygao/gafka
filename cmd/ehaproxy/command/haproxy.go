package command

import (
	"fmt"
	"os"
	"os/exec"
	"text/template"

	log "github.com/funkygao/log4go"
)

//go:generate go-bindata -nomemcopy -pkg command templates/...

type BackendServers struct {
	CpuNum      int
	HaproxyRoot string

	Pub []Backend
	Sub []Backend
	Man []Backend
}

func (this *BackendServers) reset() {
	this.Pub = make([]Backend, 0)
	this.Sub = make([]Backend, 0)
	this.Man = make([]Backend, 0)
}

func (this *BackendServers) sort() {
	this.Pub = sortBackendByName(this.Pub)
	this.Sub = sortBackendByName(this.Sub)
	this.Man = sortBackendByName(this.Man)
}

type Backend struct {
	Name string
	Addr string
	Cpu  string
}

func (this *Start) createConfigFile(servers BackendServers) error {
	servers.sort()
	log.Info("backends: %+v", servers)

	tmpFile := fmt.Sprintf("%s.tmp", configFile)
	cfgFile, err := os.Create(tmpFile)
	if err != nil {
		return err
	}
	defer cfgFile.Close()

	b, _ := Asset("templates/haproxy.tpl")
	t := template.Must(template.New("haproxy").Parse(string(b)))

	err = t.Execute(cfgFile, servers)
	if err != nil {
		return err
	}

	return os.Rename(tmpFile, configFile)
}

func (this *Start) reloadHAproxy() (err error) {
	var cmd *exec.Cmd = nil
	waitStartCh := make(chan struct{})
	if this.starting {
		log.Info("haproxy starting")
		cmd = exec.Command(this.command, "-f", configFile) // TODO use absolute path
		this.starting = false

		go func() {
			<-waitStartCh
			log.Info("haproxy started")
			if err := cmd.Wait(); err != nil {
				log.Error("haproxy: %v", err)
			}
		}()
	} else {
		shellScript := fmt.Sprintf("%s -f %s/%s -sf `cat %s/%s`",
			this.command, this.root, configFile, this.root, haproxyPidFile)
		log.Info("haproxy reloading: %s", shellScript)
		cmd = exec.Command("/bin/sh", "-c", shellScript)
		go func() {
			<-waitStartCh
			log.Info("haproxy reloaded")
			if err := cmd.Wait(); err != nil {
				log.Error("haproxy: %v", err)
			}
		}()
	}

	if err = cmd.Start(); err == nil {
		waitStartCh <- struct{}{}
	}

	return err
}
