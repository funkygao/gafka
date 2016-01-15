package main

import (
	"fmt"
	"log"
	"os"
	"os/exec"
	"strconv"
	"text/template"
)

//go:generate go-bindata -nomemcopy -pkg main etc/...

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

func createConfigFile(servers BackendServers, templateFile, outputFile string) error {
	tmp := fmt.Sprintf("%s.tmp", outputFile)
	cfgFile, _ := os.Create(tmp)
	defer cfgFile.Close()

	b, _ := Asset("etc/haproxy.tpl")
	t := template.Must(template.New("haproxy").Parse(string(b)))

	err := t.Execute(cfgFile, servers)
	os.Rename(tmp, outputFile)
	return err
}

func reloadHAproxy(command, configFile string) error {
	var cmd *exec.Cmd = nil
	if pid == -1 {
		log.Println("Starting haproxy")
		cmd = exec.Command(command, "-f", configFile)
		go cmd.Wait()
	} else {
		log.Println("Restarting haproxy")
		cmd = exec.Command(command, "-f", configFile, "-sf", strconv.Itoa(pid))
		go cmd.Wait()
	}

	err := cmd.Start()
	if err == nil {
		pid = cmd.Process.Pid
		log.Printf("haproxy pid: %d", pid)
	}
	return err
}
