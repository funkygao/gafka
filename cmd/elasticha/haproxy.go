package main

import (
	"log"
	"os"
	"os/exec"
	"strconv"
	"text/template"
)

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
	cfgFile, _ := os.Create(outputFile)
	defer cfgFile.Close()

	if tpl == nil {
		var err error = nil
		tpl, err = template.ParseFiles(templateFile)
		if err != nil {
			return err
		}
	}

	return tpl.Execute(cfgFile, servers)
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
