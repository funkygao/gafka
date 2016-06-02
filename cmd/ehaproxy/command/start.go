package command

import (
	"bufio"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/funkygao/etclib"
	"github.com/funkygao/gafka/ctx"
	zkr "github.com/funkygao/gafka/registry/zk"
	"github.com/funkygao/gocli"
	gio "github.com/funkygao/golib/io"
	"github.com/funkygao/golib/signal"
	log "github.com/funkygao/log4go"
)

type Start struct {
	Ui  cli.Ui
	Cmd string

	zone       string
	root       string
	command    string
	logfile    string
	pubPort    int
	subPort    int
	manPort    int
	starting   bool
	forwardFor bool
	quitCh     chan struct{}
}

func (this *Start) Run(args []string) (exitCode int) {
	cmdFlags := flag.NewFlagSet("start", flag.ContinueOnError)
	cmdFlags.Usage = func() { this.Ui.Output(this.Help()) }
	cmdFlags.StringVar(&this.logfile, "log", defaultLogfile, "")
	cmdFlags.StringVar(&this.zone, "z", ctx.ZkDefaultZone(), "")
	cmdFlags.StringVar(&this.root, "p", defaultPrefix, "")
	cmdFlags.BoolVar(&this.forwardFor, "forwardfor", false, "")
	cmdFlags.IntVar(&this.pubPort, "pub", 10891, "")
	cmdFlags.IntVar(&this.subPort, "sub", 10892, "")
	cmdFlags.IntVar(&this.manPort, "man", 10893, "")

	if err := cmdFlags.Parse(args); err != nil {
		return 1
	}

	err := os.Chdir(this.root)
	swalllow(err)

	this.command = fmt.Sprintf("%s/sbin/haproxy", this.root)
	if _, err := os.Stat(this.command); err != nil {
		panic(err)
	}

	this.setupLogging(this.logfile, "info", "panic")
	this.starting = true
	this.quitCh = make(chan struct{})
	signal.RegisterSignalsHandler(func(sig os.Signal) {
		log.Info("got signal %s", sig)
		this.shutdown()

		log.Info("removing %s", configFile)
		os.Remove(configFile)

		log.Info("shutdown complete")
	}, syscall.SIGINT, syscall.SIGTERM)

	this.main()

	return
}

func (this *Start) main() {
	ctx.LoadFromHome()

	// TODO zk session timeout
	err := etclib.Dial(strings.Split(ctx.ZoneZkAddrs(this.zone), ","))
	swalllow(err)

	root := zkr.Root(this.zone)
	ch := make(chan []string, 10)
	go etclib.WatchChildren(root, ch)

	var servers = BackendServers{
		CpuNum:      ctx.NumCPU(),
		HaproxyRoot: this.root,
		ForwardFor:  this.forwardFor,
		PubPort:     this.pubPort,
		SubPort:     this.subPort,
		ManPort:     this.manPort,
	}
	var lastInstances []string
	for {
		select {
		case <-this.quitCh:
			time.Sleep(time.Second) // FIXME just wait log flush
			return

		case <-ch:
			kwInstances, err := etclib.Children(root)
			if err != nil {
				log.Error("%s: %v", root, err)
				continue
			}

			log.Info("kateway ids: %+v -> %+v", lastInstances, kwInstances)
			lastInstances = kwInstances

			servers.reset()
			for _, kwId := range kwInstances {
				kwNode := fmt.Sprintf("%s/%s", root, kwId)
				data, err := etclib.Get(kwNode)
				if err != nil {
					log.Error("%s: %v", kwNode, err)
					continue
				}

				info := make(map[string]string)
				if err = json.Unmarshal([]byte(data), &info); err != nil {
					log.Error("%s: %v", data, err)
					continue
				}

				// pub
				if info["pub"] != "" {
					be := Backend{
						Name: "p" + info["id"],
						Addr: info["pub"],
						Cpu:  info["cpu"],
					}
					servers.Pub = append(servers.Pub, be)
				}

				// sub
				if info["sub"] != "" {
					be := Backend{
						Name: "s" + info["id"],
						Addr: info["sub"],
						Cpu:  info["cpu"],
					}
					servers.Sub = append(servers.Sub, be)
				}

				// man
				if info["man"] != "" {
					be := Backend{
						Name: "m" + info["id"],
						Addr: info["man"],
						Cpu:  info["cpu"],
					}
					servers.Man = append(servers.Man, be)
				}

			}

			if err = this.createConfigFile(servers); err != nil {
				log.Error(err)
				continue
			}

			if err = this.reloadHAproxy(); err != nil {
				log.Error("reloading haproxy: %v", err)
				panic(err)
			}

		}
	}
}

func (this *Start) shutdown() {
	// kill haproxy
	log.Info("killling haproxy processes")

	f, e := os.Open(haproxyPidFile)
	swalllow(e)

	reader := bufio.NewReader(f)
	for {
		l, e := gio.ReadLine(reader)
		if e != nil {
			// EOF
			break
		}

		pid, _ := strconv.Atoi(string(l))
		p := &os.Process{
			Pid: pid,
		}
		if err := p.Kill(); err != nil {
			log.Error(err)
		} else {
			log.Info("haproxy[%d] terminated", pid)
		}
	}

	log.Info("removing %s", haproxyPidFile)
	os.Remove(haproxyPidFile)

	close(this.quitCh)
}

func (this *Start) Synopsis() string {
	return fmt.Sprintf("Start %s system on localhost", this.Cmd)
}

func (this *Start) Help() string {
	help := fmt.Sprintf(`
Usage: %s start [options]

    Start %s system on localhost

Options:

    -z zone
      Default %s

    -forwardfor
      Default false.
      If true, haproxy will add X-Forwarded-For http header.

    -pub pub server listen port

    -sub sub server listen port

    -man manager server listen port

    -p directory prefix
      Default %s

    -log log file
      Default %s

`, this.Cmd, this.Cmd, ctx.ZkDefaultZone(), defaultPrefix, defaultLogfile)
	return strings.TrimSpace(help)
}
