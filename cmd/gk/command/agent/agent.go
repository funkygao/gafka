package agent

import (
	"os"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/funkygao/golib/signal"
	log "github.com/funkygao/log4go"
	"github.com/hashicorp/memberlist"
	"github.com/pborman/uuid"
)

type Agent struct {
	*memberlist.Memberlist
	broadcasts *memberlist.TransmitLimitedQueue

	quit chan struct{}
	once sync.Once
	wg   sync.WaitGroup
}

func New() *Agent {
	return &Agent{
		quit: make(chan struct{}),
	}
}

func (a *Agent) ServeForever(port int, seeds ...string) {
	signal.RegisterHandler(func(sig os.Signal) {
		log.Info("received signal: %s", strings.ToUpper(sig.String()))
		log.Info("quiting...")

		if err := a.Leave(time.Second * 35); err != nil {
			log.Error("leave: %v", err)
		}

		a.once.Do(func() {
			close(a.quit)
		})
	}, syscall.SIGINT, syscall.SIGTERM)

	cf := memberlist.DefaultWANConfig()
	hostname, _ := os.Hostname()
	cf.Name = hostname + "-" + uuid.NewUUID().String()
	cf.BindPort = port
	cf.Delegate = newDelegate(a)

	var err error
	a.Memberlist, err = memberlist.Create(cf)
	if err != nil {
		panic(err)
	}

	if len(seeds) > 0 {
		n, err := a.Join(seeds)
		if err != nil {
			log.Error("join %+v: %s", seeds, err)
		} else {
			log.Info("joined %d seeds", n)
		}
	}

	a.broadcasts = &memberlist.TransmitLimitedQueue{
		NumNodes: func() int {
			return a.NumMembers()
		},
		RetransmitMult: 3,
	}

	log.Info("local node: %+v", a.LocalNode())
	go a.startServer(port + 1)

	<-a.quit
	log.Close()
}

func (a *Agent) xx() {
	a.broadcasts.QueueBroadcast(&broadcast{
		msg:    []byte("d"),
		notify: nil,
	})
}
