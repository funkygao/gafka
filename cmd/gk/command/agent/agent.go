package agent

import (
	"os"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/funkygao/gafka/ctx"
	"github.com/funkygao/golib/peer"
	"github.com/funkygao/golib/signal"
	log "github.com/funkygao/log4go"
)

// Agent provides membership, failure detection, and event broadcast.
// TODO LAN+WAN gossip
type Agent struct {
	*peer.Peer

	quit chan struct{}
	once sync.Once
	wg   sync.WaitGroup
}

func New() *Agent {
	return &Agent{
		quit: make(chan struct{}),
	}
}

func (a *Agent) ServeForever(port int, tags []string, seeds ...string) {
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

	apiPort := port + 1
	ip, _ := ctx.LocalIP()

	p, err := peer.New(ip.String(), port, tags, seeds, apiPort, false)
	if err != nil {
		panic(err)
	}
	a.Peer = p

	go a.startAPIServer(apiPort)

	<-a.quit
	log.Close()
}
