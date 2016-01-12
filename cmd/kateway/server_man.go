package main

import (
	"strings"

	"github.com/funkygao/etclib"
	"github.com/funkygao/gafka/ctx"
	zkr "github.com/funkygao/gafka/registry/zk"
)

// management server
type manServer struct {
	*webServer
}

func newManServer(httpAddr, httpsAddr string, maxClients int, gw *Gateway) *manServer {
	this := &manServer{
		webServer: newWebServer("man", httpAddr, httpsAddr, maxClients, gw),
	}

	if options.clusterAware {
		<-this.peersHousekeep()
	}

	return this
}

func (this *manServer) peersHousekeep() chan struct{} {
	r := make(chan struct{})
	if err := etclib.Dial(strings.Split(ctx.ZoneZkAddrs(this.gw.zone), ",")); err != nil {
		panic(err)
	}

	ch := make(chan []string, 10)
	go etclib.WatchChildren(zkr.Root(this.gw.zone), ch)
	close(r)

	go func() {
		for {
			select {
			case <-ch:
				this.gw.pubPeersLock.Lock()
				// get the kateway nodes and update this.gw.pubPeers
				this.gw.pubPeersLock.Unlock()
			}
		}
	}()

	return r
}
