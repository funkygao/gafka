package gateway

import (
	"github.com/funkygao/gafka/registry"
	log "github.com/funkygao/log4go"
	zklib "github.com/samuel/go-zookeeper/zk"
)

func (this *Gateway) healthCheck(birthCry chan struct{}) {
	if registry.Default == nil {
		close(birthCry)
		return
	}

	evtCh, ok := this.zkzone.SessionEvents()
	if !ok {
		panic("someone else is stealing my zkzone events?")
	}

	// during connecting phase, the following events are fired:
	// StateConnecting -> StateConnected -> StateHasSession
	firstHandShaked := false
	for {
		select {
		case <-this.shutdownCh:
			return

		case evt, ok := <-evtCh:
			if !ok {
				return
			}
			if evt.Path != "" {
				continue
			}

			log.Warn("zk jitter: %+v", evt)

			if evt.State == zklib.StateHasSession {
				log.Trace("registering kateway[%s] in %s...", this.id, registry.Default.Name())
				registry.Default.Register(this.id, this.InstanceInfo())
				log.Trace("registered kateway[%s] in %s", this.id, registry.Default.Name())

				if !firstHandShaked {
					firstHandShaked = true
					close(birthCry)
				}

			}
		}
	}
}
