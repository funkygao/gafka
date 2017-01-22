package gateway

import (
	"github.com/funkygao/gafka/registry"
	log "github.com/funkygao/log4go"
	zklib "github.com/samuel/go-zookeeper/zk"
)

func (this *Gateway) healthCheck() {
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

			if !firstHandShaked {
				if evt.State == zklib.StateHasSession {
					firstHandShaked = true
				}

				continue
			}

			log.Warn("zk jitter: %+v", evt)

			if evt.State == zklib.StateHasSession && registry.Default != nil {
				log.Warn("re-registering kateway[%s] in %s...", this.id, registry.Default.Name())
				registry.Default.Register(this.id, this.InstanceInfo())
				log.Info("re-register kateway[%s] in %s done", this.id, registry.Default.Name())
			}
		}
	}
}
