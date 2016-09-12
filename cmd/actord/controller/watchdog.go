package controller

import (
	"fmt"

	log "github.com/funkygao/log4go"
	zklib "github.com/samuel/go-zookeeper/zk"
)

func (this *controller) watchZk() {
	evtCh, ok := this.orchestrator.SessionEvents()
	if !ok {
		panic("someone else is stealing my zk events?")
	}

	// during connecting phase, the following events are fired:
	// StateConnecting -> StateConnected -> StateHasSession
	firstHandShaked := false
	for {
		select {
		case <-this.quiting:
			return

		case evt := <-evtCh:
			if !firstHandShaked {
				if evt.State == zklib.StateHasSession {
					firstHandShaked = true
				}

				continue
			}

			log.Warn("zk jitter: %+v", evt)

			if evt.State == zklib.StateHasSession {
				log.Warn("zk reconnected after session lost, watcher/ephemeral might be lost")

				registered, err := this.orchestrator.ActorRegistered(this.Id())
				if err != nil {
					log.Error("registry: %s", err)
				} else if !registered {
					if err = this.orchestrator.RegisterActor(this.Id(), this.Bytes()); err != nil {
						log.Error("registry: %s", err)
					} else {
						log.Info("registry re-register controller[%s] ok", this.ident)
					}
				} else {
					log.Info("registry lucky, ephemeral still present")
				}

				this.orchestrator.CallSOS(fmt.Sprintf("actord[%s]", this.Id()), "zk session expired")
			}
		}
	}
}
