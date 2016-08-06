package controller

import (
	"time"

	"github.com/funkygao/gafka/cmd/actord/worker"
	log "github.com/funkygao/log4go"
)

func (this *controller) invokeWorker(jobQueue string, stopper <-chan struct{}) {
	defer this.wg.Done()

	for retries := 0; retries < 3; retries++ {
		log.Trace("%s claiming owner of %s #%d", this.id(), jobQueue, retries)
		if err := this.orchestrator.ClaimJobQueue(jobQueue); err == nil {
			log.Info("%s claimed owner of %s", this.id(), jobQueue)
			break
		} else {
			time.Sleep(time.Second)
		}
	}

	defer func(q string) {
		this.orchestrator.ReleaseJobQueue(q)
		log.Info("%s de-claimed owner of %s", this.id(), q)
	}(jobQueue)

	cluster, err := this.orchestrator.JobQueueCluster(jobQueue)
	if err != nil {
		log.Error(err)
	}

	w := worker.New(cluster, jobQueue, this.mc, stopper)
	w.Run()

}
