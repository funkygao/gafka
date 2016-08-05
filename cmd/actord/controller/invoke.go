package controller

import (
	"github.com/funkygao/gafka/cmd/actord/worker"
	log "github.com/funkygao/log4go"
)

func (this *controller) startWorker(jobQueue string, stopper <-chan struct{}) {
	defer this.wg.Done()

	for retries := 0; retries < 3; retries++ {
		if err := this.orchestrator.ClaimJobQueue(jobQueue); err == nil {
			log.Info("%s claimed owner of %s", this.id(), jobQueue)
			break
		}

	}
	defer func(q string) {
		this.orchestrator.ReleaseJobQueue(q)
		log.Info("%s de-claimed owner of %s", this.id(), q)
	}(jobQueue)

	cluster, err := this.orchestrator.JobQueueCluster(jobQueue)
	if err != nil {

	}

	w := worker.New(cluster, jobQueue, this.mc, stopper)
	w.Run()

}
