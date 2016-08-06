package controller

import (
	"sync"
	"time"

	"github.com/funkygao/gafka/cmd/actord/worker"
	log "github.com/funkygao/log4go"
)

func (this *controller) invokeWorker(jobQueue string, wg *sync.WaitGroup, stopper <-chan struct{}) {
	defer wg.Done()

	for retries := 0; retries < 3; retries++ {
		log.Trace("claiming owner of %s #%d", jobQueue, retries)
		if err := this.orchestrator.ClaimJobQueue(jobQueue); err == nil {
			log.Info("claimed owner of %s", jobQueue)
			break
		} else {
			time.Sleep(time.Second)
		}
	}

	defer func(q string) {
		this.orchestrator.ReleaseJobQueue(q)
		log.Info("de-claimed owner of %s", q)
	}(jobQueue)

	cluster, err := this.orchestrator.JobQueueCluster(jobQueue)
	if err != nil {
		log.Error(err)
	}

	w := worker.New(this.shortId, cluster, jobQueue, this.mc, stopper)
	w.Run()

}
