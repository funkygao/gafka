package controller

import (
	"sync"
	"time"

	"github.com/funkygao/gafka/cmd/actord/worker"
	"github.com/funkygao/gafka/zk"
	log "github.com/funkygao/log4go"
)

func (this *controller) invokeWorker(jobQueue string, wg *sync.WaitGroup, stopper <-chan struct{}) {
	defer wg.Done()

	var err error
	for retries := 0; retries < 3; retries++ {
		log.Trace("claiming owner of %s #%d", jobQueue, retries)
		if err = this.orchestrator.ClaimJobQueue(this.Id(), jobQueue); err == nil {
			log.Info("claimed owner of %s", jobQueue)
			break
		} else if err == zk.ErrClaimedByOthers {
			log.Error("%s #%d", err, retries)
			time.Sleep(time.Second)
		} else {
			log.Error("%s #%d", err, retries)
			return
		}
	}

	if err != nil {
		// still err(ErrClaimedByOthers) encountered after max retries
		return
	}

	defer func(q string) {
		this.orchestrator.ReleaseJobQueue(this.Id(), q)
		log.Info("de-claimed owner of %s", q)
	}(jobQueue)

	cluster, err := this.orchestrator.JobQueueCluster(jobQueue)
	if err != nil {
		log.Error(err)
	}

	w := worker.New(this.shortId, cluster, jobQueue, this.mc, stopper, this.auditor)
	w.Run()

}
