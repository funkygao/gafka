package controller

import (
	"sync"
	"time"

	"github.com/funkygao/gafka/cmd/actord/executor"
	"github.com/funkygao/gafka/zk"
	log "github.com/funkygao/log4go"
)

func (this *controller) dispatchJobQueues(quit chan<- struct{}) {
	defer close(quit)

REBALANCE:
	for {
		// each loop is a new rebalance process

		select {
		case <-this.quiting:
			break REBALANCE
		default:
		}

		jobQueues, jobQueueChanges, err := this.orchestrator.WatchResources(zk.PubsubJobQueues)
		if err != nil {
			log.Error("watch job queues: %s", err)
			time.Sleep(time.Second)
			continue REBALANCE
		}

		actors, actorChanges, err := this.orchestrator.WatchActors()
		if err != nil {
			log.Error("watch actors: %s", err)
			time.Sleep(time.Second)
			continue REBALANCE
		}

		log.Info("deciding: found %d job queues, %d actors", len(jobQueues), len(actors))
		decision := assignResourcesToActors(actors, jobQueues)
		myJobQueues := decision[this.Id()]

		if len(myJobQueues) == 0 {
			// standby mode
			log.Warn("decided: no job assignment, awaiting rebalance...")
		} else {
			log.Info("decided: claiming %d/%d job queues", len(jobQueues), len(myJobQueues))
		}

		var (
			wg              sync.WaitGroup
			executorStopper = make(chan struct{})
		)
		for _, jobQueue := range myJobQueues {
			wg.Add(1)
			log.Trace("invoking executor for %s", jobQueue)
			go this.invokeJobExexutor(jobQueue, &wg, executorStopper)
		}

		select {
		case <-this.quiting:
			close(executorStopper)
			wg.Wait()
			break REBALANCE

		case <-jobQueueChanges:
			log.Info("rebalance due to job queue changes")

			close(executorStopper)
			wg.Wait()

		case <-actorChanges:
			log.Info("rebalance due to actor changes")

			stillAlive, err := this.orchestrator.ActorRegistered(this.Id())
			if err != nil {
				log.Error(err)
			} else if !stillAlive {
				this.orchestrator.RegisterActor(this.Id())
			}

			close(executorStopper)
			wg.Wait()
		}
	}

	log.Info("controller[%s] dispatchJobQueues stopped", this.Id())
	return
}

func (this *controller) invokeJobExexutor(jobQueue string, wg *sync.WaitGroup, stopper <-chan struct{}) {
	defer wg.Done()

	var err error
	for retries := 0; retries < 3; retries++ {
		log.Trace("claiming owner of %s #%d", jobQueue, retries)
		if err = this.orchestrator.ClaimResource(this.Id(), zk.PubsubJobQueueOwners, jobQueue); err == nil {
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
		this.orchestrator.ReleaseResource(this.Id(), zk.PubsubJobQueueOwners, q)
		log.Info("de-claimed owner of %s", q)
	}(jobQueue)

	cluster, err := this.orchestrator.JobQueueCluster(jobQueue)
	if err != nil {
		log.Error(err)
	}

	exe := executor.NewJobExecutor(this.shortId, cluster, jobQueue, this.mc, stopper, this.auditor)
	exe.Run()

}
