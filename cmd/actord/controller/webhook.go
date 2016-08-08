package controller

import (
	"sync"
	"time"

	"github.com/funkygao/gafka/cmd/actord/executor"
	"github.com/funkygao/gafka/zk"
	log "github.com/funkygao/log4go"
)

func (this *controller) dispatchWebhooks(quit chan<- struct{}) {
	defer close(quit)

REBALANCE:
	for {
		// each loop is a new rebalance process

		select {
		case <-this.quiting:
			break REBALANCE
		default:
		}

		webhooks, webhookChanges, err := this.orchestrator.WatchResources(zk.PubsubWebhooks)
		if err != nil {
			log.Error("watch webhooks: %s", err)
			time.Sleep(time.Second)
			continue REBALANCE
		}

		actors, actorChanges, err := this.orchestrator.WatchActors()
		if err != nil {
			log.Error("watch actors: %s", err)
			time.Sleep(time.Second)
			continue REBALANCE
		}

		log.Info("deciding: found %d webhooks, %d actors", len(webhooks), len(actors))
		decision := assignResourcesToActors(actors, webhooks)
		myWebhooks := decision[this.Id()]

		if len(myWebhooks) == 0 {
			// standby mode
			log.Warn("decided: no webhook assignment, awaiting rebalance...")
		} else {
			log.Info("decided: claiming %d/%d webhooks", len(webhooks), len(myWebhooks))
		}

		var (
			wg               sync.WaitGroup
			executorsStopper = make(chan struct{})
		)
		for _, topic := range myWebhooks {
			wg.Add(1)
			log.Trace("invoking executor for %s", topic)
			go this.invokeWebhookExecutor(topic, &wg, executorsStopper)
		}

		select {
		case <-this.quiting:
			close(executorsStopper)
			wg.Wait()
			break REBALANCE

		case <-webhookChanges:
			log.Info("rebalance due to webhooks changes")

			close(executorsStopper)
			wg.Wait()

		case <-actorChanges:
			log.Info("rebalance due to actor changes")

			stillAlive, err := this.orchestrator.ActorRegistered(this.Id())
			if err != nil {
				log.Error(err)
			} else if !stillAlive {
				this.orchestrator.RegisterActor(this.Id())
			}

			close(executorsStopper)
			wg.Wait()
		}
	}

	log.Info("controller[%s] dispatchWebhooks stopped", this.Id())
	return
}

func (this *controller) invokeWebhookExecutor(topic string, wg *sync.WaitGroup, stopper <-chan struct{}) {
	defer wg.Done()

	var err error
	for retries := 0; retries < 3; retries++ {
		log.Trace("claiming owner of %s #%d", topic, retries)
		if err = this.orchestrator.ClaimResource(this.Id(), zk.PubsubWebhookOwners, topic); err == nil {
			log.Info("claimed owner of %s", topic)
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

	defer func(topic string) {
		this.orchestrator.ReleaseResource(this.Id(), zk.PubsubWebhookOwners, topic)
		log.Info("de-claimed owner of %s", topic)
	}(topic)

	info, err := this.orchestrator.WebhookInfo(topic)
	if err != nil {
		log.Error(err)
		return
	}

	exe := executor.NewWebhookExecutor(this.shortId, info.Cluster, topic, info.Endpoints, stopper, this.auditor)
	exe.Run()

}
