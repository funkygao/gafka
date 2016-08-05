package controller

import (
	"sync"

	"github.com/funkygao/fae/config"
	"github.com/funkygao/fae/servant/mysql"
	"github.com/funkygao/gafka/zk"
	log "github.com/funkygao/log4go"
)

type Controller interface {
	ServeForever() error
	Stop()
}

type controller struct {
	orchestrator *zk.Orchestrator
	wg           sync.WaitGroup
	mc           *mysql.MysqlCluster
	quiting      chan struct{}
}

func New(zkzone *zk.ZkZone) Controller {
	var mcc = &config.ConfigMysql{}
	b, err := zkzone.KatewayJobClusterConfig()
	if err != nil {
		panic(err)
	}
	if err = mcc.From(b); err != nil {
		panic(err)
	}
	log.Debug("%+v", *mcc)

	return &controller{
		quiting:      make(chan struct{}),
		orchestrator: zkzone.NewOrchestrator(),
		mc:           mysql.New(mcc),
	}
}

func (this *controller) ServeForever() (err error) {
	id := this.id()
	if err = this.orchestrator.RegisterActor(id); err != nil {
		return err
	}

	for {
		// each loop is a new rebalance process

		select {
		case <-this.quiting:
			return nil
		default:
		}

		jobQueues, jobQueueChanges, err := this.orchestrator.WatchJobQueues()
		if err != nil {
			return err
		}

		actors, actorChanges, err := this.orchestrator.WatchActors()
		if err != nil {
			return err
		}

		log.Info("rebalancing, found %d job queues, %d actors", len(jobQueues), len(actors))
		decision := assignJobsToActors(actors, jobQueues)
		myJobQueues := decision[id]

		if len(myJobQueues) == 0 {
			// standby mode
			log.Warn("no job assignment, awaiting rebalance...")
		}

		workerStopper := make(chan struct{})
		for _, jobQueue := range myJobQueues {
			this.wg.Add(1)
			go this.startWorker(jobQueue, workerStopper)
		}

		select {
		case <-this.quiting:
			close(workerStopper)
			//return FIXME

		case <-jobQueueChanges:
			close(workerStopper)
			this.wg.Wait()

		case <-actorChanges:
			stillAlive, err := this.orchestrator.ActorRegistered(id)
			if err != nil {
				log.Error(err)
			} else if !stillAlive {
				this.orchestrator.RegisterActor(id)
			}

			close(workerStopper)
			this.wg.Wait()
		}
	}

}

func (this *controller) Stop() {
	close(this.quiting)
}

func (this *controller) id() string {
	return "actor1"
}
