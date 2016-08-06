package controller

import (
	"fmt"
	"os"
	"sync"

	"github.com/funkygao/fae/config"
	"github.com/funkygao/fae/servant/mysql"
	"github.com/funkygao/gafka/zk"
	log "github.com/funkygao/log4go"
	"github.com/hashicorp/go-uuid"
)

type Controller interface {
	ServeForever() error
	Stop()

	Id() string
}

type controller struct {
	orchestrator *zk.Orchestrator
	wg           sync.WaitGroup
	mc           *mysql.MysqlCluster
	quiting      chan struct{}

	ident string
}

func New(zkzone *zk.ZkZone) Controller {
	// mysql cluster config
	var mcc = &config.ConfigMysql{}
	b, err := zkzone.KatewayJobClusterConfig()
	if err != nil {
		panic(err)
	}
	if err = mcc.From(b); err != nil {
		panic(err)
	}
	log.Debug("%+v", *mcc)

	this := &controller{
		quiting:      make(chan struct{}),
		orchestrator: zkzone.NewOrchestrator(),
		mc:           mysql.New(mcc),
	}
	this.ident, err = this.generateIdent()
	if err != nil {
		panic(err)
	}
	return this
}

func (this *controller) ServeForever() (err error) {
	if err = this.orchestrator.RegisterActor(this.Id()); err != nil {
		return err
	}
	defer this.orchestrator.ResignActor(this.Id())

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
		myJobQueues := decision[this.Id()]

		if len(myJobQueues) == 0 {
			// standby mode
			log.Warn("no job assignment, awaiting rebalance...")
		}

		workerStopper := make(chan struct{})
		for _, jobQueue := range myJobQueues {
			this.wg.Add(1)
			go this.invokeWorker(jobQueue, workerStopper)
		}

		select {
		case <-this.quiting:
			close(workerStopper)
			//return FIXME

		case <-jobQueueChanges:
			close(workerStopper)
			this.wg.Wait()

		case <-actorChanges:
			stillAlive, err := this.orchestrator.ActorRegistered(this.Id())
			if err != nil {
				log.Error(err)
			} else if !stillAlive {
				this.orchestrator.RegisterActor(this.Id())
			}

			close(workerStopper)
			this.wg.Wait()
		}
	}

}

func (this *controller) Stop() {
	close(this.quiting)
}

func (this *controller) Id() string {
	return this.ident
}

func (this *controller) generateIdent() (string, error) {
	uuid, err := uuid.GenerateUUID()
	if err != nil {
		return "", err
	}

	hostname, err := os.Hostname()
	if err != nil {
		return "", err
	}

	return fmt.Sprintf("%s:%s", hostname, uuid), nil
}
