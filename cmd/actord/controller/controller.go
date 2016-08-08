package controller

import (
	"fmt"
	"os"
	"strings"
	"sync"
	"time"

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
	mc           *mysql.MysqlCluster
	quiting      chan struct{}
	auditor      log.Logger

	ident   string // cache
	shortId string // cache
}

func New(zkzone *zk.ZkZone) Controller {
	// mysql cluster config
	b, err := zkzone.KatewayJobClusterConfig()
	if err != nil {
		panic(err)
	}
	var mcc = &config.ConfigMysql{}
	if err = mcc.From(b); err != nil {
		panic(err)
	}

	this := &controller{
		quiting:      make(chan struct{}),
		orchestrator: zkzone.NewOrchestrator(),
		mc:           mysql.New(mcc),
	}
	this.ident, err = this.generateIdent()
	if err != nil {
		panic(err)
	}

	// hostname:95f333fb-731c-9c95-c598-8d6b99a9ec7d
	p := strings.SplitN(this.ident, ":", 2)
	this.shortId = fmt.Sprintf("%s:%s", p[0], this.ident[strings.LastIndexByte(this.ident, '-')+1:])
	this.setupAuditor()

	return this
}

func (this *controller) ServeForever() (err error) {
	log.Info("controller[%s] starting", this.Id())

	if err = this.orchestrator.RegisterActor(this.Id()); err != nil {
		return err
	}
	defer this.orchestrator.ResignActor(this.Id())

REBALANCE:
	for {
		// each loop is a new rebalance process

		select {
		case <-this.quiting:
			break REBALANCE
		default:
		}

		jobQueues, jobQueueChanges, err := this.orchestrator.WatchJobQueues()
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
		decision := assignJobsToActors(actors, jobQueues)
		myJobQueues := decision[this.Id()]

		if len(myJobQueues) == 0 {
			// standby mode
			log.Warn("decided: no job assignment, awaiting rebalance...")
		} else {
			log.Info("decided: claiming %d/%d job queues", len(jobQueues), len(myJobQueues))
		}

		var (
			wg            sync.WaitGroup
			workerStopper = make(chan struct{})
		)
		for _, jobQueue := range myJobQueues {
			wg.Add(1)
			log.Trace("invoking worker for %s", jobQueue)
			go this.invokeWorker(jobQueue, &wg, workerStopper)
		}

		select {
		case <-this.quiting:
			close(workerStopper)
			wg.Wait()
			break REBALANCE

		case <-jobQueueChanges:
			log.Info("rebalance due to job queue changes")

			close(workerStopper)
			wg.Wait()

		case <-actorChanges:
			log.Info("rebalance due to actor changes")

			stillAlive, err := this.orchestrator.ActorRegistered(this.Id())
			if err != nil {
				log.Error(err)
			} else if !stillAlive {
				this.orchestrator.RegisterActor(this.Id())
			}

			close(workerStopper)
			wg.Wait()
		}
	}

	log.Info("controller[%s] stopped", this.Id())
	return nil
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
