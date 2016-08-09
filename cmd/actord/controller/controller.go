package controller

import (
	"encoding/json"
	"fmt"
	"os"
	"strings"

	"github.com/funkygao/fae/config"
	"github.com/funkygao/fae/servant/mysql"
	"github.com/funkygao/gafka/zk"
	"github.com/funkygao/golib/sync2"
	log "github.com/funkygao/log4go"
	"github.com/hashicorp/go-uuid"
)

type Controller interface {
	RunForever() error
	Stop()

	Id() string
}

type controller struct {
	orchestrator *zk.Orchestrator
	mc           *mysql.MysqlCluster
	quiting      chan struct{}
	auditor      log.Logger

	ListenAddr string `json:"addr"`

	ActorN, JobQueueN, WebhookN    sync2.AtomicInt32
	JobExecutorN, WebhookExecutorN sync2.AtomicInt32

	ident   string // cache
	shortId string // cache
}

func New(zkzone *zk.ZkZone, listenAddr string) Controller {
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
		ListenAddr:   listenAddr,
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

func (this *controller) RunForever() (err error) {
	log.Info("controller[%s] starting", this.Id())

	if err = this.orchestrator.RegisterActor(this.Id(), this.Bytes()); err != nil {
		return err
	}
	defer this.orchestrator.ResignActor(this.Id())

	go this.runWebServer()

	jobDispatchQuit := make(chan struct{})
	go this.dispatchJobQueues(jobDispatchQuit)

	webhookDispatchQuit := make(chan struct{})
	go this.dispatchWebhooks(webhookDispatchQuit)

	select {
	case <-jobDispatchQuit:
		log.Warn("dispatchJobQueues quit")

	case <-webhookDispatchQuit:
		log.Warn("dispatchWebhooks quit")
	}

	return
}

func (this *controller) Stop() {
	close(this.quiting)
}

func (this *controller) Id() string {
	return this.ident
}

func (this *controller) Bytes() []byte {
	b, _ := json.Marshal(this)
	return b
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
