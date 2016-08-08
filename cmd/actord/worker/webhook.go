package worker

import (
	log "github.com/funkygao/log4go"
)

type WebhookWorker struct {
	parentId       string // controller short id
	cluster, topic string
	stopper        <-chan struct{}
	auditor        log.Logger
}

func NewWebhookWorker(parentId, cluster, topic string, stopper <-chan struct{}, auditor log.Logger) *WebhookWorker {
	this := &WebhookWorker{
		parentId: parentId,
		cluster:  cluster,
		topic:    topic,
		stopper:  stopper,
		auditor:  auditor,
	}
	return this
}

func (this *WebhookWorker) Run() {

}
