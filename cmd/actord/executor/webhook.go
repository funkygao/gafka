package executor

import (
	log "github.com/funkygao/log4go"
)

type WebhookExecutor struct {
	parentId       string // controller short id
	cluster, topic string
	stopper        <-chan struct{}
	auditor        log.Logger
}

func NewWebhookExecutor(parentId, cluster, topic string, stopper <-chan struct{}, auditor log.Logger) *WebhookExecutor {
	this := &WebhookExecutor{
		parentId: parentId,
		cluster:  cluster,
		topic:    topic,
		stopper:  stopper,
		auditor:  auditor,
	}
	return this
}

func (this *WebhookExecutor) Run() {

}
