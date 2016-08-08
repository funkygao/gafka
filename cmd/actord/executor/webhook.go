package executor

import (
	"sync"
	"time"

	"github.com/Shopify/sarama"
	"github.com/funkygao/gafka/cmd/kateway/meta"
	"github.com/funkygao/gafka/zk"
	"github.com/funkygao/golib/breaker"
	"github.com/funkygao/kafka-cg/consumergroup"
	log "github.com/funkygao/log4go"
)

const (
	groupName = "_webhook"
)

type WebhookExecutor struct {
	parentId       string // controller short id
	cluster, topic string
	endpoints      []string
	stopper        <-chan struct{}
	auditor        log.Logger

	circuits map[string]breaker.Consecutive
	fetcher  *consumergroup.ConsumerGroup
	msgCh    chan *sarama.ConsumerMessage
}

func NewWebhookExecutor(parentId, cluster, topic string, endpoints []string,
	stopper <-chan struct{}, auditor log.Logger) *WebhookExecutor {
	this := &WebhookExecutor{
		parentId:  parentId,
		cluster:   cluster,
		topic:     topic,
		stopper:   stopper,
		endpoints: endpoints,
		auditor:   auditor,
		msgCh:     make(chan *sarama.ConsumerMessage, 20),
		circuits:  make(map[string]breaker.Consecutive, len(endpoints)),
	}

	for _, ep := range endpoints {
		this.circuits[ep] = breaker.Consecutive{
			RetryTimeout:     time.Second * 5,
			FailureAllowance: 5,
		}
	}

	return this
}

func (this *WebhookExecutor) Run() {
	// TODO watch the znode change, its endpoint might change any time

	cf := consumergroup.NewConfig()
	cf.Net.DialTimeout = time.Second * 10
	cf.Net.WriteTimeout = time.Second * 10
	cf.Net.ReadTimeout = time.Second * 10
	cf.ChannelBufferSize = 100
	cf.Consumer.Return.Errors = true
	cf.Consumer.MaxProcessingTime = time.Second * 2 // chan recv timeout
	cf.Zookeeper.Chroot = meta.Default.ZkChroot(this.cluster)
	cf.Zookeeper.Timeout = zk.DefaultZkSessionTimeout()
	cf.Offsets.CommitInterval = time.Minute
	cf.Offsets.ProcessingTimeout = time.Second
	cf.Offsets.ResetOffsets = false
	cf.Offsets.Initial = sarama.OffsetOldest
	cg, err := consumergroup.JoinConsumerGroup(groupName, []string{this.topic}, meta.Default.ZkAddrs(), cf)
	if err == nil {
		log.Error("%s stopped: %s", this.topic, err)
		return
	}
	this.fetcher = cg

	var wg sync.WaitGroup
	for i := 0; i < 1; i++ {
		wg.Add(1)
		go this.pump(&wg)
	}

	for {
		select {
		case <-this.stopper:
			log.Debug("%s stopping", this.topic)
			wg.Wait()
			return

		case err := <-cg.Errors():
			log.Error("%s %s", this.topic, err)
			// TODO

		case msg := <-cg.Messages():
			this.msgCh <- msg
		}

	}

}

func (this *WebhookExecutor) pump(wg *sync.WaitGroup) {
	defer wg.Done()

	for {
		select {
		case <-this.stopper:
			return

		case msg := <-this.msgCh:
			for _, ep := range this.endpoints {
				this.pushToEndpoint(msg, ep)
			}

			this.fetcher.CommitUpto(msg)
		}
	}

}

func (this *WebhookExecutor) pushToEndpoint(msg *sarama.ConsumerMessage, uri string) {
	log.Debug("%s sending[%s] %s", this.topic, uri, string(msg.Value))
}
