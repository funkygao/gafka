package executor

import (
	"fmt"
	"io"
	"io/ioutil"
	"net"
	"net/http"
	"strconv"
	"sync"
	"time"

	"github.com/Shopify/sarama"
	"github.com/funkygao/gafka"
	"github.com/funkygao/gafka/cmd/kateway/gateway"
	"github.com/funkygao/gafka/cmd/kateway/manager"
	"github.com/funkygao/gafka/cmd/kateway/meta"
	"github.com/funkygao/gafka/mpool"
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

	appid, appSignature, userAgent string

	circuits   map[string]*breaker.Consecutive
	fetcher    *consumergroup.ConsumerGroup
	msgCh      chan *sarama.ConsumerMessage
	httpClient *http.Client // it has builtin pooling
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
		userAgent: fmt.Sprintf("actor.%s", gafka.BuildId),
		msgCh:     make(chan *sarama.ConsumerMessage, 20),
		circuits:  make(map[string]*breaker.Consecutive, len(endpoints)),
		httpClient: &http.Client{
			Timeout: time.Second * 4,
			Transport: &http.Transport{
				MaxIdleConnsPerHost: 20, // pooling
				Dial: (&net.Dialer{
					Timeout: time.Second * 4,
				}).Dial,
				DisableKeepAlives:     false, // enable http conn reuse
				ResponseHeaderTimeout: time.Second * 4,
				TLSHandshakeTimeout:   time.Second * 4,
			},
		},
	}

	for _, ep := range endpoints {
		this.circuits[ep] = &breaker.Consecutive{
			RetryTimeout:     time.Second * 5,
			FailureAllowance: 5,
		}
	}

	return this
}

func (this *WebhookExecutor) Run() {
	// TODO watch the znode change, its endpoint might change any time

	this.appid = manager.Default.TopicAppid(this.topic)
	if this.appid == "" {
		log.Warn("invalid topic: %s", this.topic)
		return
	}

	this.appSignature = manager.Default.Signature(this.appid)
	if this.appSignature == "" {
		log.Warn("%s/%s invalid app signature", this.topic, this.appid)
	}

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
	if err != nil {
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

func (this *WebhookExecutor) pushToEndpoint(msg *sarama.ConsumerMessage, uri string) (ok bool) {
	log.Debug("%s sending[%s] %s", this.topic, uri, string(msg.Value))

	if this.circuits[uri].Open() {
		log.Warn("%s %s circuit open", this.topic, uri)
		return false
	}

	body := mpool.BytesBufferGet()
	defer mpool.BytesBufferPut(body)

	body.Reset()
	body.Write(msg.Value)

	// TODO user defined post body schema, e,g. ElasticSearch
	req, err := http.NewRequest("POST", uri, body)
	if err != nil {
		this.circuits[uri].Fail()
		return false
	}

	req.Header.Set(gateway.HttpHeaderOffset, strconv.FormatInt(msg.Offset, 10))
	req.Header.Set(gateway.HttpHeaderPartition, strconv.FormatInt(int64(msg.Partition), 10))
	req.Header.Set("User-Agent", this.userAgent)
	req.Header.Set("X-App-Signature", this.appSignature)
	response, err := this.httpClient.Do(req)
	if err != nil {
		log.Error("%s %s %s", this.topic, uri, err)
		this.circuits[uri].Fail()
		return false
	}

	io.Copy(ioutil.Discard, response.Body)
	response.Body.Close()

	if response.StatusCode != http.StatusOK {
		log.Warn("%s %s response: %s", this.topic, uri, http.StatusText(response.StatusCode))
	}

	// audit
	log.Info("pushed %s/%d %d", this.topic, msg.Partition, msg.Offset)
	return true
}
