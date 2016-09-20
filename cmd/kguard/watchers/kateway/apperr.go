package kateway

import (
	"bytes"
	"fmt"
	"os"
	"sync"

	"github.com/Shopify/sarama"
	"github.com/funkygao/gafka/cmd/kguard/monitor"
	"github.com/funkygao/gafka/zk"
	"github.com/funkygao/go-metrics"
	log "github.com/funkygao/log4go"
)

var (
	javaSdkPubErr       = []byte("send msg error")
	javaSdkPubStatusErr = []byte("StatusLine is null")
)

func init() {
	monitor.RegisterWatcher("kateway.apperr", func() monitor.Watcher {
		return &WatchAppError{}
	})
}

// WatchAppError monitors app err log to find all pubsub related err.
type WatchAppError struct {
	Zkzone *zk.ZkZone
	Stop   <-chan struct{}
	Wg     *sync.WaitGroup
}

func (this *WatchAppError) Init(ctx monitor.Context) {
	this.Zkzone = ctx.ZkZone()
	this.Stop = ctx.StopChan()
	this.Wg = ctx.Inflight()
}

func (this *WatchAppError) Run() {
	defer this.Wg.Done()

	appError := metrics.NewRegisteredCounter("kateway.apperr", nil)
	msgChan := make(chan *sarama.ConsumerMessage, 2000)

	if err := this.consumeAppErrLogs(msgChan); err != nil {
		close(msgChan)

		log.Error("%v", err)
		return
	}

	for {
		select {
		case <-this.Stop:
			log.Info("kateway.apperr stopped")
			return

		case msg, ok := <-msgChan:
			if !ok {
				return
			}

			appError.Inc(1)
			log.Warn("%d/%d %s", msg.Partition, msg.Offset, string(msg.Value))
		}
	}
}

func (this *WatchAppError) consumeAppErrLogs(msgChan chan<- *sarama.ConsumerMessage) error {
	var (
		cluster = os.Getenv("APPLOG_CLUSTER")
		topic   = os.Getenv("APPLOG_TOPIC")
	)

	if cluster == "" || topic == "" {
		return fmt.Errorf("empty cluster/topic params provided, kateway.apperr disabled")
	}

	zkcluster := this.Zkzone.NewCluster(cluster)
	brokerList := zkcluster.BrokerList()
	if len(brokerList) == 0 {
		return fmt.Errorf("cluster[%s] has empty brokers", cluster)
	}
	kfk, err := sarama.NewClient(brokerList, sarama.NewConfig())
	if err != nil {
		return err
	}
	defer kfk.Close()

	consumer, err := sarama.NewConsumerFromClient(kfk)
	if err != nil {
		return err
	}
	defer consumer.Close()

	partitions, err := kfk.Partitions(topic)
	if err != nil {
		return err
	}

	var wg sync.WaitGroup
	for _, p := range partitions {
		wg.Add(1)
		go this.consumePartition(zkcluster, consumer, topic, p, sarama.OffsetNewest, msgChan, &wg)
	}

	wg.Wait()
	return nil
}

func (this *WatchAppError) consumePartition(zkcluster *zk.ZkCluster, consumer sarama.Consumer,
	topic string, partitionId int32, offset int64, msgCh chan<- *sarama.ConsumerMessage, wg *sync.WaitGroup) {
	defer wg.Done()

	p, err := consumer.ConsumePartition(topic, partitionId, offset)
	if err != nil {
		log.Error("%s %s/%d: offset=%d %v", zkcluster.Name(), topic, partitionId, offset, err)
		return
	}
	defer p.Close()

	for {
		select {
		case <-this.Stop:
			return

		case err := <-p.Errors():
			log.Critical("cluster[%s] %s/%d: %s", zkcluster.Name(), topic, partitionId, err)

		case msg := <-p.Messages():
			if msg != nil && this.predicate(msg.Value) {
				msgCh <- msg
			}

		}
	}
}

func (this *WatchAppError) predicate(msg []byte) bool {
	switch {
	case bytes.Contains(msg, javaSdkPubErr):
		return true

	case bytes.Contains(msg, javaSdkPubStatusErr):
		return true

	default:
		return false
	}
}
