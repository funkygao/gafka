package influxdb

import (
	"encoding/json"
	"fmt"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/Shopify/sarama"
	"github.com/funkygao/gafka/cmd/kguard/monitor"
	"github.com/funkygao/gafka/zk"
	"github.com/funkygao/go-metrics"
	log "github.com/funkygao/log4go"
	"github.com/satyrius/gonx"
)

var (
	ErrLogEntrySkipped = fmt.Errorf("skipped")
)

func init() {
	monitor.RegisterWatcher("influxdb.server", func() monitor.Watcher {
		return &WatchInfluxServer{
			Tick: time.Minute,
		}
	})
}

type WatchInfluxServer struct {
	Zkzone *zk.ZkZone
	Stop   <-chan struct{}
	Tick   time.Duration
	Wg     *sync.WaitGroup

	parser *gonx.Parser
}

func (this *WatchInfluxServer) Init(ctx monitor.Context) {
	this.Zkzone = ctx.ZkZone()
	this.Stop = ctx.StopChan()
	this.Wg = ctx.Inflight()
}

func (this *WatchInfluxServer) Run() {
	defer this.Wg.Done()

	postLatency := metrics.NewRegisteredHistogram("influxdb.latency.post", nil, metrics.NewExpDecaySample(1028, 0.015))
	getLatency := metrics.NewRegisteredHistogram("influxdb.latency.get", nil, metrics.NewExpDecaySample(1028, 0.015))
	postSize := metrics.NewRegisteredHistogram("influxdb.size.post", nil, metrics.NewExpDecaySample(1028, 0.015))
	getSize := metrics.NewRegisteredHistogram("influxdb.size.get", nil, metrics.NewExpDecaySample(1028, 0.015))

	msgChan := make(chan *sarama.ConsumerMessage, 128)
	if err := this.consumeInfluxdbAccessLog(msgChan); err != nil {
		close(msgChan)

		log.Error("influxdb.server: %v", err)
		return
	}

	httpdFormat := "[$prefix] $remote_addr $remote_log_name $remote_user [$start_time] \"$request\" $status $resp_bytes \"$referer\" \"$user_agent\" $req_id $time_elapsed"
	this.parser = gonx.NewParser(httpdFormat)

	for {
		select {
		case <-this.Stop:
			log.Info("influxdb.server stopped")
			return

		case msg, ok := <-msgChan:
			if !ok {
				log.Info("influxdb.server EOF from access log stream")
				return
			}

			pl, gl, ps, gs, m, err := this.parseMessage(msg.Value)
			if err != nil {
				if err != ErrLogEntrySkipped {
					log.Error("influxdb.server: %v", err)
				}

				continue
			}

			if m == "POST" {
				postLatency.Update(pl)
				postSize.Update(ps)
			} else if m == "GET" {
				getLatency.Update(gl)
				getSize.Update(gs)
			}
		}
	}

}

func (this *WatchInfluxServer) parseMessage(message []byte) (postLatency, getLatency, postSize, getSize int64, method string, err error) {
	msg := make(map[string]string)
	if err = json.Unmarshal(message, msg); err != nil {
		return
	}

	line := msg["message"] // TODO parse msg["host"]

	if !strings.HasPrefix(line, "[httpd]") {
		return 0, 0, 0, 0, "", ErrLogEntrySkipped
	}

	entry, err := this.parser.ParseString(line)
	if err != nil {
		return 0, 0, 0, 0, "", err
	}

	req, err := entry.Field("request")
	if err != nil {
		return 0, 0, 0, 0, "", err
	}

	method, uri := this.getMethod(req)
	if err = this.isTargetReq(method, uri); err != nil {
		return 0, 0, 0, 0, "", err
	}

	// in byte
	respSizeStr, err := entry.Field("resp_bytes")
	if err != nil {
		return 0, 0, 0, 0, "", err
	}
	respSize, err := strconv.ParseInt(respSizeStr, 10, 64)
	if err != nil {
		return 0, 0, 0, 0, "", err
	}

	// in us
	latencyStr, err := entry.Field("time_elapsed")
	if err != nil {
		return 0, 0, 0, 0, "", err
	}
	latency, err := strconv.ParseInt(latencyStr, 10, 64)
	if err != nil {
		return 0, 0, 0, 0, "", err
	}

	if method == "POST" {
		postLatency = latency
		postSize = respSize
	} else if method == "GET" {
		getLatency = latency
		getSize = respSize
	}

	return
}

func (this *WatchInfluxServer) getMethod(req string) (string, string) {
	items := strings.Split(req, " ")
	return strings.ToUpper(items[0]), items[1]
}

func (this *WatchInfluxServer) isTargetReq(method, uri string) (err error) {
	if method == "POST" {
		if strings.HasPrefix(uri, "/write") {
			return nil
		} else {
			return ErrLogEntrySkipped
		}
	} else if method == "GET" {
		if strings.HasPrefix(uri, "/query") {
			return nil
		} else {
			return ErrLogEntrySkipped
		}
	}

	return ErrLogEntrySkipped
}

func (this *WatchInfluxServer) consumeInfluxdbAccessLog(msgChan chan<- *sarama.ConsumerMessage) error {
	var (
		cluster = os.Getenv("INFLUXLOG_CLUSTER")
		topic   = os.Getenv("INFLUXLOG_TOPIC")
	)

	if cluster == "" || topic == "" {
		return fmt.Errorf("empty cluster/topic params provided, influxdb.server disabled")
	}

	zkcluster := this.Zkzone.NewCluster(cluster)
	brokerList := zkcluster.BrokerList()
	if len(brokerList) == 0 {
		return fmt.Errorf("influxdb.server cluster[%s] has empty brokers", cluster)
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

func (this *WatchInfluxServer) consumePartition(zkcluster *zk.ZkCluster, consumer sarama.Consumer,
	topic string, partitionId int32, offset int64, msgCh chan<- *sarama.ConsumerMessage, wg *sync.WaitGroup) {
	defer wg.Done()

	p, err := consumer.ConsumePartition(topic, partitionId, offset)
	if err != nil {
		log.Error("influxdb.server %s %s/%d: offset=%d %v", zkcluster.Name(), topic, partitionId, offset, err)
		return
	}
	defer p.Close()

	for {
		select {
		case <-this.Stop:
			return

		case err := <-p.Errors():
			log.Critical("influxdb.server cluster[%s] %s/%d: %s", zkcluster.Name(), topic, partitionId, err)
			// TODO resume when conn broken
			return

		case msg := <-p.Messages():
			if msg != nil {
				msgCh <- msg
			}

		}
	}
}
