package kafka

import (
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/Shopify/sarama"
	"github.com/funkygao/anomalyzer"
	"github.com/funkygao/gafka/cmd/kguard/monitor"
	"github.com/funkygao/gafka/telemetry"
	"github.com/funkygao/gafka/zk"
	"github.com/funkygao/go-metrics"
	log "github.com/funkygao/log4go"
)

func init() {
	monitor.RegisterWatcher("kafka.topic", func() monitor.Watcher {
		return &WatchTopics{
			Tick: time.Minute,
		}
	})
}

// WatchTopics montor kafka total msg count over time.
type WatchTopics struct {
	Zkzone *zk.ZkZone
	Stop   <-chan struct{}
	Tick   time.Duration
	Wg     *sync.WaitGroup

	pubQps      map[string]metrics.Meter
	lastOffsets map[string]int64

	aggPubQpsAnomalyGauge metrics.Gauge
	aggPubQpsAnomaly      anomalyzer.Anomalyzer
}

func (this *WatchTopics) Init(ctx monitor.Context) {
	this.Zkzone = ctx.ZkZone()
	this.Stop = ctx.StopChan()
	this.Wg = ctx.Inflight()

	conf := &anomalyzer.AnomalyzerConf{
		Delay:       true,
		ActiveSize:  5,
		NSeasons:    5,
		Sensitivity: 0.1,    // magnitude
		UpperBound:  500000, // fence
		LowerBound:  0,      // fence
		PermCount:   1000,   // diff & rank
		Methods:     []string{"diff", "fence", "highrank", "lowrank", "magnitude", "ks", "cdf"},
	}
	var err error
	this.aggPubQpsAnomaly, err = anomalyzer.NewAnomalyzer(conf, nil)
	if err != nil {
		panic(err)
	}
	this.aggPubQpsAnomalyGauge = metrics.NewRegisteredGauge("pub.qps.anomaly", nil)
}

// set?key=kta-as:4
func (this *WatchTopics) Set(key string) {
	tuples := strings.SplitN(key, ":", 2)
	if len(tuples) != 2 {
		return
	}

	switch tuples[0] {
	case "kta-as":
		if n, err := strconv.Atoi(tuples[1]); err == nil && n > 0 {
			this.aggPubQpsAnomaly.Conf.ActiveSize = n
			log.Info("ActiveSize set to %d", n)
		}

	case "kta-ns":
		if n, err := strconv.Atoi(tuples[1]); err == nil && n > 0 {
			this.aggPubQpsAnomaly.Conf.NSeasons = n
			log.Info("NSeasons set to %d", n)
		}

	case "kta-sen":
		if f, err := strconv.ParseFloat(tuples[1], 64); err == nil && f > 0.01 {
			this.aggPubQpsAnomaly.Conf.Sensitivity = f
			log.Info("Sensitivity set to %f", f)
		}

	case "kta-upper":
		if f, err := strconv.ParseFloat(tuples[1], 64); err == nil && f > 0.01 {
			this.aggPubQpsAnomaly.Conf.UpperBound = f
			log.Info("UpperBound set to %f", f)
		}

	case "kta-lower":
		if f, err := strconv.ParseFloat(tuples[1], 64); err == nil && f > 0.01 {
			this.aggPubQpsAnomaly.Conf.LowerBound = f
			log.Info("LowerBound set to %f", f)
		}
	}
}

func (this *WatchTopics) Run() {
	defer this.Wg.Done()

	ticker := time.NewTicker(this.Tick)
	defer ticker.Stop()

	this.pubQps = make(map[string]metrics.Meter, 10)
	this.lastOffsets = make(map[string]int64, 10)

	offsets := metrics.NewRegisteredGauge("msg.cum", nil)
	topics := metrics.NewRegisteredGauge("topics", nil)
	partitions := metrics.NewRegisteredGauge("partitions", nil)
	brokers := metrics.NewRegisteredGauge("brokers", nil)
	newTopics := metrics.NewRegisteredGauge("kfk.newtopic.1d", nil)
	for {

		select {
		case <-this.Stop:
			log.Info("kafka.topic stopped")
			return

		case now := <-ticker.C:
			o, t, p, b := this.report()
			offsets.Update(o)
			topics.Update(t)
			partitions.Update(p)
			brokers.Update(b)
			newTopics.Update(this.newTopicsSince(now, time.Hour*24))
		}
	}

}

func (this *WatchTopics) newTopicsSince(now time.Time, since time.Duration) (n int64) {
	excludedClusters := this.Zkzone.PublicClusters()
	this.Zkzone.ForSortedClusters(func(zkcluster *zk.ZkCluster) {
		// kateway topics excluded
		clusterExcluded := false
		for _, cluster := range excludedClusters {
			if cluster.Name() == zkcluster.Name() {
				clusterExcluded = true
				break
			}
		}
		if clusterExcluded {
			return
		}

		// find recently how many topics created
		for _, ctime := range zkcluster.TopicsCtime() {
			if now.Sub(ctime) <= since {
				n += 1
			}
		}
	})

	return
}

func (this *WatchTopics) report() (totalOffsets int64, topicsN int64,
	partitionN int64, brokersN int64) {
	var totalPubQpsRate1 float64
	this.Zkzone.ForSortedClusters(func(zkcluster *zk.ZkCluster) {
		brokerList := zkcluster.BrokerList()
		kfk, err := sarama.NewClient(brokerList, sarama.NewConfig())
		if err != nil {
			log.Error("cluster[%s] %v", zkcluster.Name(), err)
			return
		}
		defer kfk.Close()

		brokersN += int64(len(brokerList))

		topics, err := kfk.Topics()
		if err != nil {
			log.Error("cluster[%s] %v", zkcluster.Name(), err)
			return
		}

		for _, topic := range topics {
			partions, err := kfk.Partitions(topic)
			if err != nil {
				log.Error("cluster[%s] topic:%s %v", zkcluster.Name(), topic, err)
				continue
			}

			topicsN++

			offsetOfTopic := int64(0)
			for _, partitionId := range partions {
				latestOffset, err := kfk.GetOffset(topic, partitionId,
					sarama.OffsetNewest)
				if err != nil {
					log.Error("cluster[%s] topic[%s/%d]: %v",
						zkcluster.Name(), topic, partitionId, err)
					continue
				}

				partitionN++
				totalOffsets += latestOffset
				offsetOfTopic += latestOffset
			}

			// update pubQps metrics
			tag := telemetry.Tag(zkcluster.Name(), strings.Replace(topic, ".", "_", -1), "v1")
			if _, present := this.pubQps[tag]; !present {
				this.pubQps[tag] = metrics.NewRegisteredMeter(tag+"pub.qps", nil)
			}
			lastOffset := this.lastOffsets[tag]
			if lastOffset == 0 {
				// first run
				this.lastOffsets[tag] = offsetOfTopic
			} else {
				delta := offsetOfTopic - lastOffset
				if delta >= 0 {
					this.pubQps[tag].Mark(delta)
					this.lastOffsets[tag] = offsetOfTopic
				} else {
					log.Warn("cluster[%s] topic[%s] offset backwards, skipped: %d <- %d",
						zkcluster.Name(), topic, offsetOfTopic, lastOffset)
				}

			}

			totalPubQpsRate1 += this.pubQps[tag].Rate1()
		}

	})

	this.aggPubQpsAnomaly.Update([]float64{totalPubQpsRate1}) // avoid overflow
	this.aggPubQpsAnomalyGauge.Update(int64(100 * this.aggPubQpsAnomaly.Eval()))

	return
}
