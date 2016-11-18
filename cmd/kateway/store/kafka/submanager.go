package kafka

import (
	"sync"
	"time"

	"github.com/Shopify/sarama"
	"github.com/funkygao/gafka/cmd/kateway/meta"
	"github.com/funkygao/gafka/cmd/kateway/store"
	"github.com/funkygao/gafka/zk"
	"github.com/funkygao/kafka-cg/consumergroup"
	log "github.com/funkygao/log4go"
)

type subManager struct {
	clientMap     map[string]*consumergroup.ConsumerGroup // key is client remote addr, a client can only sub 1 topic
	clientMapLock sync.RWMutex                            // TODO the lock is too big
}

func newSubManager() *subManager {
	return &subManager{
		clientMap: make(map[string]*consumergroup.ConsumerGroup, 500),
	}
}

func (this *subManager) PickConsumerGroup(cluster, topic, group, remoteAddr, realIp string,
	resetOffset string, permitStandby bool) (cg *consumergroup.ConsumerGroup, err error) {
	// find consumger group from cache
	var present bool
	this.clientMapLock.RLock()
	cg, present = this.clientMap[remoteAddr]
	this.clientMapLock.RUnlock()
	if present {
		return
	}

	if !permitStandby {
		// ensure concurrent sub threads didn't exceed partition count
		// the 1st non-strict barrier, consumer group is the final barrier
		partitionN := len(meta.Default.TopicPartitions(cluster, topic))
		if partitionN == 0 {
			err = store.ErrInvalidTopic
			return
		}
		onlineN, e := meta.Default.OnlineConsumersCount(cluster, topic, group)
		if e != nil {
			return nil, e
		}
		if onlineN >= partitionN {
			err = store.ErrTooManyConsumers
			return
		}
	}

	this.clientMapLock.Lock()
	defer this.clientMapLock.Unlock()

	// double check lock
	cg, present = this.clientMap[remoteAddr]
	if present {
		return
	}

	// cache miss, create the consumer group for this client
	cf := consumergroup.NewConfig()
	cf.PermitStandby = permitStandby
	cf.OneToOne = true

	cf.Net.DialTimeout = time.Second * 10
	cf.Net.WriteTimeout = time.Second * 10
	cf.Net.ReadTimeout = time.Second * 10

	// kafka Fetch already batched into MessageSet，
	// this chan buf size influence on throughput is ignoreable
	cf.ChannelBufferSize = 0
	// kafka Fetch MaxWaitTime 250ms, MinByte=1 by default

	cf.Consumer.Return.Errors = true
	cf.Consumer.MaxProcessingTime = time.Second * 2 // chan recv timeout
	cf.Zookeeper.Chroot = meta.Default.ZkChroot(cluster)
	cf.Zookeeper.Timeout = zk.DefaultZkSessionTimeout()
	cf.Offsets.CommitInterval = time.Minute
	cf.Offsets.ProcessingTimeout = time.Second
	switch resetOffset {
	case "newest":
		cf.Offsets.ResetOffsets = true
		cf.Offsets.Initial = sarama.OffsetNewest
	case "oldest":
		cf.Offsets.ResetOffsets = true
		cf.Offsets.Initial = sarama.OffsetOldest
	default:
		cf.Offsets.ResetOffsets = false
		cf.Offsets.Initial = sarama.OffsetOldest
	}

	// runs in serial
	cg, err = consumergroup.JoinConsumerGroupRealIp(realIp, group, []string{topic},
		meta.Default.ZkAddrs(), cf)
	if err == nil {
		this.clientMap[remoteAddr] = cg
	}

	return
}

// For a given consumer client, it might be killed twice:
// 1. on socket level, the socket is closed
// 2. websocket/sub handler, conn closed or error occurs, explicitly kill the client
func (this *subManager) killClient(remoteAddr string) (err error) {
	this.clientMapLock.Lock()
	cg, present := this.clientMap[remoteAddr]
	if present {
		delete(this.clientMap, remoteAddr)
	}
	this.clientMapLock.Unlock()

	if !present {
		// e,g. client connects to Sub port but not Sub request(404), will lead to this case
		// e,g. Sub a non-exist topic
		return
	}

	if err = cg.Close(); err != nil {
		// will flush offset, must wait, otherwise offset is not guanranteed
		log.Error("cg[%s] close %s: %v", cg.Name(), remoteAddr, err)
	}

	return
}

func (this *subManager) Stop() {
	this.clientMapLock.Lock()
	defer this.clientMapLock.Unlock()

	var wg sync.WaitGroup
	for _, cg := range this.clientMap {
		wg.Add(1)
		go func(cg *consumergroup.ConsumerGroup) {
			cg.Close() // will commit inflight offsets
			wg.Done()
		}(cg)
	}

	wg.Wait()
	log.Trace("all consumer offsets committed")
}
