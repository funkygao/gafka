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

type subPool struct {
	clientMap     map[string]*consumergroup.ConsumerGroup // key is client remote addr, a client can only sub 1 topic
	clientMapLock sync.RWMutex                            // TODO the lock is too big
}

func newSubPool() *subPool {
	return &subPool{
		clientMap: make(map[string]*consumergroup.ConsumerGroup, 500),
	}
}

func (this *subPool) PickConsumerGroup(cluster, topic, group, remoteAddr string,
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
		onlineN := meta.Default.OnlineConsumersCount(cluster, topic, group)
		partitionN := len(meta.Default.TopicPartitions(cluster, topic))
		if partitionN > 0 && onlineN >= partitionN {
			err = store.ErrTooManyConsumers
			return
		}
	}

	// cache miss, create the consumer group for this client
	cf := consumergroup.NewConfig()
	cf.Net.DialTimeout = time.Second * 10
	cf.Net.WriteTimeout = time.Second * 10
	cf.Net.ReadTimeout = time.Second * 10

	// kafka Fetch already batched into MessageSet，
	// this chan buf size influence on throughput is ignoreable
	cf.ChannelBufferSize = 0
	// kafka Fetch MaxWaitTime 250ms, MinByte=1 by default

	cf.Consumer.Return.Errors = true
	cf.Consumer.MaxProcessingTime = 100 * time.Millisecond // chan recv timeout
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

	// double check lock
	this.clientMapLock.Lock()
	defer this.clientMapLock.Unlock()
	cg, present = this.clientMap[remoteAddr]
	if present {
		return
	}

	// runs in serial
	cg, err = consumergroup.JoinConsumerGroup(group, []string{topic},
		meta.Default.ZkAddrs(), cf)
	if err == nil {
		this.clientMap[remoteAddr] = cg
	}

	return
}

// For a given consumer client, it might be killed twice:
// 1. on socket level, the socket is closed
// 2. websocket/sub handler, conn closed or error occurs, explicitly kill the client
func (this *subPool) killClient(remoteAddr string) (err error) {
	this.clientMapLock.Lock()
	defer this.clientMapLock.Unlock()

	if cg, present := this.clientMap[remoteAddr]; present {
		err = cg.Close() // will flush offset, must wait, otherwise offset is not guanranteed
		if err != nil {
			log.Error("cg[%s] close %s: %v", cg.Name(), remoteAddr, err)
		}

		delete(this.clientMap, remoteAddr)
	}

	return
}

func (this *subPool) Stop() {
	this.clientMapLock.Lock()
	defer this.clientMapLock.Unlock()

	var wg sync.WaitGroup
	for _, cg := range this.clientMap {
		wg.Add(1)
		go func() {
			cg.Close() // will commit inflight offsets
			wg.Done()
		}()
	}

	wg.Wait()
	log.Trace("all consumer offsets committed")
}
