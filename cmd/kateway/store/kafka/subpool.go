package kafka

import (
	"sync"
	"time"

	"github.com/Shopify/sarama"
	"github.com/funkygao/gafka/cmd/kateway/meta"
	"github.com/funkygao/gafka/cmd/kateway/store"
	"github.com/funkygao/gafka/zk"
	log "github.com/funkygao/log4go"
	"github.com/wvanbergen/kafka/consumergroup"
)

type subPool struct {
	clientMap     map[string]*consumergroup.ConsumerGroup // key is client remote addr, a client can only sub 1 topic
	clientMapLock sync.RWMutex                            // TODO the lock is too big

	rebalancing bool // FIXME 1 topic rebalance should not affect other topics
}

func newSubPool() *subPool {
	return &subPool{
		clientMap: make(map[string]*consumergroup.ConsumerGroup, 500),
	}
}

func (this *subPool) PickConsumerGroup(cluster, topic, group,
	remoteAddr, reset string) (cg *consumergroup.ConsumerGroup, err error) {
	this.clientMapLock.Lock()
	defer this.clientMapLock.Unlock()

	for retries := 0; retries < 5; retries++ {
		if this.rebalancing {
			time.Sleep(time.Millisecond * 200)
		} else {
			break
		}
	}
	if this.rebalancing {
		err = store.ErrRebalancing
		return
	}

	var present bool
	cg, present = this.clientMap[remoteAddr]
	if present {
		return
	}

	// FIXME what if client wants to consumer a non-existent topic?
	onlineN := meta.Default.OnlineConsumersCount(cluster, topic, group)
	partitionN := len(meta.Default.TopicPartitions(cluster, topic))
	if partitionN > 0 && onlineN >= partitionN {
		log.Warn("cluster[%s] topic=%s group=%s online groups:%d >= partitions:%d, remote addr: %s",
			cluster, topic, group,
			onlineN, partitionN, remoteAddr)
		err = store.ErrTooManyConsumers
		return
	}

	// cache miss, create the consumer group for this client
	cf := consumergroup.NewConfig()
	cf.ChannelBufferSize = 0 // TODO
	cf.Consumer.Return.Errors = true

	cf.Zookeeper.Chroot = meta.Default.ZkChroot(cluster)
	cf.Zookeeper.Timeout = zk.DefaultZkSessionTimeout()

	switch reset {
	case "newest":
		cf.Offsets.ResetOffsets = true
		cf.Offsets.Initial = sarama.OffsetNewest
	case "oldest":
		cf.Offsets.ResetOffsets = true
		cf.Offsets.Initial = sarama.OffsetOldest
	default:
		cf.Offsets.Initial = sarama.OffsetOldest
	}
	cf.Offsets.CommitInterval = time.Minute
	// time to wait for all the offsets for a partition to be processed after stopping to consume from it.
	cf.Offsets.ProcessingTimeout = time.Second * 10

	for i := 0; i < 3; i++ {
		// join group will async register zk owners znodes
		// so, if many client concurrently connects to kateway, will not
		// strictly throw ErrTooManyConsumers
		cg, err = consumergroup.JoinConsumerGroup(group, []string{topic},
			meta.Default.ZkAddrs(), cf)
		if err == nil {
			this.clientMap[remoteAddr] = cg
			break
		}

		// backoff
		log.Warn("cluster:%s topic:%s join group:%s %v, retry after 100ms",
			cluster, topic, group, err)
		time.Sleep(time.Millisecond * 100)
	}

	return
}

// For a given consumer client, it might be killed twice:
// 1. on socket level, the socket is closed
// 2. websocket/sub handler, conn closed or error occurs, explicitly kill the client
func (this *subPool) killClient(remoteAddr string) {
	this.clientMapLock.Lock()
	defer this.clientMapLock.Unlock()

	this.rebalancing = true
	if cg, present := this.clientMap[remoteAddr]; present {
		cg.Close() // will flush offset, must wait, otherwise offset is not guanranteed
	} else {
		// client quit before getting the chance to consume
		// e,g. 1 partition, 2 clients, the 2nd will not get consume chance, then quit
		this.rebalancing = false

		return
	}

	delete(this.clientMap, remoteAddr)
	this.rebalancing = false

	log.Trace("consumer %s closed, rebalanced ok", remoteAddr)
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
