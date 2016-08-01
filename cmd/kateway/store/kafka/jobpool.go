package kafka

import (
	"sync"
	"time"

	"github.com/EverythingMe/go-disque/disque"
	log "github.com/funkygao/log4go"
	"github.com/garyburd/redigo/redis"
)

// jobPool will use Disque as backend storage.
type jobPool struct {
	pool *disque.Pool

	cluster   string
	queues    map[string]struct{}
	lk        sync.RWMutex
	newQueues chan string
}

func newJobPool(cluster string, addrs []string) *jobPool {
	jp := &jobPool{
		cluster:   cluster,
		queues:    make(map[string]struct{}, 10),
		newQueues: make(chan string, 10),
	}
	jp.pool = disque.NewPool(disque.DialFunc(jp.dial), addrs...)
	go jp.runWorker()
	return jp
}

func (*jobPool) dial(addr string) (redis.Conn, error) {
	log.Debug("job dialing: %s", addr)
	return redis.Dial("tcp", addr)
}

func (this *jobPool) RefreshNodes() error {
	return this.pool.RefreshNodes()
}

func (this *jobPool) AddJob(topic string, payload []byte, delay time.Duration) (jobId string, err error) {
	var c disque.Client
	c, err = this.pool.Get()
	if err != nil {
		return "", err
	}

	foundNewTopic := false
	this.lk.RLock()
	if _, present := this.queues[topic]; !present {
		foundNewTopic = true
	}
	this.lk.RUnlock()

	if foundNewTopic {
		this.lk.Lock()
		if _, present := this.queues[topic]; !present {
			this.queues[topic] = struct{}{}

			this.newQueues <- topic
		}
		this.lk.Unlock()
	}

	jobId, err = c.Add(disque.AddRequest{
		Job: disque.Job{
			Queue: topic,
			Data:  payload,
		},
		Delay:   delay,
		TTL:     delay + time.Hour, // 1h should be enough? TODO
		Timeout: time.Millisecond * 100,
		Retry:   1,
	})

	c.Close() // recycle to pool
	return
}

func (this *jobPool) DeleteJob(jobId string) error {
	c, err := this.pool.Get()
	if err != nil {
		return err
	}

	err = c.FastAck(jobId)

	c.Close()
	return err
}
