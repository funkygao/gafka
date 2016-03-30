package kafka

import (
	"time"

	"github.com/EverythingMe/go-disque/disque"
	"github.com/garyburd/redigo/redis"
)

// jobPool will use Disque as backend storage.
type jobPool struct {
	pool *disque.Pool
}

func newJobPool(addrs []string) *jobPool {
	jp := &jobPool{}
	jp.pool = disque.NewPool(disque.DialFunc(jp.dial), addrs...)
	return jp
}

func (*jobPool) dial(addr string) (redis.Conn, error) {
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

	jobId, err = c.Add(disque.AddRequest{
		Job: disque.Job{
			Queue: topic,
			Data:  payload,
		},
		Delay:   delay,
		Timeout: time.Millisecond * 100,
	})

	c.Close() // recycle to pool
	return
}
