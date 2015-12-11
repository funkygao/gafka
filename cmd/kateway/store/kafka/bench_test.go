package kafka

import (
	"testing"
	"time"

	"github.com/funkygao/gafka/ctx"
	"github.com/funkygao/golib/pool"
	log "github.com/funkygao/log4go"
)

func init() {
	ctx.LoadConfig("/etc/kateway.cf")
	log.AddFilter("stdout", log.INFO, log.NewConsoleLogWriter())
}

func BenchmarkPubPool(b *testing.B) {
	s := NewPubStore(nil, false)
	p := newPubPool(s, []string{"localhost:9092"}, 100)
	for i := 0; i < b.N; i++ {
		c, err := p.Get()
		if err != nil {
			b.Fatal(err)
		}

		c.Recycle()
	}
	s.Stop()
}

type TestResource struct {
	num    int64
	closed bool
}

func (tr *TestResource) Close() {
}

func (this *TestResource) Id() uint64 {
	return 1
}

func PoolFactory() (pool.Resource, error) {
	return &TestResource{1, false}, nil
}

func BenchmarkDumbPool(b *testing.B) {
	p := pool.NewResourcePool("dumbpool", PoolFactory, 100,
		100, 0, time.Hour, 0)
	for i := 0; i < b.N; i++ {
		r, e := p.Get()
		if e != nil {
			b.Fatal(e)
		}

		p.Put(r)
	}

}
