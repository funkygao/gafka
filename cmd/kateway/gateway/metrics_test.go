package gateway

import (
	"testing"
	"time"

	"github.com/funkygao/gafka/ctx"
)

func init() {
	ctx.LoadFromHome()
}

func BenchmarkMetricsCounterWithoutLock(b *testing.B) {
	s := NewServerMetrics(time.Hour, nil)
	for i := 0; i < b.N; i++ {
		s.TotalConns.Inc(1)
	}
}

func BenchmarkMetricsPubOkCounter(b *testing.B) {
	p := NewPubMetrics(nil)
	for i := 0; i < b.N; i++ {
		p.PubOk("appid", "topic", "ver")
	}
}

func BenchmarkMetricsQpsMeter(b *testing.B) {
	p := NewPubMetrics(nil)
	for i := 0; i < b.N; i++ {
		p.PubQps.Mark(1)
	}
}

func BenchmarkMetricsLatencyHistogram(b *testing.B) {
	p := NewPubMetrics(nil)
	for i := 0; i < b.N; i++ {
		p.PubLatency.Update(5)
	}
}
