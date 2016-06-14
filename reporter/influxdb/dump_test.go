package influxdb

import (
	"testing"
	"time"

	"github.com/funkygao/gafka/ctx"
	"github.com/rcrowley/go-metrics"
)

func createReporter() *reporter {
	ctx.LoadFromHome()
	cf, _ := NewConfig("uri", "db", "user", "pass", time.Second)
	return New(metrics.DefaultRegistry, cf)

}

func TestDump(t *testing.T) {
	g := metrics.NewRegisteredGaugeFloat64("gauge", nil)
	g.Update(1)

	r := createReporter()
	t.Logf("%+v", r.dump())
}

func TestTags(t *testing.T) {
	r := createReporter()
	r.extractTagsFromMetricsName("pub.qps")
}

func BenchmarkExtractTags(b *testing.B) {
	r := createReporter()
	for i := 0; i < b.N; i++ {
		_ = r.extractTagsFromMetricsName("aadfs.fdafd")
	}
}
