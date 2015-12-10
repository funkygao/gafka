package main

import (
	"testing"

	"github.com/funkygao/assert"
	"github.com/funkygao/gafka/cmd/kateway/store"
)

func TestIsBrokerError(t *testing.T) {
	assert.Equal(t, false, isBrokerError(store.ErrRebalancing))
	assert.Equal(t, false, isBrokerError(store.ErrTooManyConsumers))
}

func TestExtractFromMetricsName(t *testing.T) {
	appid, topic, ver, realname := extractFromMetricsName("{app1.mytopic.v1}pub.ok")
	assert.Equal(t, "app1", appid)
	assert.Equal(t, "mytopic", topic)
	assert.Equal(t, "v1", ver)
	assert.Equal(t, "pub.ok", realname)

	appid, topic, ver, realname = extractFromMetricsName("pub.ok")
	assert.Equal(t, "pub.ok", realname)
	assert.Equal(t, "", appid)
	assert.Equal(t, "", topic)
	assert.Equal(t, "", ver)
}

func BenchmarkExtractFromMetricsName(b *testing.B) {
	for i := 0; i < b.N; i++ {
		extractFromMetricsName("{app1.mytopic.v1}pub.ok")
	}
}
