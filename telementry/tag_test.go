package telementry

import (
	"testing"

	"github.com/funkygao/assert"
)

func TestExtractFromMetricsName(t *testing.T) {
	appid, topic, ver, realname := ExtractFromMetricsName("{app1.mytopic.v1}pub.ok")
	assert.Equal(t, "app1", appid)
	assert.Equal(t, "mytopic", topic)
	assert.Equal(t, "v1", ver)
	assert.Equal(t, "pub.ok", realname)

	appid, topic, ver, realname = ExtractFromMetricsName("pub.ok")
	assert.Equal(t, "pub.ok", realname)
	assert.Equal(t, "", appid)
	assert.Equal(t, "", topic)
	assert.Equal(t, "", ver)
}

// 186 ns/op
func BenchmarkExtractFromMetricsName(b *testing.B) {
	for i := 0; i < b.N; i++ {
		ExtractFromMetricsName("{app1.mytopic.v1}pub.ok")
	}
}
