package telementry

import (
	"testing"

	"github.com/funkygao/assert"
)

func TestDecodeMetricName(t *testing.T) {
	appid, topic, ver, realname := DecodeMetricName("{app1.mytopic.v1}pub.ok")
	assert.Equal(t, "app1", appid)
	assert.Equal(t, "mytopic", topic)
	assert.Equal(t, "v1", ver)
	assert.Equal(t, "pub.ok", realname)

	appid, topic, ver, realname = DecodeMetricName("pub.ok")
	assert.Equal(t, "pub.ok", realname)
	assert.Equal(t, "", appid)
	assert.Equal(t, "", topic)
	assert.Equal(t, "", ver)
}

// 186 ns/op
func BenchmarkDecodeMetricName(b *testing.B) {
	for i := 0; i < b.N; i++ {
		DecodeMetricName("{app1.mytopic.v1}pub.ok")
	}
}
