package telementry

import (
	"testing"

	"github.com/funkygao/assert"
)

func TestUntag(t *testing.T) {
	appid, topic, ver, realname := Untag("{app1.mytopic.v1}pub.ok")
	assert.Equal(t, "app1", appid)
	assert.Equal(t, "mytopic", topic)
	assert.Equal(t, "v1", ver)
	assert.Equal(t, "pub.ok", realname)

	appid, topic, ver, realname = Untag("pub.ok")
	assert.Equal(t, "pub.ok", realname)
	assert.Equal(t, "", appid)
	assert.Equal(t, "", topic)
	assert.Equal(t, "", ver)
}

func TestTag(t *testing.T) {
	assert.Equal(t, "{appid.topic.ver}", Tag("appid", "topic", "ver"))
}

// 186 ns/op
func BenchmarkUntag(b *testing.B) {
	for i := 0; i < b.N; i++ {
		Untag("{app1.mytopic.v1}pub.ok")
	}
}

// 159 ns/op	    2 allocs/op
func BenchmarkTag(b *testing.B) {
	for i := 0; i < b.N; i++ {
		Tag("appid", "topic", "ver")
	}
}
