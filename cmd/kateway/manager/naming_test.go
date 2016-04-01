package manager

import (
	"testing"

	"github.com/funkygao/assert"
)

func TestKafkaTopic(t *testing.T) {
	appid := "ap1"
	topic := "foobar"
	ver := "v1"
	if kafkaTopicWithSprintf(appid, topic, ver) != KafkaTopic(appid, topic, ver) {
		t.Fail()
	}
}

func TestShadowTopic(t *testing.T) {
	topic := "foobar"
	ver := "v1"
	assert.Equal(t, "hisapp.foobar.v1.myapp.group1.retry",
		ShadowTopic("retry", "myapp", "hisapp", topic, ver, "group1"))
}
