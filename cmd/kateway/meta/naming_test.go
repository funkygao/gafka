package meta

import (
	"testing"
)

func TestKafkaTopic(t *testing.T) {
	appid := "ap1"
	topic := "foobar"
	ver := "v1"
	if kafkaTopicWithSprintf(appid, topic, ver) != KafkaTopic(appid, topic, ver) {
		t.Fail()
	}
}
