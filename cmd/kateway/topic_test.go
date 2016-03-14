package main

import (
	"testing"

	"github.com/funkygao/assert"
	"github.com/funkygao/gafka/cmd/kateway/meta"
	"github.com/funkygao/gafka/sla"
)

func TestGuardedTopicName(t *testing.T) {
	topic := "foo"
	rawTopic := meta.KafkaTopic("32", topic, "v1")
	assert.Equal(t, "32.foo.v1", rawTopic)

	gt := topic + "." + sla.SlaKeyRetryTopic
	rawTopic = meta.KafkaTopic("32", gt, "v1")
	assert.Equal(t, "32.foo.retry.v1", rawTopic)

	gt = topic + "." + sla.SlaKeyDeadLetterTopic
	rawTopic = meta.KafkaTopic("32", gt, "v1")
	assert.Equal(t, "32.foo.dead.v1", rawTopic)
}
