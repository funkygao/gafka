package mirror

import (
	"testing"

	"github.com/funkygao/assert"
)

func TestRealTopicsDumb(t *testing.T) {
	cf := DefaultConfig()
	topics := []string{"t1", "t2", "__consumer_offsets"}
	assert.Equal(t, []string{"t1", "t2"}, cf.realTopics(topics))

	topics = []string{}
	assert.Equal(t, topics, cf.realTopics(topics))
}

func TestRealTopicsWithExclusion(t *testing.T) {
	cf := DefaultConfig()
	cf.ExcludedTopics = map[string]struct{}{
		"t1": {},
	}
	topics := []string{"t1", "t2", "__consumer_offsets"}
	assert.Equal(t, []string{"t2"}, cf.realTopics(topics))

	topics = []string{}
	assert.Equal(t, topics, cf.realTopics(topics))
}

func TestRealTopicsOnly(t *testing.T) {
	cf := DefaultConfig()
	cf.TopicsOnly = map[string]struct{}{
		"t1": {},
	}
	topics := []string{"t1", "t2", "__consumer_offsets"}
	assert.Equal(t, []string{"t1"}, cf.realTopics(topics))

	topics = []string{}
	assert.Equal(t, topics, cf.realTopics(topics))
}
