package disk

import (
	"testing"
	"time"

	"github.com/funkygao/assert"
)

func TestQueueBasic(t *testing.T) {
	cfg := DefaultConfig()
	cfg.Dirs = []string{"hh"}
	var b block
	q := newQueue("hh", clusterTopic{cluster: "me", topic: "foobar"}, 1<<10, time.Second, time.Hour)
	err := q.Open()
	q.Start()
	assert.Equal(t, nil, err)
	for i := 0; i < 10; i++ {
		b.key = []byte("key")
		b.value = []byte("value")
		err = q.Append(&b)
		assert.Equal(t, nil, err)
	}
	err = q.Close()
	assert.Equal(t, nil, err)
}
