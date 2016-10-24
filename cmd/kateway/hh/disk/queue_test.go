package disk

import (
	"fmt"
	"testing"
	"time"

	"github.com/funkygao/assert"
)

func TestQueueBasic(t *testing.T) {
	var b block
	q := newQueue("hh", clusterTopic{cluster: "me", topic: "foobar"}, 0, time.Second, time.Hour)
	err := q.Open()
	q.Start()
	assert.Equal(t, nil, err)
	for i := 0; i < 10; i++ {
		b.key = []byte(fmt.Sprintf("key%d", i))
		b.value = []byte(fmt.Sprintf("value%d", i))
		err = q.Append(&b)
		assert.Equal(t, nil, err)
	}
	err = q.Close()
	assert.Equal(t, nil, err)
}

func TestQueueCorrupt(t *testing.T) {
	var b block
	q := newQueue("hh", clusterTopic{cluster: "me", topic: "foobar"}, 0, time.Second, time.Hour)
	err := q.Open()
	assert.Equal(t, nil, err)
	go func() {
		for {
			err := q.Next(&b)
			switch err {
			case nil:
				t.Logf("got block: %s/%s", string(b.key), string(b.value))

			case ErrQueueNotOpen:
				t.Fatal("queu not open")

			case ErrCursorOutOfRange:
				t.Fatal("out of range")

			case ErrEOQ:
				t.Log("end of queue, sleeping...")
				time.Sleep(pollSleep)

			default:
				t.Fatal(err)
			}
		}
	}()

	for i := 0; i < 10; i++ {
		b.key = []byte(fmt.Sprintf("key%d", i))
		b.value = []byte(fmt.Sprintf("value%d", i))
		err = q.Append(&b)
		assert.Equal(t, nil, err)
	}

}
