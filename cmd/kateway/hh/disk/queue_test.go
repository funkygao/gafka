package disk

import (
	"encoding/binary"
	"fmt"
	"io/ioutil"
	"os"
	"testing"
	"time"

	"github.com/funkygao/assert"
)

func TestQueueBasic(t *testing.T) {
	os.RemoveAll("hh")

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
	os.RemoveAll("hh")
}

func TestQueueCorrupt(t *testing.T) {
	var b block
	q := newQueue("hh", clusterTopic{cluster: "me", topic: "foobar"}, 0, time.Second, time.Hour)
	err := q.Open()
	assert.Equal(t, nil, err)

	buf := make([]byte, 10)
	binary.BigEndian.PutUint32(buf[:], maxBlockSize+5)
	corruptData := []byte{0, 0} // magic
	corruptData = append(corruptData, buf...)
	corruptData = append(corruptData, []byte("key1")...)

	for i := 0; i < 10; i++ {
		b.key = []byte(fmt.Sprintf("key%d", i))
		b.value = []byte(fmt.Sprintf("value%d", i))
		err = q.Append(&b)
		assert.Equal(t, nil, err)
		t.Logf("written block: %s/%s", string(b.key), string(b.value))
	}
	q.Close()

	// write corruption
	ioutil.WriteFile("hh/me/foobar/00000000000000000002", corruptData, 0644)

	err = q.Open()
	assert.Equal(t, nil, err)
	defer q.Close()

	//time.Sleep(time.Second)
	for {
		err := q.Next(&b)
		q.Append(&b)
		switch err {
		case nil:
			q.cursor.commitPosition()
			t.Logf("    got block: %s/%s", string(b.key), string(b.value))

		case ErrEOQ:
			//t.Log("end of queue")
			return

		case ErrSegmentCorrupt:
			t.Error("shit")
			return

		default:
			t.Fatal(err)
		}
	}
}
