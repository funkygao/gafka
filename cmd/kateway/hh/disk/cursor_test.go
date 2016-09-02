package disk

import (
	"os"
	"testing"
	"time"

	"github.com/funkygao/assert"
)

func TestCursor(t *testing.T) {
	ct := clusterTopic{cluster: "cluster", topic: "topic"}
	q := newQueue(ct, "dir", -1, time.Hour, time.Hour)
	defer os.RemoveAll("dir")

	err := q.Open() // will open cursor internally
	assert.Equal(t, nil, err)
	defer q.Close()

	q.cursor.pos.Offset = 90
	q.cursor.pos.SegmentID = 5
	err = q.cursor.dump()
	assert.Equal(t, nil, err)

	q.cursor.pos = position{} // reset
	err = q.cursor.open()
	assert.Equal(t, nil, err)
	assert.Equal(t, int64(90), q.cursor.pos.Offset)
	assert.Equal(t, uint64(5), q.cursor.pos.SegmentID)
}
