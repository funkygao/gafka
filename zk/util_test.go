package zk

import (
	"testing"

	"github.com/funkygao/assert"
)

func TestTimestampToTime(t *testing.T) {
	tm := TimestampToTime("143761637400")
	t.Logf("%+v", tm)
	assert.Equal(t, 1974, tm.Year())
	assert.Equal(t, "July", tm.Month().String())
}
