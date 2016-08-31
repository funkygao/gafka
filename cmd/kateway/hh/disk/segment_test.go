package disk

import (
	"os"
	"testing"

	"github.com/funkygao/assert"
)

func TestSegment(t *testing.T) {
	path := "/Users/funky/gopkg/src/github.com/funkygao/gafka/cmd/kateway/hh/disk/segment.001"
	s, err := newSegment(1, path, 2<<20)
	assert.Equal(t, nil, err)
	b := &block{
		key:   "hello",
		value: []byte("world"),
	}
	s.Append(b)
	s.Flush()

	t.Logf("%s", s.file.Name())

	s.Seek(0)
	b1 := new(block)
	s.ReadOne(b1)
	assert.Equal(t, "hello", b1.key)
	assert.Equal(t, "world", string(b1.value))
	os.Remove(path)
}
