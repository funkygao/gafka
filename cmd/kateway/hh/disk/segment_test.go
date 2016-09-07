package disk

import (
	"os"
	"testing"

	"github.com/funkygao/assert"
)

func TestSegment(t *testing.T) {
	path := "/Users/funky/gopkg/src/github.com/funkygao/gafka/cmd/kateway/hh/disk/segment.001"
	defer os.Remove(path)

	s, err := newSegment(1, path, 2<<20)
	assert.Equal(t, nil, err)
	b := &block{
		key:   []byte("hello"),
		value: []byte("world"),
	}
	s.Append(b)
	s.flush()

	t.Logf("%s", s.wfile.Name())

	s.Seek(0)
	b1 := new(block)
	s.ReadOne(b1)
	assert.Equal(t, "hello", string(b1.key))
	assert.Equal(t, "world", string(b1.value))

}
