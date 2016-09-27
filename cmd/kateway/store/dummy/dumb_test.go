package dummy

import (
	"testing"

	"github.com/funkygao/assert"
)

func TestPubName(t *testing.T) {
	p := NewPubStore(false)
	assert.Equal(t, "dummy", p.Name())
}

func TestSubName(t *testing.T) {
	s := NewSubStore(nil, false)
	assert.Equal(t, "dummy", s.Name())
}
