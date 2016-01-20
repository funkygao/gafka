package dummy

import (
	"testing"

	"github.com/funkygao/assert"
)

func TestPubName(t *testing.T) {
	p := NewPubStore(nil, false)
	assert.Equal(t, "dummy", p.Name())
}

func TestSubName(t *testing.T) {
	s := NewSubStore(nil, nil, false)
	assert.Equal(t, "dummy", s.Name())
}
