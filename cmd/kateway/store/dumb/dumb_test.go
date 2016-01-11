package dumb

import (
	"testing"

	"github.com/funkygao/assert"
)

func TestPubName(t *testing.T) {
	p := NewPubStore(nil, false)
	assert.Equal(t, "dumb", p.Name())
}

func TestSubName(t *testing.T) {
	s := NewSubStore(nil, nil, false)
	assert.Equal(t, "dumb", s.Name())
}
