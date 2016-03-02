package mpool

import (
	"testing"

	"github.com/funkygao/assert"
)

func TestMessageWriteString(t *testing.T) {
	m := NewMessage(1029)
	m.WriteString("hello")
	m.WriteString("world")
	assert.Equal(t, "helloworld", string(m.Bytes()))

	m.Reset()
	err := m.WriteString("yes")
	assert.Equal(t, "yes", string(m.Bytes()))
	assert.Equal(t, nil, err)
}

func TestMessageBytes(t *testing.T) {
	m := NewMessage(1029)
	m.WriteString("hello")
	m.WriteString("world")
	assert.Equal(t, "helloworld", string(m.Bytes()))
}
