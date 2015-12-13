package mpool

import (
	"testing"

	"github.com/funkygao/assert"
)

func TestMessageWriteString(t *testing.T) {
	m := NewMessage(1029)
	m.WriteString("hello")
	m.WriteString("world")
	assert.Equal(t, "helloworld", m.String())

	m.Reset()
	err := m.WriteString("yes")
	assert.Equal(t, "yes", m.String())
	assert.Equal(t, nil, err)
}

func TestMessageBytes(t *testing.T) {
	m := NewMessage(1029)
	m.WriteString("hello")
	m.WriteString("world")
	assert.Equal(t, "helloworld", string(m.Bytes()))
}
