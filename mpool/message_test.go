package mpool

import (
	"testing"

	"github.com/funkygao/assert"
)

func TestMessageWriteString(t *testing.T) {
	m := NewMessage(1029)
	m.WriteString("hello")
	m.WriteString("world")
	assert.Equal(t, "helloworld", string(m.Body))

	m.Reset()
	err := m.WriteString("yes")
	assert.Equal(t, "yes", string(m.Body))
	assert.Equal(t, nil, err)
}

func TestMessageBytes(t *testing.T) {
	m := NewMessage(1029)
	m.WriteString("hello")
	m.WriteString("world")
	assert.Equal(t, "helloworld", string(m.Body))
}

func TestMessageWrite(t *testing.T) {
	m := NewMessage(102)
	m.Write([]byte("hello"))
	m.Write([]byte(" "))
	m.Write([]byte("world"))
	assert.Equal(t, "hello world", string(m.Body))
}

func TestMessageMixeWriteAndWriteString(t *testing.T) {
	m := NewMessage(102)
	m.Write([]byte("hello"))
	m.WriteString(" world")
	assert.Equal(t, "hello world", string(m.Body))
}
