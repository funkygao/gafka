package mpool

import (
	"testing"

	"github.com/funkygao/assert"
)

func TestMessageUsage(t *testing.T) {
	m := NewMessage(1029)
	msg := "hello world"
	m.Body = m.Body[:len(msg)]
	copy(m.Body, msg)
	assert.Equal(t, msg, string(m.Body))
	assert.Equal(t, len(msg), len(m.Body))
}
