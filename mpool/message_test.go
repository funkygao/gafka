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

func TestRound(t *testing.T) {
	fixtures := assert.Fixtures{
		assert.Fixture{Input: 2, Expected: 256},
		assert.Fixture{Input: 198, Expected: 256},
		assert.Fixture{Input: 256, Expected: 256},
	}

	for _, test := range fixtures {
		assert.Equal(t, test.Expected.(int), round(test.Input.(int), 256))
	}

}
