package redis

import (
	"testing"

	"github.com/funkygao/assert"
)

func TestExtractKeysCount(t *testing.T) {
	line := "keys=15500,expires=15500,avg_ttl=27438570"
	keys := extractKeysCount(line)
	assert.Equal(t, int64(15500), keys)
}
