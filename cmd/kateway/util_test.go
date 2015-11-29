package main

import (
	"testing"

	"github.com/funkygao/assert"
)

func TestIsBrokerError(t *testing.T) {
	assert.Equal(t, false, isBrokerError(ErrRebalancing))
	assert.Equal(t, false, isBrokerError(ErrTooManyConsumers))
}
