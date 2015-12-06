package main

import (
	"testing"

	"github.com/funkygao/assert"
	"github.com/funkygao/gafka/cmd/kateway/store"
)

func TestIsBrokerError(t *testing.T) {
	assert.Equal(t, false, isBrokerError(store.ErrRebalancing))
	assert.Equal(t, false, isBrokerError(store.ErrTooManyConsumers))
}
