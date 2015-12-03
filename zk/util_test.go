package zk

import (
	"testing"

	"github.com/funkygao/assert"
)

func TestTimestampToTime(t *testing.T) {
	tm := TimestampToTime("143761637400")
	t.Logf("%+v", tm)
	assert.Equal(t, 1974, tm.Year())
	assert.Equal(t, "July", tm.Month().String())
}

func TestParseConsumerHost(t *testing.T) {
	host := hostOfConsumer("console-consumer-48389_mac-2.local-1449108222694-9f9b7aa7")
	assert.Equal(t, "mac-2.local", host)

	host = hostOfConsumer("mac-2.local:ab3373df-02c7-4074-adc0-49078af110ff")
	assert.Equal(t, "mac-2.local", host)

	// FIXME add more robust test case
	host = hostOfConsumer("mac-2.local-invalid")
	assert.Equal(t, "mac", host)
}
