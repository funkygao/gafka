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
	fixtures := assert.Fixtures{
		assert.Fixture{Input: "console-consumer-48389_mac-2.local-1449108222694-9f9b7aa7", Expected: "mac-2.local"},
		assert.Fixture{Input: "mygroup_myhost-1474160855114-d726ea23", Expected: "myhost"},
		assert.Fixture{Input: "mac-2.local:ab3373df-02c7-4074-adc0-49078af110ff", Expected: "mac-2.local"},
		assert.Fixture{Input: "mac-2.local-invalid", Expected: "mac"},
	}

	for _, f := range fixtures {
		assert.Equal(t, f.Expected, hostOfConsumer(f.Input.(string)))
	}
}

func TestExtractConsumerIdFromOwnerInfo(t *testing.T) {
	fixtures := assert.Fixtures{
		assert.Fixture{Input: "ABCD-243036014:4c925a17-b5fb-4a59-a15e-649a5762e663", Expected: "ABCD-243036014:4c925a17-b5fb-4a59-a15e-649a5762e663"},
		assert.Fixture{Input: "ABCD-243043184@10.10.10.1:2eddbd1a-2500-4d3f-99bd-2258096787aa", Expected: "ABCD-243043184@10.10.10.1:2eddbd1a-2500-4d3f-99bd-2258096787aa"},
		assert.Fixture{Input: "dozer-a_4d02c42b-739d-4f09-8610-6299ffc27f85", Expected: "dozer-a_4d02c42b-739d-4f09-8610-6299ffc27f85"},
		assert.Fixture{Input: "timerTaskGroup_ABM1E03-245010067-1472192713049-7f6157ae-0", Expected: "timerTaskGroup_ABM1E03-245010067-1472192713049-7f6157ae"},
		assert.Fixture{Input: "timerTaskGroup_ABM1E03-245010067-1472192713049-7f6157ae-10", Expected: "timerTaskGroup_ABM1E03-245010067-1472192713049-7f6157ae"},
	}

	for _, f := range fixtures {
		assert.Equal(t, f.Expected, extractConsumerIdFromOwnerInfo(f.Input.(string)))
	}
}
