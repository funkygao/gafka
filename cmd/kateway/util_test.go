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

func TestValidateGroupName(t *testing.T) {
	type fixture struct {
		ok    bool
		group string
	}

	fixtures := []fixture{
		fixture{false, ""}, // group cannot be empty
		fixture{true, "testA"},
		fixture{true, "te_stA"},
		fixture{true, "a"},
		fixture{true, "111111"},
		fixture{true, "Zb44444444"},
		fixture{false, "a b"},
		fixture{false, "a.adsf"},
		fixture{false, "a.a.bbb3"},
		fixture{false, "(xxx)"},
		fixture{false, "[asdf"},
		fixture{false, "'asdfasdf"},
		fixture{false, "&asdf"},
		fixture{false, ">adsf"},
		fixture{false, "adf/asdf"},
		fixture{false, "a+b4"},
		fixture{true, "4-2323"},
	}
	for _, f := range fixtures {
		assert.Equal(t, f.ok, validateGroupName(f.group), f.group)
	}
}

func TestValidateTopicName(t *testing.T) {
	assert.Equal(t, false, validateTopicName("")) // topic cannot be empty
	assert.Equal(t, true, validateTopicName("topic"))
	assert.Equal(t, true, validateTopicName("trade-store"))
	assert.Equal(t, true, validateTopicName("trade-store_x"))
	assert.Equal(t, true, validateTopicName("trade-sTore_"))
	assert.Equal(t, true, validateTopicName("trade-st99ore_x"))
	assert.Equal(t, true, validateTopicName("trade-st99ore_x000"))
	assert.Equal(t, false, validateTopicName("trade-.store"))
	assert.Equal(t, false, validateTopicName("trade-$store"))
	assert.Equal(t, false, validateTopicName("trade-@store"))
}

func TestExtractFromMetricsName(t *testing.T) {
	appid, topic, ver, realname := extractFromMetricsName("{app1.mytopic.v1}pub.ok")
	assert.Equal(t, "app1", appid)
	assert.Equal(t, "mytopic", topic)
	assert.Equal(t, "v1", ver)
	assert.Equal(t, "pub.ok", realname)

	appid, topic, ver, realname = extractFromMetricsName("pub.ok")
	assert.Equal(t, "pub.ok", realname)
	assert.Equal(t, "", appid)
	assert.Equal(t, "", topic)
	assert.Equal(t, "", ver)
}

// 186 ns/op
func BenchmarkExtractFromMetricsName(b *testing.B) {
	for i := 0; i < b.N; i++ {
		extractFromMetricsName("{app1.mytopic.v1}pub.ok")
	}
}

// 46.1 ns/op
func BenchmarkValidateGroupName(b *testing.B) {
	for i := 0; i < b.N; i++ {
		validateGroupName("asdfasdf-1")
	}
}

// 837 ns/op
func BenchmarkValidateTopicName(b *testing.B) {
	for i := 0; i < b.N; i++ {
		validateTopicName("asdfasdf-1")
	}
}

// 73.4 ns/op
func BenchmarkGetHttpRemoteIp(b *testing.B) {
	r, err := buildHttpRequest()
	if err != nil {
		b.Fatal(err)
	}

	for i := 0; i < b.N; i++ {
		getHttpRemoteIp(r)
	}
}
