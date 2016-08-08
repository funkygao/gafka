package mysql

import (
	"net/http"
	"testing"

	"github.com/funkygao/assert"
	"github.com/funkygao/gafka/ctx"
)

func validateTopicName(topic string) bool {
	m := mysqlStore{}
	return m.ValidateTopicName(topic)
}

func TestKafkaTopic(t *testing.T) {
	m := &mysqlStore{}

	appid := "ap1"
	topic := "foobar"
	ver := "v1"
	if kafkaTopicWithSprintf(m, appid, topic, ver) != m.KafkaTopic(appid, topic, ver) {
		t.Fail()
	}
}

func TestTopicAppid(t *testing.T) {
	m := &mysqlStore{}
	assert.Equal(t, "app1", m.TopicAppid("app1.foobar.v1"))
}

func TestKafkaTopicWithObfuscation(t *testing.T) {
	ctx.LoadFromHome()
	m := New(DefaultConfig("local"))
	appid := "app1"
	topic := "foobar"
	ver := "v10"
	m.appSecretMap = make(map[string]string)
	m.appSecretMap[appid] = "b7b73ac504d84944a3fedb801b348b2e"
	t.Logf("topic: %s", m.KafkaTopic(appid, topic, ver))
	assert.Equal(t, "app1.foobar.v10.844", m.KafkaTopic(appid, topic, ver))
}

func TestShadowTopic(t *testing.T) {
	m := &mysqlStore{}

	topic := "foobar"
	ver := "v1"
	assert.Equal(t, "hisapp.foobar.v1.myapp.group1.retry",
		m.ShadowTopic("retry", "myapp", "hisapp", topic, ver, "group1"))
}

func TestShadowKey(t *testing.T) {
	m := mysqlStore{}
	assert.Equal(t, "hisAppid.topic.ver.myAppid", m.shadowKey("hisAppid", "topic", "ver", "myAppid"))
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
		fixture{false, "__smoketest__"},
	}
	m := mysqlStore{}
	for _, f := range fixtures {
		assert.Equal(t, f.ok, m.ValidateGroupName(nil, f.group), f.group)
	}

	var h = make(http.Header)
	h.Set("X-Origin", "smoketest")
	assert.Equal(t, true, m.ValidateGroupName(h, "__smoketest__"))
}
