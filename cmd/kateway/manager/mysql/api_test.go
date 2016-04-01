package mysql

import (
	"net/http"
	"testing"

	"github.com/funkygao/assert"
)

func validateTopicName(topic string) bool {
	m := mysqlStore{}
	return m.ValidateTopicName(topic)
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

// 46.1 ns/op
func BenchmarkValidateGroupName(b *testing.B) {
	m := mysqlStore{}
	for i := 0; i < b.N; i++ {
		m.ValidateGroupName(nil, "asdfasdf-1")
	}
}

// 837 ns/op
func BenchmarkValidateTopicName(b *testing.B) {
	for i := 0; i < b.N; i++ {
		validateTopicName("asdfasdf-1")
	}
}
