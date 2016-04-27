package command

import (
	"strconv"
	"testing"

	"github.com/funkygao/assert"
)

func TestSortMap(t *testing.T) {
	m := make(map[string]int)
	for i := 0; i < 10; i++ {
		m[strconv.Itoa(i+10)] = i * 10
	}

	r := sortStrMap(m)
	t.Logf("%+v", r)
}

func TestPatternMatched(t *testing.T) {
	assert.Equal(t, true, patternMatched("orderstatus", "order"))
	assert.Equal(t, false, patternMatched("orderstatus", "~order"))
	assert.Equal(t, true, patternMatched("flashtrade_web", "~svc_hippo"))
	assert.Equal(t, false, patternMatched("34.StrollSearchRuleMsg.v1", "laxinRiskControl"))
	assert.Equal(t, true, patternMatched("34.laxinRiskControl.v1", "laxinRiskControl"))
}

func TestShortIp(t *testing.T) {
	assert.Equal(t, "44.212", shortIp("12.21.44.212"))
}
