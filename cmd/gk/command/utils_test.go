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
	assert.Equal(t, false, patternMatched("orderstatus", "!order"))

}
