package command

import (
	"strconv"
	"testing"
)

func TestSortMap(t *testing.T) {
	m := make(map[string]int)
	for i := 0; i < 10; i++ {
		m[strconv.Itoa(i+10)] = i * 10
	}

	r := sortStrMap(m)
	t.Logf("%+v", r)
}
