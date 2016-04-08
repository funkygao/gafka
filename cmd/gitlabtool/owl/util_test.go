package main

import (
	"testing"
)

func TestWideStr(t *testing.T) {
	strs := []string{
		wideStr("网吗", 20),
		wideStr("网 吗", 20),
		wideStr("a网吗", 20),
		wideStr("网吗没", 20),
		wideStr("funkygao", 20),
	}
	for _, s := range strs {
		t.Logf("%s", s)
		//t.Logf("%20s", "网吗")
	}

	t.Logf("%s", "我们")
	t.Logf("%s", "abcd")
}
