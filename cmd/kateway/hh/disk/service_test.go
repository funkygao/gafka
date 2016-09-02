package disk

import (
	"os"
	"strings"
	"testing"
)

func BenchmarkHintedHandoffAppend(b *testing.B) {
	val := []byte(strings.Repeat("X", 1024))
	cfg := DefaultConfig()
	cfg.Dir = "hh"
	s := New(cfg)
	s.Start()
	defer s.Stop()
	if false {
		defer os.RemoveAll("hh")
	}

	for i := 0; i < b.N; i++ {
		s.Append("cluster", "topic", nil, val)
	}
}
