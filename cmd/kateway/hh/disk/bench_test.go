package disk

import (
	"os"
	"strings"
	"testing"

	"github.com/funkygao/gafka/cmd/kateway/store"
	"github.com/funkygao/gafka/cmd/kateway/store/dummy"
	log "github.com/funkygao/log4go"
)

func init() {
	store.DefaultPubStore = dummy.NewPubStore(false)
	log.Disable()
}

func BenchmarkHintedHandoffAppendWithBufio(b *testing.B) {
	DisableBufio = false

	valLen := 1 << 10
	val := []byte(strings.Repeat("X", valLen))
	cfg := DefaultConfig()
	cfg.Dirs = []string{"hh"}
	s := New(cfg)
	s.Start()
	defer s.Stop()
	if true {
		defer os.RemoveAll("hh")
	}

	for i := 0; i < b.N; i++ {
		s.Append("cluster", "topic", nil, val)
	}

	b.SetBytes(int64(valLen))
}

func BenchmarkHintedHandoffAppendWithoutBufio(b *testing.B) {
	DisableBufio = true

	valLen := 1 << 10
	val := []byte(strings.Repeat("X", valLen))
	cfg := DefaultConfig()
	cfg.Dirs = []string{"hh"}
	s := New(cfg)
	s.Start()
	defer s.Stop()
	if true {
		defer os.RemoveAll("hh")
	}

	for i := 0; i < b.N; i++ {
		s.Append("cluster", "topic", nil, val)
	}

	b.SetBytes(int64(valLen))
}

func BenchmarkHintedHandoffAppendWithBufioAndFlushEvery1K(b *testing.B) {
	DisableBufio = false
	flushEveryBlocks = 1000

	valLen := 1 << 10
	val := []byte(strings.Repeat("X", valLen))
	cfg := DefaultConfig()
	cfg.Dirs = []string{"hh"}
	s := New(cfg)
	s.Start()
	defer s.Stop()
	if true {
		defer os.RemoveAll("hh")
	}

	for i := 0; i < b.N; i++ {
		s.Append("cluster", "topic", nil, val)
	}

	b.SetBytes(int64(valLen))
}
