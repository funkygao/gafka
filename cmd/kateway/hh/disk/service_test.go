package disk

import (
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/funkygao/gafka/cmd/kateway/store"
	"github.com/funkygao/gafka/cmd/kateway/store/dummy"
	log "github.com/funkygao/log4go"
)

func init() {
	var wg sync.WaitGroup
	store.DefaultPubStore = dummy.NewPubStore(&wg, false)
	log.Disable()
}

func BenchmarkHintedHandoffAppendWithBufio(b *testing.B) {
	DisableBufio = false

	valLen := 1 << 10
	val := []byte(strings.Repeat("X", valLen))
	cfg := DefaultConfig()
	cfg.Dir = "hh"
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
	cfg.Dir = "hh"
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

func BenchmarkHintedHandoffAppendWithBufioAndFlushLess(b *testing.B) {
	DisableBufio = false
	flushEveryBlocks = 1000
	flushInterval = time.Minute

	valLen := 1 << 10
	val := []byte(strings.Repeat("X", valLen))
	cfg := DefaultConfig()
	cfg.Dir = "hh"
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
