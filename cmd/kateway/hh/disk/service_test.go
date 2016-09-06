package disk

import (
	"os"
	"strings"
	"sync"
	"testing"

	"github.com/funkygao/gafka/cmd/kateway/store"
	"github.com/funkygao/gafka/cmd/kateway/store/dummy"
	log "github.com/funkygao/log4go"
)

func init() {
	var wg sync.WaitGroup
	store.DefaultPubStore = dummy.NewPubStore(&wg, false)
	log.Disable()
}

func BenchmarkHintedHandoffAppend(b *testing.B) {
	valLen := 1 << 10
	val := []byte(strings.Repeat("X", valLen))
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

	b.SetBytes(int64(valLen))
}
