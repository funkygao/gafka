package main

import (
	"fmt"
	"net/http"
	_ "net/http/pprof"
	"strings"
	"sync"
	"time"

	"github.com/funkygao/gafka/cmd/kateway/hh/disk"
	log "github.com/funkygao/log4go"
)

// TODO
// 1. simulate a segment corrupts
// 2. perf tuning
// 3. mem reuse
// 4. load balance disk IO
// 5. metrics for hh

func main() {
	go http.ListenAndServe("localhost:6786", nil)
	log.Info("pprof ready on http://localhost:6786/debug/pprof")

	cfg := disk.DefaultConfig()
	cfg.Dir = "hh"
	s := disk.New(cfg)
	if err := s.Start(); err != nil {
		panic(err)
	}

	placeholder := strings.Repeat(".", 1<<10)

	i, j := 5, 1000
	cluster, topic := "me", "app1.foobar.v1"
	var wg sync.WaitGroup

	for seq := 0; seq < i; seq++ {
		wg.Add(1)
		go func(seq int) {
			defer wg.Done()

			for loops := 0; loops < j; loops++ {
				if err := s.Append(cluster, topic, []byte("key"),
					[]byte(fmt.Sprintf("<#%d/%d sent at: %s %s>", seq, loops+1, time.Now(), placeholder))); err != nil {
					panic(err)
				}
			}
		}(seq)
	}

	log.Info("%d sent, waiting Append finish...", i*j)
	wg.Wait()
	log.Info("all Append done")
	s.Stop()
	log.Info("service stopped")

	log.Info("Did you see %d messages? Inflight empty: %v", i*j, s.Empty(cluster, topic))
	log.Info("bye!")

	s.FlushInflights()
	log.Close()
}
