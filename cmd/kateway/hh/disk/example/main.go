package main

import (
	"fmt"
	"net/http"
	_ "net/http/pprof"
	"sync"
	"time"

	"github.com/funkygao/gafka/cmd/kateway/hh/disk"
	log "github.com/funkygao/log4go"
)

func main() {
	go http.ListenAndServe("localhost:6786", nil)
	log.Info("pprf ready on http://localhost:6786/debug/pprof")

	cfg := disk.DefaultConfig()
	cfg.Dir = "hh"
	s := disk.New(cfg)
	if err := s.Start(); err != nil {
		panic(err)
	}

	i, j := 5, 5
	var wg sync.WaitGroup

	for seq := 0; seq < i; seq++ {
		wg.Add(1)
		go func(seq int) {
			defer wg.Done()

			for loops := 0; loops < j; loops++ {
				if err := s.Append("me", "app1.foobar.v1", []byte("key"),
					[]byte(fmt.Sprintf("<#%d/%d sent at: %s>", seq, loops+1, time.Now()))); err != nil {
					panic(err)
				}
			}
		}(seq)
	}

	log.Info("%d sent, waiting Append finish...", i*j)
	wg.Wait()
	log.Info("all Append done")

	s.Stop()
	log.Info("Did you see %d messages?", i*j)
	log.Info("bye!")
	log.Close()
}
